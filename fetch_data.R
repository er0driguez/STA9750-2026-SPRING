# =============================================================================
# fetch_data.R
# =============================================================================
# Downloads and caches all data needed for individual_report.qmd:
#   1. NYC 311 Closed Service Requests (2022-2025) via Socrata API
#   2. NYC Historical Weather (2022-2025) via Open-Meteo Archive API
#      — one API call per borough, results stacked into one tidy table
#   3. Joins both datasets on borough + date
#
# HOW TO USE:
#   Run this script ONCE manually from the RStudio console before rendering:
#   source("fetch_data.R")
#
#   The script checks file.exists() before every download — re-running it
#   will skip anything already cached. Delete a file to force a fresh fetch.
#
# OUTPUT FILES (all saved to data/individual/):
#   311/raw/page_0001.parquet ... page_NNNN.parquet  <- raw API pages
#   311/nyc_311_clean.parquet                        <- cleaned 311 data
#   weather/weather_clean.parquet                    <- borough-level weather
#   nyc_311_with_weather.parquet                     <- 311 + weather joined
#
# FUTURE EXTENSION — NEAREST NEIGHBOR JOIN:
#   The BOROUGH_COORDS table is structured so additional coordinates per
#   borough can be added as rows. When ready to switch to a nearest-neighbor
#   spatial join using sf::st_join(), only the join step in Part 3 needs
#   to be updated — the weather fetch requires no structural changes.
# =============================================================================

library(tidyverse)
library(httr2)
library(arrow)
library(janitor)
library(lubridate)

# =============================================================================
# CONFIGURATION
# All constants in one place
# =============================================================================

DATE_START <- "2022-01-01"
DATE_END   <- "2025-12-31"

# Socrata API endpoint for 311 Service Requests from 2020 to Present
SOCRATA_URL <- "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
PAGE_SIZE   <- 50000L  # Max rows per request; Socrata hard limit is 50,000

# Open-Meteo historical archive API — no key required
OPENMETEO_URL <- "https://archive-api.open-meteo.com/v1/archive"

# One representative coordinate per borough
# Each row is one weather location; borough column links to 311 data
# To add more points per borough later, simply add rows to this table
BOROUGH_COORDS <- tribble(
  ~borough,         ~lat,     ~lon,
  "Manhattan",      40.7829, -73.9654,  # Central Park
  "Brooklyn",       40.6602, -73.9690,  # Prospect Park
  "Queens",         40.7282, -73.8456,  # Flushing Meadows
  "Bronx",          40.8448, -73.8648,  # Bronx Park
  "Staten Island",  40.5795, -74.1502   # Geographic center
)

# Adverse/extreme weather thresholds (NWS and NYC OEM standards)
# Temperatures in °F, precipitation in inches, wind in mph
THRESH_HEAVY_RAIN <- 1     # inches/day — NWS heavy rain advisory (~25mm)
THRESH_HEAT       <- 90    # °F         — NYC OEM heat emergency
THRESH_COLD       <- 25    # °F         — NWS Hard Freeze threshold
THRESH_SNOW       <- 2     # inches/day — significant snowfall
THRESH_WIND       <- 31    # mph        — NWS wind advisory (~50 km/h)

# Output paths
DIR_311_RAW <- file.path("data", "individual", "311", "raw")
DIR_WEATHER <- file.path("data", "individual", "weather")
DIR_MAIN    <- file.path("data", "individual")

PATH_311_CLEAN <- file.path(DIR_MAIN, "nyc_311_clean.parquet")
PATH_WEATHER   <- file.path(DIR_WEATHER, "weather_clean.parquet")
PATH_JOINED    <- file.path(DIR_MAIN, "nyc_311_with_weather.parquet")

# Create all output directories if they don't already exist
dir.create(DIR_311_RAW, recursive = TRUE, showWarnings = FALSE)
dir.create(DIR_WEATHER, recursive = TRUE, showWarnings = FALSE)

# =============================================================================
# PART 1: FETCH 311 DATA
# =============================================================================
# The 311 dataset is large (~13M rows for 2022-2025 Closed requests).
# We page through it in chunks of 50,000 rows, saving each page immediately
# as a parquet file so progress is preserved if the session is interrupted.
# =============================================================================

# Build a single page request using SoQL filtering
# offset controls which page we are on
build_311_request <- function(offset) {
  request(SOCRATA_URL) |>
    req_url_query(
      # Push all filtering to the server so we only download what we need
      `$where`  = glue::glue(
        "status='Closed' AND ",
        "created_date >= '{DATE_START}T00:00:00' AND ",
        "created_date <= '{DATE_END}T23:59:59'"
      ),
      # Request only the columns we need — reduces payload size ~60%
      `$select` = paste(
        "unique_key", "created_date", "closed_date",
        "agency", "agency_name",
        "complaint_type", "descriptor",
        "resolution_description",
        "community_board", "borough", "incident_zip",
        "latitude", "longitude",
        "x_coordinate_state_plane", "y_coordinate_state_plane",
        sep = ","
      ),
      `$limit`  = PAGE_SIZE,
      `$offset` = offset,
      `$order`  = "created_date ASC"
    ) |>
    # App token gives higher rate limits — set env var if you have one:
    # Sys.setenv(SOCRATA_APP_TOKEN = "your_token_here")
    req_headers(
      `X-App-Token` = Sys.getenv("SOCRATA_APP_TOKEN", unset = "")
    )
}

fetch_311_data <- function() {
  
  # Skip entire download if clean parquet already exists
  if (file.exists(PATH_311_CLEAN)) {
    message(">> Clean 311 parquet found — skipping download and cleaning.")
    message("   Delete '", PATH_311_CLEAN, "' to re-fetch.")
    return(invisible(NULL))
  }
  
  message("\n== PART 1: Downloading NYC 311 data ==")
  message("   Filter: status=Closed, ", DATE_START, " to ", DATE_END)
  message("   This may take 20-40 minutes on first run...\n")
  
  offset     <- 0L
  page_num   <- 1L
  total_rows <- 0L
  
  # Page through the API until we get a response smaller than PAGE_SIZE
  repeat {
    out_file <- file.path(DIR_311_RAW, sprintf("page_%04d.parquet", page_num))
    
    # Skip pages already saved from a previous interrupted run
    if (file.exists(out_file)) {
      message(sprintf("   Page %d already cached — skipping.", page_num))
      offset   <- offset + PAGE_SIZE
      page_num <- page_num + 1L
      next
    }
    
    message(sprintf("   Fetching page %d (rows %d to %d)...",
                    page_num, offset + 1L, offset + PAGE_SIZE))
    
    # Retry up to 3 times on transient network errors
    resp <- build_311_request(offset) |>
      req_retry(max_tries = 3, backoff = \(x) 15) |>
      req_perform()
    
    rows <- resp |> resp_body_json(simplifyVector = TRUE)
    
    # Empty response means we have fetched all available records
    if (length(rows) == 0 || nrow(as_tibble(rows)) == 0) {
      message("   No more records returned. Download complete.")
      break
    }
    
    page_data <- as_tibble(rows)
    write_parquet(page_data, out_file)
    
    total_rows <- total_rows + nrow(page_data)
    message(sprintf("   Saved %d rows  |  Running total: %s",
                    nrow(page_data), scales::comma(total_rows)))
    
    # A page smaller than PAGE_SIZE means this is the last page
    if (nrow(page_data) < PAGE_SIZE) {
      message("   Last page reached.")
      break
    }
    
    offset   <- offset + PAGE_SIZE
    page_num <- page_num + 1L
    
    Sys.sleep(0.3)  # Brief pause to be a polite API citizen
  }
  
  message(sprintf("\n   Download complete. Total rows fetched: %s\n",
                  scales::comma(total_rows)))
  
  # ---- Clean the raw pages into a single tidy parquet ----
  message("   Cleaning and combining all pages...")
  
  # Read all raw parquet pages at once using Arrow's multi-file dataset
  raw <- open_dataset(DIR_311_RAW) |> collect()
  
  clean <- raw |>
    # Standardise all column names to snake_case
    rename_with(make_clean_names) |>
    mutate(
      # Parse datetime strings — Socrata returns ISO 8601 format
      created_date = as_datetime(created_date, tz = "America/New_York"),
      closed_date  = as_datetime(closed_date,  tz = "America/New_York"),
      
      # Resolution time in decimal hours — our primary outcome variable
      resolution_hrs = as.numeric(
        difftime(closed_date, created_date, units = "hours")
      ),
      
      # Plain date extracted for joining to daily weather data
      date = as_date(created_date),
      
      # Standardise borough to title case to match BOROUGH_COORDS table
      # e.g. "BROOKLYN" -> "Brooklyn", "STATEN ISLAND" -> "Staten Island"
      borough = str_to_title(str_trim(borough)),
      
      # Recode unspecified borough values to NA so they don't get a false
      # weather match — these rows are kept but weather fields will be NA
      borough = na_if(borough, "Unspecified"),
      
      # Extract numeric community district from "03 BROOKLYN" format
      community_district_num = as.integer(
        str_extract(community_board, "^\\d+")
      ),
      
      # Coerce coordinate columns to numeric (API returns them as character)
      latitude                  = as.numeric(latitude),
      longitude                 = as.numeric(longitude),
      x_coordinate_state_plane  = as.numeric(x_coordinate_state_plane),
      y_coordinate_state_plane  = as.numeric(y_coordinate_state_plane)
    ) |>
    
    # Remove records with invalid resolution times:
    # Negative = data entry error
    # > 17520 hrs (2 years) = outside our analytical window
    filter(
      !is.na(resolution_hrs),
      resolution_hrs >= 0,
      resolution_hrs <= 17520
    ) |>
    
    # Guard against any records outside our date window from API edge cases
    filter(date >= as.Date(DATE_START), date <= as.Date(DATE_END)) |>
    
    # Final column selection — clean and minimal
    select(
      unique_key, date, created_date, closed_date, resolution_hrs,
      agency, agency_name,
      complaint_type, descriptor,
      resolution_description,
      borough, community_board, community_district_num, incident_zip,
      latitude, longitude, x_coordinate_state_plane, y_coordinate_state_plane
    )
  
  write_parquet(clean, PATH_311_CLEAN)
  message(sprintf("   Clean 311 data saved: %s rows -> %s\n",
                  scales::comma(nrow(clean)), PATH_311_CLEAN))
  
  invisible(clean)
}

# =============================================================================
# PART 2: FETCH WEATHER DATA
# =============================================================================
# We make one API call per borough rather than a single multi-coordinate call.
# This avoids an httr2 URL-encoding issue where commas in coordinate lists
# get encoded as %2C, which the Open-Meteo API rejects with a 400 error.
# Five small calls is negligible — the weather data is tiny (~1,461 rows each).
# Results are stacked into one tidy tibble with a borough column for joining.
# All units are American: °F, inches, mph.
# =============================================================================

# Fetch and classify weather for one borough given its coordinates
# Called once per row in BOROUGH_COORDS via pmap() in fetch_weather_data()
fetch_one_borough_weather <- function(borough, lat, lon) {
  
  message(sprintf("   Fetching weather for %s (%.4f, %.4f)...",
                  borough, lat, lon))
  
  resp <- request(OPENMETEO_URL) |>
    req_url_query(
      latitude           = lat,
      longitude          = lon,
      start_date         = DATE_START,
      end_date           = DATE_END,
      daily              = paste(
        "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
        "precipitation_sum", "snowfall_sum", "windspeed_10m_max",
        sep = ","
      ),
      timezone           = "America/New_York",
      temperature_unit   = "fahrenheit",  # °F
      windspeed_unit     = "mph",         # miles per hour
      precipitation_unit = "inch"         # inches
    ) |>
    req_retry(max_tries = 3, backoff = \(x) 5) |>
    req_perform()
  
  raw <- resp |> resp_body_json(simplifyVector = TRUE)
  
  # Parse the daily data, coerce types, and apply weather classification
  as_tibble(raw$daily) |>
    rename(
      date          = time,
      temp_max_f    = temperature_2m_max,
      temp_min_f    = temperature_2m_min,
      temp_avg_f    = temperature_2m_mean,  # true 24hr hourly mean
      precip_in     = precipitation_sum,
      snowfall_in   = snowfall_sum,
      windspeed_mph = windspeed_10m_max
    ) |>
    mutate(
      # Coerce to plain numeric and parse date in one mutate block
      across(c(temp_max_f, temp_min_f, temp_avg_f,
               precip_in, snowfall_in, windspeed_mph), as.numeric),
      borough = borough,
      date    = as.Date(as.character(date)),
      
      # Replace NA precipitation/snowfall with 0 — no data means no precip
      across(c(precip_in, snowfall_in), \(x) replace_na(x, 0)),
      
      # ---- Adverse/Extreme weather classification ----
      # Boolean flag for each individual hazard type
      is_heavy_rain = precip_in     >= THRESH_HEAVY_RAIN,
      is_heat       = temp_max_f    >= THRESH_HEAT,
      is_cold       = temp_min_f    <= THRESH_COLD,   # NWS Hard Freeze: 25°F
      is_snow       = snowfall_in   >= THRESH_SNOW,
      is_high_wind  = windspeed_mph >= THRESH_WIND,
      
      # Count simultaneous hazards active on each day
      hazard_count = is_heavy_rain + is_heat + is_cold +
        is_snow + is_high_wind,
      
      # Ordered category:
      # Extreme = significant snow OR 2+ simultaneous hazards
      # Adverse = exactly 1 hazard
      # Normal  = no hazards
      weather_category = case_when(
        is_snow | hazard_count >= 2 ~ "Extreme",
        hazard_count == 1           ~ "Adverse",
        TRUE                        ~ "Normal"
      ) |> factor(levels = c("Normal", "Adverse", "Extreme"), ordered = TRUE),
      
      # Human-readable dominant condition for labels and map tooltips
      dominant_condition = case_when(
        is_snow       ~ "Snow",
        is_heat       ~ "Extreme Heat",
        is_cold       ~ "Extreme Cold",
        is_heavy_rain ~ "Heavy Rain",
        is_high_wind  ~ "High Wind",
        TRUE          ~ "Clear"
      )
    ) |>
    # Put borough first for readability
    select(borough, date, everything())
}

fetch_weather_data <- function() {
  
  if (file.exists(PATH_WEATHER)) {
    message(">> Clean weather parquet found — skipping download.")
    message("   Delete '", PATH_WEATHER, "' to re-fetch.")
    return(invisible(NULL))
  }
  
  message("\n== PART 2: Downloading NYC borough-level weather data ==")
  message("   Source: Open-Meteo Historical Archive API")
  message("   Boroughs: ", paste(BOROUGH_COORDS$borough, collapse = ", "))
  message(sprintf("   Date range: %s to %s", DATE_START, DATE_END))
  message("   Units: °F, inches, mph\n")
  
  # pmap() loops over each row of BOROUGH_COORDS, passing borough, lat, lon
  # as named arguments to fetch_one_borough_weather() — one API call per borough
  # bind_rows() stacks all five results into one tidy tibble
  weather <- pmap(BOROUGH_COORDS, fetch_one_borough_weather) |>
    bind_rows()
  
  write_parquet(weather, PATH_WEATHER)
  message(sprintf(
    "\n   Weather data saved: %d boroughs x %d days = %d rows -> %s\n",
    n_distinct(weather$borough),
    n_distinct(weather$date),
    nrow(weather),
    PATH_WEATHER
  ))
  
  invisible(weather)
}

# =============================================================================
# PART 3: JOIN 311 + WEATHER
# =============================================================================
# Each 311 request is matched to the weather data for its borough on the
# date it was created. Records with a missing borough get NA weather fields
# rather than a false match.
#
# FUTURE NEAREST-NEIGHBOR UPGRADE:
# When ready, replace this left_join() with an sf::st_join() that matches
# each 311 request's lat/lon to the nearest weather coordinate point. The
# BOROUGH_COORDS table and weather parquet require no structural changes.
# =============================================================================

build_joined_data <- function() {
  
  if (file.exists(PATH_JOINED)) {
    message(">> Joined parquet found — skipping join.")
    message("   Delete '", PATH_JOINED, "' to rebuild.")
    return(invisible(NULL))
  }
  
  # Both clean parquets must exist before we can join
  if (!file.exists(PATH_311_CLEAN)) {
    stop("Clean 311 parquet not found. Run fetch_311_data() first.")
  }
  if (!file.exists(PATH_WEATHER)) {
    stop("Weather parquet not found. Run fetch_weather_data() first.")
  }
  
  message("\n== PART 3: Joining 311 and weather data ==")
  message("   Join key: borough + date")
  message("   Records with missing borough will have NA weather fields\n")
  
  nyc_311 <- read_parquet(PATH_311_CLEAN)
  weather  <- read_parquet(PATH_WEATHER)
  
  # Left join on borough + date — every 311 request is kept
  # Requests with NA borough won't match any weather row -> weather cols = NA
  nyc_311_with_weather <- nyc_311 |>
    left_join(
      # Only bring in the weather columns needed for analysis
      weather |> select(
        borough, date,
        temp_max_f, temp_min_f, temp_avg_f,
        precip_in, snowfall_in, windspeed_mph,
        weather_category, dominant_condition, hazard_count
      ),
      by = c("borough", "date")
    )
  
  # Report how many records were matched vs left without weather data
  n_matched   <- sum(!is.na(nyc_311_with_weather$weather_category))
  n_unmatched <- sum(is.na(nyc_311_with_weather$weather_category))
  
  message(sprintf("   Matched:                        %s rows",
                  scales::comma(n_matched)))
  message(sprintf("   No weather match (missing borough): %s rows",
                  scales::comma(n_unmatched)))
  
  write_parquet(nyc_311_with_weather, PATH_JOINED)
  message(sprintf("\n   Joined dataset saved: %s total rows -> %s\n",
                  scales::comma(nrow(nyc_311_with_weather)), PATH_JOINED))
  
  invisible(nyc_311_with_weather)
}

# =============================================================================
# RUN ALL THREE STEPS IN ORDER
# =============================================================================

fetch_311_data()
fetch_weather_data()
build_joined_data()

message("========================================")
message("All data ready.")
message("========================================\n")
