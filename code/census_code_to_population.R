

# Code reference:
# https://stanfordfuturebay.github.io/covid19/safegraph_normalization_explainer.html#patterns-data-expansion
# Script to get census info from 12-letter blockgroup code

library(censusapi)
library(dplyr)

Sys.setenv(CENSUS_KEY="9fd08d983763a05db9004cac40be824deba9c435")

############# Actual code ###############

# first two letter in cbg code is state code
fips_to_state_code <- function(fips_code){
  return(substr(fips_code, 1, 2))
}

fips_to_county_code <- function(fips_code){
  return(substr(fips_code, 3, 5))
}


fips_to_tract_code <- function(fips_code){
  return(substr(fips_code, 6, 11))
}

fips_to_block_code <- function(fips_code){
  return(substr(fips_code, 12, 12))
}


block_group_to_population <- function(fips_code, field = "B01003_001E") {
  tryCatch({
    # Extract code from full 12-digits code
    state_code <- fips_to_state_code(fips_code)
    county_code <- fips_to_county_code(fips_code)
    tract_code <- fips_to_tract_code(fips_code)
    block_code <- fips_to_block_code(fips_code)
    
    data <-
      getCensus(
        name = "acs/acs5",
        vintage = 2018,
        region = "block group:*", 
        regionin = paste0("state:", state_code, "+county:", county_code),
        vars = "B01003_001E") %>%
      filter(tract == tract_code) %>%
      filter(block_group == block_code) %>% 
      select(B01003_001E)
    
    return(data$B01003_001E)
  }, error = function(e) {
    cat("An error occurred: ", conditionMessage(e), "\n")
    return(NA)
  })
}


#raw data from event

block_groups <- read.csv('/scratch/09069/dhp563/hack/raw/keys_to_find_count.csv', header=FALSE) %>% unlist()
population <- lapply(block_groups[1:3], block_group_to_population)
print(population)
