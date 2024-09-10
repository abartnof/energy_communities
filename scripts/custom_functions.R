# Create a suite of functions that allows us to work with 
# census tract partitions

library(tidyverse)
# library(skimr)
# library(arrow)
library(scales)

# CensusJoiner <-
# 	read_parquet('/Volumes/Extreme SSD/energy_communities/clean_input/geography/census_joiner.parquet')

# Input <- read_csv('/Volumes/Extreme SSD/energy_communities/raw_input/Clean Energy Tax Credit Map/Coal_Closures_EnergyComm_v2024_1/IRA_EnergyComm_CTracts_CoalClosures_v2024_1.csv', col_types = 'c')

# input_census_geoid <- Input$geoid_tract_2020


#### Inspect census geoid coverage ####

inspect_census_geoid_coverage <- function(input_census_geoid, CensusJoiner){
	# See how many of the census tract GEOIDS in the input list match, as-is, 
	# in the CensusJoiner table; 
	# OTOH, see how many would match if we abbreviate to the first 9 digits.
	# Input: 
		# input_census_geoid, a list of (str) census_geoids, 11-long
		# CensusJoiner, created in the elt_create_census_joiner table; contains
			# all of the BigQuery census_tract_geoids, and the abbreviated versions
	# Output: NA, just messages
	
	is_match_full <- input_census_geoid %in% CensusJoiner$census_geoid
	is_match_9 <- str_extract(input_census_geoid, '^\\d{9}') %in% str_extract(CensusJoiner$census_geoid, '^\\d{9}')
	
	sum_covg_full <- sum(is_match_full)
	prop_covg_full <- sum_covg_full / total
	
	result_message_full <- sprintf(
		'%s of %s census_geoids match as-is: %s', 
		comma(sum(is_match_full)), 
		comma(length(is_match_full)), 
		percent(mean(is_match_full))
	)
	
	result_message_9 <- sprintf(
		'%s of %s census_geoids match if abbreviated to 9 digits: %s', 
		comma(sum(is_match_9)), 
		comma(length(is_match_9)), 
		percent(mean(is_match_9))
	)
	print(result_message_full)
	print(result_message_9)
}


#### Abbreviate census geoids ####
abbreviate_census_geoids <- function(input_census_geoid, CensusJoiner){
	# Now create a function that creates a vector based on the input vector
	# that will clean up the census_tract geoids-- leave ones that match as-is,
	# and abbreviate ones that will match if abbreviated.
	# Input: 
		# input_census_geoid, a list of (str) census_geoids, 11-long
		# CensusJoiner, created in the elt_create_census_joiner table; contains
			# all of the BigQuery census_tract_geoids, and the abbreviated versions
	# Output: a list that can be used to join to CensusJoiner, either with
			# full geoids or with abbreviated ones
	
	input_census_geoid <- Input$geoid_tract_2020
	geoid_9 <- str_extract(input_census_geoid, '^\\d{9}')
	is_match_full <- input_census_geoid %in% CensusJoiner$census_geoid
	is_match_9 <- str_extract(input_census_geoid, '^\\d{9}') %in% str_extract(CensusJoiner$census_geoid, '^\\d{9}')
	
	Result <-
		tibble(
			input_census_geoid,
			geoid_9,
			is_match_full, 
			is_match_9
		) %>%
		mutate(
			result = case_when(
				is_match_full ~ input_census_geoid,
				is_match_9 ~ geoid_9,
				TRUE ~ 'NO_MATCH'
			)
		)
	return(Result$result)
}

# abbreviate_census_geoids(Input$geoid_tract_2020, CensusJoiner = CensusJoiner)
	



