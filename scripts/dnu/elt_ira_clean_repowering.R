library(tidyverse)
library(skimr)
library(arrow)
fn_in <- "/Volumes/Extreme SSD/energy_communities/raw_input/ira/clean_repowering_by_plant_with_coordinates(Least Cost).csv"
fn_out <- "/Volumes/Extreme SSD/energy_communities/clean_input/observations/ira/clean_repowering_clean.parquet"
fn_state_dims <- "/Volumes/Extreme SSD/energy_communities/clean_input/geography/state_dims.csv"

CleanRepoweringRaw <- read_csv(fn_in) %>%
	rename_all(str_to_lower)
StateDims <- read_csv(fn_state_dims)

CleanRepowering <-
	CleanRepoweringRaw %>%
	rowid_to_column %>%
	mutate(
		dataset_name = 'Clean Repowering',
		observation_id = str_c(dataset_name, rowid, sep = '__')
	) %>%
	rename_all(str_replace_all, '\\s', '_') %>%
	rename(state_name_abbrev = state) %>%
	select(
		dataset_name, 
		observation_id,
		state_name_abbrev, 
		majority_owner,
		technology = opportunity_by_technology,
		latitude,
		longitude,
		capacity_mw
	)

# CleanRepowering %>%
# 	distinct(state_name_abbrev) %>%
# 	mutate(check = state_name_abbrev %in% StateDims$state_name_abbrev) %>%
# 	count(check)

CleanRepowering %>%
	write_parquet(fn_out)
