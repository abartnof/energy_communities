library(tidyverse)
library(skimr)
library(arrow)

fn_manufacturing_energy_and_industry_facility_metadata <- '/Volumes/Extreme SSD/energy_communities/raw_input/clean_investment_monitor/clean_investment_monitor_q2_2024/manufacturing_energy_and_industry_facility_metadata.csv'
ManufacturingRaw <- read_csv(fn_manufacturing_energy_and_industry_facility_metadata, skip = 5) %>%
	rename_all(str_to_lower)

fn_census_dims <- '/Volumes/Extreme SSD/energy_communities/clean_input/geography/census_dims.csv'
CensusDims <- read_csv(fn_census_dims)
CensusDims

ManufacturingRaw %>%
	select(contains('census'))

mean(ManufacturingRaw$census_tract_2010_id %in% CensusDims$census_tract)
mean(ManufacturingRaw$census_tract_2020_id %in% CensusDims$census_tract)

Manufacturing <-
	ManufacturingRaw %>%
	mutate(
		dataset_name = 'Clean Investment Monitor',
		observation_id = str_c(dataset_name, unique_id, sep = '__')
	) %>%
	select(dataset_name, observation_id, 
				 segment,	company,	technology,	subcategory,	project_type,	current_facility_status,	status_description, 
				 census_tract = census_tract_2010_id,
				 latitude, longitude, address,
				 state_name_abbrev = state, total_facility_capex_estimated)

Manufacturing %>%
	write_parquet('/Volumes/Extreme SSD/energy_communities/clean_input/observations/clean_investment_monitor_manufacturing.parquet')


#### End Here ####
#### QC ####
# We can't join 19 records from the manufacturing table to the CensusDims--
# 6 distinct records
ManufacturingRaw %>%
	select(census_geoid = census_tract_2010_geoid) %>%
	drop_na %>%
	.$census_geoid %in% CensusDims$census_geoid %>% table

ManufacturingRaw %>%
	select(census_geoid = census_tract_2010_geoid) %>%
	distinct %>%
	drop_na %>%
	.$census_geoid %in% CensusDims$census_geoid %>% table

# unique_id functions as intended
ManufacturingRaw %>%
	count(unique_id) %>%
	filter(n > 1)

#### ELT ####
Manufacturing <-
	ManufacturingRaw %>%
	mutate(
		dataset_name = 'Clean Investment Monitor: Manufacturing, industrial, and energy facility metadata',
		observation_id = str_c(dataset_name, unique_id, sep = '__')
	) %>%
	select(dataset_name, observation_id, segment, technology, company, project_type, status_description, census_geoid = census_tract_2010_geoid) %>%
	unite('segment_technology', c('segment', 'technology'), remove = TRUE, sep = ': ')  %>%
	drop_na(census_geoid)

Manufacturing %>%
	write_parquet('/Volumes/Extreme SSD/energy_communities/clean_input/observations/clean_investment_monitor_manufacturing.parquet')
