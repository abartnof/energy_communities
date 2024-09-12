library(tidyverse)
library(skimr)
library(arrow)
source(file = 'Documents/rmi/energy_communities/source/custom_functions.R')

dir_clean <- '/Volumes/Extreme SSD/energy_communities/clean_input/observations/'
CensusDims <- read_csv('/Volumes/Extreme SSD/energy_communities/clean_input/geography/census_dims.csv')

RetiredRaw <- read_csv('/Volumes/Extreme SSD/energy_communities/raw_input/IWG/iwg_energycommunitiesdatasets_csv_april2021/RETIREDPowerPlants_wCoalGen_btwn2005_2020.csv', 
													col_types = cols(
														Nameplate_Capacity__MW_ = 'd',
														Operating_Year = 'i',
														Retirement_Year = 'i',
														.default = col_character())
	)


# OID works as a UID
length(unique(RetiredRaw$OID_)) == nrow(RetiredRaw)

#### Check how well GIS data works ####

# Create county_geoid helper table
CountyGeoid <-
	CensusDims %>%
	distinct(state_fips, county_fips) %>%
	unite('county_geoid', c('state_fips', 'county_fips'), sep = '', remove = FALSE)

# Any county_geoid with fewer than 5 digits won't join, but we can fix this by padding on the left side
Padded <-
	RetiredRaw %>%
	select(county_geoid = County_GEOID) %>%
	mutate(
		pad_left = str_pad(county_geoid, side = 'left', pad = '0', width = 5L),
		pad_right = str_pad(county_geoid, side = 'right', pad = '0', width = 5L),
	)
Padded %>%
	gather(variable, value) %>%
	mutate(is_match = value %in% CountyGeoid$county_geoid) %>%
	group_by(variable) %>%
	summarize(
		prop = mean(is_match),
		num_matches = sum(is_match),
		num_bad = sum(!is_match)
	) %>%
	ungroup

# Padding to the left passes a QC-- 
# arizona is fips code 04, and that gives us Cholla plant.
# Likewise, alabama is 01, which is consistent with the Lowman Energy Center
Padded %>%
	filter(str_length(county_geoid) < 5) %>%
	inner_join(RetiredRaw, by = c('county_geoid' = 'County_GEOID'))

#### Munge ####
RetiredRaw %>%
	glimpse

Retired <-
	RetiredRaw %>%
	rename_all(str_to_lower) %>%
	mutate(
		dataset = 'iwg_retired_powerplants_coal_',
		observation_id = str_c(dataset, oid_, sep = '_')
	) %>%
	select(
		dataset,
		observation_id,
		plant_name, 
		nameplate_capacity_mw = nameplate_capacity__mw_,
		technology,
		operating_year,
		retirement_year,
		county_geoid
	) %>%
	mutate(
		county_geoid = str_pad(county_geoid, side = 'left', pad = '0', width = 5L),
	) %>%
	inner_join(CountyGeoid, by = 'county_geoid') %>%
	select(-county_geoid) %>%
	relocate(state_fips, county_fips)

Retired %>%
write_parquet(file.path(dir_clean, 'iwg_retired_powerplants_coal.parquet'))
