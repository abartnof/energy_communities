library(tidyverse)
library(skimr)
library(arrow)

dir_clean <- '/Volumes/Extreme SSD/energy_communities/clean_input/observations/'
CensusDims <- read_csv('/Volumes/Extreme SSD/energy_communities/clean_input/geography/census_dims.csv')

RetiredRaw <- read_csv('/Volumes/Extreme SSD/energy_communities/raw_input/IWG/iwg_energycommunitiesdatasets_csv_april2021/RETIREDPowerPlants_wCoalGen_btwn2005_2020.csv', 
													col_types = cols(
														Nameplate_Capacity__MW_ = 'd',
														Operating_Year = 'i',
														Retirement_Year = 'i',
														.default = col_character())
	)

# Create helper table
CountyGeoid <-
	CensusDims %>%
	distinct(state_fips, county_fips) %>%
	unite('county_geoid', c('state_fips', 'county_fips'), sep = '', remove = FALSE)

length(unique(RetiredRaw$OID_)) == nrow(RetiredRaw)

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
	inner_join(CountyGeoid, by = 'county_geoid') %>%
	select(-county_geoid) %>%
	relocate(state_fips, county_fips)

# Bad rows
1 - (nrow(Retired) / nrow(RetiredRaw))

Retired %>%
write_parquet(file.path(dir_clean, 'iwg_retired_powerplants_coal.parquet'))
