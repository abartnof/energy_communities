library(tidyverse)
library(skimr)
library(arrow)

dir_clean <- '/Volumes/Extreme SSD/energy_communities/clean_input/observations/'
CensusDims <- read_csv('/Volumes/Extreme SSD/energy_communities/clean_input/geography/census_dims.csv')
	

ClosedCoalRaw <- read_csv('/Volumes/Extreme SSD/energy_communities/raw_input/IWG/iwg_energycommunitiesdatasets_csv_april2021/CLOSED_CoalMines_btwn2005_2020.csv', 
													 col_types = cols(.default = col_character())
	)

# Create helper table
CountyGeoid <-
	CensusDims %>%
	distinct(state_fips, county_fips) %>%
	unite('county_geoid', c('state_fips', 'county_fips'), sep = '', remove = FALSE)

#### Munge data ####
ClosedCoalRaw %>%
	glimpse

# OID works as uid
ClosedCoalRaw %>%
	count(OID_) %>%
	filter(n > 1)

# Munge data
ClosedCoal <-
	ClosedCoalRaw %>%
	mutate(
		dataset = 'iwg_closed_coalmines',
		observation_id = str_c(dataset, OID_, sep = '_')
		) %>%
	rename_all(str_to_lower) %>%
	select(
		dataset,
		observation_id,
		mine_id,
		mine_name = current_mine_name,
		mine_type = current_mine_type,
		year_closed,
		county_geoid,
		state_name = state
	) %>%
	mutate(year_closed = parse_integer(year_closed)) %>%
	inner_join(CountyGeoid, by = 'county_geoid') %>%
	select(-county_geoid) %>%
	relocate(state_fips, county_fips)


# a few bad entries
1 - (nrow(ClosedCoal) / nrow(ClosedCoalRaw))

ClosedCoal %>%
	write_parquet(file.path(dir_clean, 'iwg_closed_coal.parquet'))