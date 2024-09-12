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

# OID works as uid
ClosedCoalRaw %>%
	count(OID_) %>%
	filter(n > 1)

# Just like IWG Retired Powerplants, we need to pad county_geoid
ClosedCoalRaw %>%
	select(county_geoid = County_GEOID) %>%
	left_join(CountyGeoid, by = 'county_geoid') %>%
	mutate(
		county_geoid_len = str_length(county_geoid),
		is_match = !is.na(state_fips)
	) %>%
	count(county_geoid_len, is_match)

# confirmed-- we need to pad county_geoid on the left size with zero
ClosedCoalRaw %>%
	rename(county_geoid = County_GEOID) %>%
	filter(str_length(county_geoid) < 5L) %>%
	select(State, `State code`, county_geoid) %>%
	sample_n(10)
	
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
	mutate(
		year_closed = parse_integer(year_closed),
		county_geoid = str_pad(county_geoid, side = 'left', pad = '0', width = 5L),
	) %>%
	inner_join(CountyGeoid, by = 'county_geoid') %>%
	select(-county_geoid) %>%
	relocate(state_fips, county_fips)


# QC-- all entries now match cleanly
nrow(ClosedCoal) / nrow(ClosedCoalRaw)

ClosedCoal %>%
	write_parquet(file.path(dir_clean, 'iwg_closed_coal.parquet'))
