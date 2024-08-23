library(tidyverse)
library(skimr)

CoalClosuresRaw <- read_csv('/Volumes/Extreme SSD/energy_communities/raw_input/Clean Energy Tax Credit Map/Coal_Closures_EnergyComm_v2024_1/IRA_EnergyComm_CTracts_CoalClosures_v2024_1.csv', col_types = 'c')
CountyDims <- read_csv('/Volumes/Extreme SSD/energy_communities/clean_input/shapes/county_dims.csv')
CensusShapes <- read_sf('/Volumes/Extreme SSD/energy_communities/clean_input/shapes/census_shapes.shp') %>%
	st_drop_geometry() %>%
	rename(state_fips = stt_fps, county_fips = cnty_fp, tract_code = trct_cd)

CoalClosures <-
	CoalClosuresRaw %>%
	select(
		state_fips = fipstate_2020,
		county_fips = fipcounty_2020,
		tract_code = fiptract_2020,
		mine_closure = Mine_Closure,
		generator_closure = Generator_Closure,
		adjacent_to_closure = Adjacent_to_Closure) %>%
	mutate(
		is_mine_closure = case_when(
			mine_closure == 'Yes' ~ TRUE,
			mine_closure == 'No' ~ FALSE,
			mine_closure == '#N/A' ~ as.logical(NA)),
		is_generator_closure = case_when(
			generator_closure == 'Yes' ~ TRUE,
			generator_closure == 'No' ~ FALSE,
			generator_closure == '#N/A' ~ as.logical(NA)),
		is_adjacent_to_closure = case_when(
			adjacent_to_closure == 'Yes' ~ TRUE,
			adjacent_to_closure == 'No' ~ FALSE,
			adjacent_to_closure == '#N/A' ~ as.logical(NA)),
	) %>%
	select(state_fips, county_fips, tract_code, starts_with('is_'))

CoalClosures %>%
	count(is_mine_closure, is_generator_closure, is_adjacent_to_closure)

# Find Coal data cleaning issues
CoalClosures %>%
	filter(state_fips == '09') %>%
	count(county_fips)

CensusShapes %>%
	filter(state_fips == '09') %>%
	count(county_fips)

CountyDims %>%
	filter(state_fips == '09') %>%
	distinct(county_fips) %>%
	arrange(county_fips)
