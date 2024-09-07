# elt.R
library(tidyverse)
library(sf)

dir_clean <- '/Volumes/Extreme SSD/energy_communities/clean_input/geography'

RawStateShapes <- read_sf('/Volumes/Extreme SSD/energy_communities/raw_input/cb_2023_us_all_20m/cb_2023_us_state_20m/cb_2023_us_state_20m.shp')

#### State Dims ####
StateDims <-
	RawStateShapes %>%
	sf::st_drop_geometry() %>%
	select(state_fips = STATEFP, state_name_abbrev = STUSPS, state_name = NAME) %>%
	arrange(state_fips)

length(StateDims$state_fips) == length(unique(StateDims$state_fips))

#### State Shapes ####
StateShapes <-
	RawStateShapes %>%
	select(
		state_fips = STATEFP, state_geometry = geometry
	) %>%
	arrange(state_fips)

#### Write to disk ####
StateDims %>%
	write_csv(file.path(dir_clean, 'state_dims.csv'))

StateShapes %>%
	sf::write_sf(file.path(dir_clean, 'state_shapes.shp'))

# convert to WKT format explicitly

bind_cols(	
	state_fips = StateShapes$state_fips,
	state_geometry = st_as_text(StateShapes$state_geometry)
) %>%
write_csv(file.path(dir_clean, 'state_shapes_for_upload.csv'))
