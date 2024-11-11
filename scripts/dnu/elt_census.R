# Collect census tract shp files into one large file
# Collect at multiple points in history, so that if there are any records that 
# no longer exist, we can still plot them

library(tidyverse)
# library(skimr)
library(sf)

dir_clean <- '/Volumes/Extreme SSD/energy_communities/clean_input/geography'
fn_list_2013 <- list.files('/Volumes/Extreme SSD/energy_communities/raw_input/census_tract/2013/', recursive = TRUE, full.names = TRUE, pattern = 'shp$')
fn_list_2023 <- list.files('/Volumes/Extreme SSD/energy_communities/raw_input/census_tract/2023/', recursive = TRUE, full.names = TRUE, pattern = 'shp$')


#
CensusTract2023 <-
	fn_list_2023 %>%
	enframe(name = NULL, value = 'fn') %>%
	mutate(
		year = 2023L,
		data = map(fn, read_sf)) %>%
	unnest(data) %>%
	select(
		year,
		state_fips = STATEFP,
		county_fips = COUNTYFP,
		census_tract = TRACTCE,
		census_geoid = GEOID,
		longitude = INTPTLON,
		latitude = INTPTLAT,
		geometry
	) %>%
	mutate_at(c('longitude', 'latitude'), parse_number) %>%
	arrange(state_fips, county_fips, census_tract)

CensusTract2013 <-
	fn_list_2013 %>%
	enframe(name = NULL, value = 'fn') %>%
	mutate(
		year = 2013L,
		data = map(fn, read_sf)) %>%
	unnest(data) %>%
	select(
		year, 
		state_fips = STATEFP,
		county_fips = COUNTYFP,
		census_tract = TRACTCE,
		census_geoid = GEOID,
		longitude = INTPTLON,
		latitude = INTPTLAT,
		geometry
	) %>%
	mutate_at(c('longitude', 'latitude'), parse_number) %>%
	arrange(state_fips, county_fips, census_tract)

# How much has coverage changed?
Coverage <-
	bind_rows(
	CensusTract2013 %>%
		select(year, census_geoid),
	CensusTract2023 %>%
		select(year, census_geoid)
	) %>%
		mutate(dummy = TRUE) %>%
		spread(year, dummy) %>%
		mutate_if(is.logical, replace_na, FALSE) %>%
		mutate(
			coverage = case_when(
				`2013` & `2023` ~ 'Both',
				`2013` ~ '2013',
				`2023` ~ '2023',
				!`2013` & !`2023` ~ 'Neither',  # impossible here
				TRUE ~ 'DEFAULT_UNKNOWN'  # impossible here
			)
		) %>%
		count(coverage)

Coverage %>%
	mutate(prop = round(n / sum(n), 2))
sum(Coverage$n)  # num total tracts

#### Shapes ####
# For each geoid listing, take the most recent shape only

SubsetterGeometry <-
	bind_rows(
		CensusTract2013,
		CensusTract2023
	) %>%
	as.data.frame %>%
	select(census_geoid, year) %>%
	group_by(census_geoid) %>%
	slice_max(year) %>%
	ungroup %>%
	as_tibble

SubsetterGeometry
nrow(SubsetterGeometry)

CensusShapes <-
	bind_rows(
		CensusTract2013,
		CensusTract2023
	) %>%
	semi_join(SubsetterGeometry, by = c('year', 'census_geoid')) %>%
	select(census_geoid, geometry)

nrow(CensusShapes) == nrow(SubsetterGeometry)


#### Latitude and Longitude ####
# At the moment i'm not writing this anywhere, but it's good to have it

CensusCoordinates <-
	bind_rows(
		CensusTract2013,
		CensusTract2023
	) %>%
	semi_join(SubsetterGeometry, by = c('year', 'census_geoid')) %>%
	select(census_geoid, longitude, latitude) %>%
	arrange(census_geoid)

nrow(CensusCoordinates) == nrow(SubsetterGeometry)

#### Dims ####
# Just pull one row for each census_geoid-- I don't think there's any more
# to the deduplication than that?

CensusDims <-
	bind_rows(
		CensusTract2013,
		CensusTract2023
	) %>%
	as.data.frame %>%
	select(-geometry) %>%
	group_by(census_geoid) %>%
	slice_max(year) %>%
	ungroup %>%
	rename(census_year = year) %>%
	select(census_geoid, state_fips, county_fips, census_tract, census_year)


#### Write to disk ####
CensusDims %>%
	write_csv(file.path(dir_clean, 'census_dims.csv'))

CensusShapes %>%
	write_sf(file.path(dir_clean, 'census_shapes.shp'))