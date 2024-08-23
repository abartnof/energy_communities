# ent_county.R
# 2010 has bad character encoding, but there are no county_fips x state_fips
# that aren't covered in the 1990 and 2018 files.

library(tidyverse)
library(skimr)
library(sf)

dir_clean <- '/Volumes/Extreme SSD/energy_communities/clean_input/geography'

RawCounty2018 <- read_sf('/Volumes/Extreme SSD/energy_communities/raw_input/county/cb_2018_us_county_20m/cb_2018_us_county_20m.shp')
RawCounty1990 <- read_sf('/Volumes/Extreme SSD/energy_communities/raw_input/county/co99_d90_shp/co99_d90.shp')

County2018 <-
	RawCounty2018 %>%
	select(
		state_fips = STATEFP,
		county_fips = COUNTYFP,
		county_geoid = GEOID,
		county_name = NAME,
		geometry
	) %>%
	mutate(year = 2018L) %>%
	relocate(county_geoid, state_fips, county_fips, county_name, year, geometry)

County1990 <-
	RawCounty1990 %>%
	select(
		state_fips = ST,
		county_fips = CO,
		county_name = NAME,
	) %>%
	mutate(
		county_geoid = str_c(state_fips, county_fips),
		year = 1990
	) %>%
	relocate(county_geoid, state_fips, county_fips, county_name, year, geometry)


#### Fix name inconsistencies ####
# See the entries where county_name changes-- looks like it's all typos
NameChangers <-
	
	County1990 %>%
		select(state_fips, county_fips, county_name_1990 = county_name) %>%
		st_drop_geometry() %>%
	inner_join(
		
		County2018 %>%
			select(state_fips, county_fips, county_name_2018 = county_name) %>%
			st_drop_geometry(),
		
		by = c('state_fips', 'county_fips')
	) %>%
	distinct %>%
	filter(county_name_1990 != county_name_2018)
NameChangers

# We can join this to another table, and replace the 1990 names with the 2018 ones
JoinmeNameChanges <-
	NameChangers %>%
	select(state_fips, county_name = county_name_1990, county_name_good = county_name_2018)

#### Dimensions 1: no assertion of uniqueness ####
CountyDims <-
	County1990 %>%
	bind_rows(County2018) %>%
	st_drop_geometry() %>%
	left_join(JoinmeNameChanges, by = c('state_fips', 'county_name')) %>%
	mutate(county_name = coalesce(county_name_good, county_name)) %>%
	distinct(county_geoid, state_fips, county_fips, county_name) %>%
	arrange(county_geoid)

#### Dimensions 2: each county_name is only mentioned once ####
# This is to ensure that if we only know the state_fips and the county_name,
# we don't accidentally map to multiple county records, and get fanout.
# In case of multiple county name entries, default to the highest county_fips 
# it's as good an heuristic as any.

# See the county_names with changing county_fip values
CountyDims %>%
	count(state_fips, county_name) %>%
	filter(n > 1L)

# Make sure there's just one county_fips for each state_fips x county_name
CountyDims_UniqueCountyNames <-
	CountyDims %>%
	group_by(state_fips, county_name) %>%
	slice_max(county_fips) %>%
	ungroup %>%
	arrange(county_geoid)

#### Shapes ####
# Take the most recent geometry entry (and only that geometry entry) for each
# geoid

SubsetterForGeometry <-
	County1990 %>%
	bind_rows(County2018) %>%
	st_drop_geometry() %>%
	group_by(county_geoid) %>%
	slice_max(year) %>%
	ungroup %>%
	select(county_geoid, year)

CountyShapes <-
	County1990 %>%
	bind_rows(County2018) %>%
	semi_join(SubsetterForGeometry) %>%
	select(county_geoid, geometry)

#### Write to disk ####	
CountyDims %>%
	write_csv(file.path(dir_clean, 'county_dims.csv'))

CountyDims_UniqueCountyNames %>%
	write_csv(file.path(dir_clean, 'county_dims_unique_county_names.csv'))

CountyShapes %>%
	sf::write_sf(file.path(dir_clean, 'county_shapes.shp'))