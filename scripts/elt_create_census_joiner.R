# This dataset is having issues matching partitioned census_tracts
# eg census_tract 311.04
# https://datadrivendetroit.org/blog/2021/09/16/2020-census-tract-changes/
# This is because extant tracts are being subdivided. Create a tool that 
# allows us to join to either the full tract, or to the first 9 digits, 
# ignoring the last 2.

library(tidyverse)
library(skimr)
library(arrow)

CensusDims <- read_csv('/Volumes/Extreme SSD/energy_communities/clean_input/geography/census_dims.csv')
# Create a version of CensusDims that contains both of
# these representations, to join.
# CTE: 9 digits
CteJoiner9 <- 
	CensusDims %>%
	mutate(census_geoid_joiner = str_extract(census_geoid, '^\\d{9}')) %>%
	select(census_geoid_joiner, census_geoid) %>%
	group_by(census_geoid_joiner) %>%
	slice_min(census_geoid) %>%
	ungroup
# CTE: full geoid
CteJoinerFull <-
	CensusDims %>%
	select(census_geoid) %>%
	mutate(census_geoid_joiner = census_geoid)
# Combine 
CensusJoiner <-
	CteJoinerFull %>%
	bind_rows(CteJoiner9)

CensusJoiner %>%
	write_parquet('/Volumes/Extreme SSD/energy_communities/clean_input/geography/census_joiner.parquet')
