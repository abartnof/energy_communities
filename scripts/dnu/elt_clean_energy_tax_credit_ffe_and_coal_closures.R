# This dataset is having issues matching partitioned census_tracts
# eg census_tract 311.04

library(tidyverse)
library(skimr)
library(arrow)
source(file = 'Documents/rmi/energy_communities/source/custom_functions.R')

CoalClosuresRaw <- read_csv('/Volumes/Extreme SSD/energy_communities/raw_input/Clean Energy Tax Credit Map/Coal_Closures_EnergyComm_v2024_1/IRA_EnergyComm_CTracts_CoalClosures_v2024_1.csv', col_types = 'c')
FossilFuelsRaw <- read_csv('/Volumes/Extreme SSD/energy_communities/raw_input/Clean Energy Tax Credit Map/MSA_NMSA_EC_FFE_v2024_1/MSA_NonMSA_EnergyCommunities_FossilFuelEmp_v2024_1.csv', col_types = 'c')

CensusDims <- read_csv('/Volumes/Extreme SSD/energy_communities/clean_input/geography/census_dims.csv')
CensusJoiner <- read_parquet('/Volumes/Extreme SSD/energy_communities/clean_input/geography/census_joiner.parquet')

#### Coal Closures ####
inspect_census_geoid_coverage(CoalClosuresRaw$geoid_tract_2020, CensusJoiner)
census_geoid_joiner <- abbreviate_census_geoids(CoalClosuresRaw$geoid_tract_2020, CensusJoiner)

CoalClosures <-
	CoalClosuresRaw %>%
	rowid_to_column() %>%
	bind_cols(census_geoid_joiner = census_geoid_joiner) %>%
	inner_join(CensusJoiner, by = 'census_geoid_joiner') %>%
	select(
		rowid,
		census_geoid,
		mine_closure = Mine_Closure,
		generator_closure = Generator_Closure,
		adjacent_to_closure = Adjacent_to_Closure) %>%
	mutate(
		dataset_name = 'clean_energy_tax_credit__coal_closures',
		observation_id = str_c(dataset_name, rowid, sep = '_'),
		mine_simple = case_when(
			mine_closure == 'Yes' ~ 'Mine closure',
			T ~ NA_character_
		),
		generator_simple = case_when(
			generator_closure == 'Yes' ~ 'Generator closure',
			T ~ NA_character_
		),
		adjacent_simple = case_when(
			adjacent_to_closure == 'Yes' ~ 'Adjacent to closure',
			T ~ NA_character_
		),
	) %>%
	select(dataset_name, observation_id, census_geoid, mine_simple, generator_simple, adjacent_simple) %>%
	gather(string, value, -dataset_name, -observation_id, -census_geoid) %>%
	drop_na(value) %>%
	group_by(dataset_name, observation_id, census_geoid) %>%
	summarize(info = str_flatten(value, collapse = ': ')) %>%
	ungroup
	
CoalClosures %>%
	skim

#### Fossil Fuel Employment ####
# Note that this also includes metropolitan statistical areas, 
# but i'll be just using county for now

# QC do these areas match the Google censusdims? looks alright.

is_match <-
	FossilFuelsRaw %>%
	select(state_fips = fipstate_2020, county_fips = fipscounty_2020) %>%
	left_join(CensusDims, by = c('state_fips', 'county_fips')) %>%
	mutate(is_match = !is.na(state_name)) %>%
	pull(is_match)

table(is_match)
sprintf(
	'%s of rows are valid matches', 
	str_c(round(mean(is_match), 4))
	)
	
FossilFuels <-
	FossilFuelsRaw %>%
	rowid_to_column() %>%
	select(
		rowid,
		state_fips = fipstate_2020,
		county_fips = fipscounty_2020,
		ffe_qual_status,
		ec_qual_status
	) %>%
	semi_join(CensusDims, by = c('state_fips', 'county_fips')) %>%
	mutate(
		dataset_name = 'clean_energy_tax_credit__fossil_fuels',
		observation_id = str_c(dataset_name, rowid, sep = '_'),
		ffe = case_when(
			ffe_qual_status == 'Yes' ~ 'FFE Yes: County is within a MSA or non-MSA that meets the 0.17 percent threshold for fossil fuel employment (ffe) as of June 7, 2024',
			ffe_qual_status == 'No' ~ 'FFE No: County is not within a MSA or non-MSA that meets the 0.17 percent threshold for fossil fuel employment (ffe) as of June 7, 2024',
			TRUE ~ NA_character_
		),
		ec = case_when(
			ec_qual_status == 'Yes' ~ 'EC Yes: County is within a MSA or non-MSA that meets both the 0.17 percent threshold for fossil fuel employment, and the unemployment rate requirement as of June 7, 2024.',
			ec_qual_status == 'No' ~ 'EC No: County is not within a MSA or non-MSA that meets both the 0.17 percent threshold for fossil fuel employment, and the unemployment rate requirement as of June 7, 2024.',
			TRUE ~ NA_character_
	)) %>%
	select(dataset_name, observation_id, state_fips, county_fips, ffe, ec) %>%
	unite('label', c('ffe', 'ec'), sep = ': ', remove = T)

#### Write to disk ####

CoalClosures %>%
	write_parquet('/Volumes/Extreme SSD/energy_communities/clean_input/observations/energy_communities_coal_closures.parquet')
FossilFuels %>%
	write_parquet('/Volumes/Extreme SSD/energy_communities/clean_input/observations/energy_communities_fossil_fuels.parquet')
