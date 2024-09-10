# This dataset is having issues matching partitioned census_tracts
# eg census_tract 311.04

library(tidyverse)
library(skimr)
library(arrow)
source(file = 'Documents/rmi/energy_communities/scripts/custom_functions.R')

CoalClosuresRaw <- read_csv('/Volumes/Extreme SSD/energy_communities/raw_input/Clean Energy Tax Credit Map/Coal_Closures_EnergyComm_v2024_1/IRA_EnergyComm_CTracts_CoalClosures_v2024_1.csv', col_types = 'c')
# FossilFuelsRaw <- read_csv('/Volumes/Extreme SSD/energy_communities/raw_input/Clean Energy Tax Credit Map/MSA_NMSA_EC_FFE_v2024_1/MSA_NonMSA_EnergyCommunities_FossilFuelEmp_v2024_1.csv', col_types = 'c')

CensusDims <- read_csv('/Volumes/Extreme SSD/energy_communities/clean_input/geography/census_dims.csv')
CensusJoiner <- read_parquet('/Volumes/Extreme SSD/energy_communities/clean_input/geography/census_joiner.parquet')

inspect_census_geoid_coverage(CoalClosuresRaw$geoid_tract_2020, CensusJoiner)
abbreviate_census_geoids(CoalClosuresRaw$geoid_tract_2020, CensusJoiner)

CoalClosuresWithGeoid <-	
	CoalClosuresRaw %>%
	rename(census_geoid = geoid_tract_2020) %>%
	mutate(
		is_match_full = census_geoid %in% geoid_list_full,
		is_match_9 = str_extract(census_geoid, '^\\d{9}') %in% geoid_list_9,
		joiner = case_when(
			is_match_full ~ census_geoid,
			is_match_9 ~ str_extract(census_geoid, '^\\d{9}'),
			TRUE ~ 'DEFAULT_UNKNOWN'
		)
	) %>%
	select(-census_geoid, -is_match_full, -is_match_9) %>%
	inner_join(JoinerToGeoid, by = 'joiner', relationship = 'many-to-one') %>%
	select(-joiner)

#### Munge Coal Closures ####

CoalClosures <-
	CoalClosuresWithGeoid %>%
	rowid_to_column() %>%
	select(
		rowid,
		census_geoid,
		# state_fips = fipstate_2020,
		# county_fips = fipcounty_2020,
		# census_tract = fiptract_2020,
		# census_geoid = geoid_tract_2020,
		mine_closure = Mine_Closure,
		generator_closure = Generator_Closure,
		adjacent_to_closure = Adjacent_to_Closure) %>%
	mutate(
		dataset_name = 'clean_energy_tax_credit__coal_closures',
		observation_id = str_c(dataset_name, rowid, sep = '_'),
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
	select(dataset_name, observation_id, census_geoid, starts_with('is_'))


table(CoalClosures$census_geoid %in% CensusDims$census_geoid)
CoalClosuresRaw %>%
	select(state_fips = fipstate_2020, county_fips = fipcounty_2020, census_name = CensusTract_Name, census_geoid = geoid_tract_2020) %>%
	mutate(
		is_match = census_geoid %in% CensusDims$census_geoid,
		is_portion = str_detect(census_name, '\\.')
	) %>%
	count(is_portion, is_match)

# There is an issue with the portioning of the census tracts here
CoalClosuresRaw %>%
	select(state_fips = fipstate_2020, county_fips = fipcounty_2020, census_name = CensusTract_Name, census_geoid = geoid_tract_2020)
CensusDims %>%
	filter(
		state_fips == '21', 
		county_fips == '153',
		str_detect(census_tract, '9702')
	)

CoalClosures %>%
	select(census_geoid) %>%
	mutate(
		is_match = census_geoid %in% CensusDims$census_geoid,
		ending_digit = str_extract(census_geoid, '\\d$')
		) %>%
	group_by(ending_digit) %>%
	summarize(mean(is_match))
	filter(!is_match)
	
CensusDims %>%
	filter(
		state_fips == '21',
		str_detect(census_tract, '0301$')
	)
	



#### Fossil Fuel Employment ####
# Note that this also includes metropolitan statistical areas, 
# but i'll be just using county for now

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
	mutate(
		dataset_name = 'clean_energy_tax_credit__fossil_fuels',
		observation_id = str_c(dataset_name, rowid, sep = '_'),
		ffe = case_when(
			ffe_qual_status == 'Yes' ~ 'Yes: County is within a MSA or non-MSA that meets the 0.17 percent threshold for fossil fuel employment (ffe) as of June 7, 2024',
			ffe_qual_status == 'No' ~ 'No: County is not within a MSA or non-MSA that meets the 0.17 percent threshold for fossil fuel employment (ffe) as of June 7, 2024',
			TRUE ~ NA_character_
		),
		ec = case_when(
			ec_qual_status == 'Yes' ~ 'Yes: County is within a MSA or non-MSA that meets both the 0.17 percent threshold for fossil fuel employment, and the unemployment rate requirement as of June 7, 2024.',
			ec_qual_status == 'No' ~ 'No: County is within a MSA or non-MSA that meets both the 0.17 percent threshold for fossil fuel employment, and the unemployment rate requirement as of June 7, 2024.',
			TRUE ~ NA_character_
	)) %>%
	select(dataset_name, observation_id, state_fips, county_fips, ffe, ec)

#### QC ####

# No missing data
CoalClosures %>%
	select(state_fips, county_fips, census_tract, census_geoid) %>%
	is.na %>% colMeans

FossilFuels %>%
	select(contains('fips')) %>%
	is.na %>% colMeans

# Closures: each census_geoid is used once
length(unique(CoalClosures$census_geoid)) == length(CoalClosures$census_geoid)

# FFE: likewise, each county fips is mentioned once
FossilFuels %>%
	count(state_fips, county_fips) %>%
	filter(n > 1)

# There are 110 census_geoids we can't understand, and we don't know which 
# of the conflicting records we should prioritize over the others to smooth
# away the inconsistencies! My conservative impulse is to just omit these few
# records.
CoalClosures %>%
	anti_join(CensusDims, by = 'census_geoid') %>%
	nrow

# We could either map them to a county or a census-- but since we don't know
# which is right, just omit these 110 records.
CoalClosures %>%
	anti_join(CensusDims, by = c('state_fips', 'county_fips')) %>%
	nrow == 0L

CoalClosures %>%
	anti_join(CensusDims, by = c('state_fips', 'census_tract')) %>%
	nrow == 0L

#### Jibe with canonical dimension files ####

CoalClosuresFinal <-
	CoalClosures %>%
	# omit 110 problematic records, in which columns contradict each other
	semi_join(CensusDims) %>%
	# add county_geoid
	inner_join(
		CountyDims[c('county_geoid', 'state_fips', 'county_fips')],
		by = c('state_fips', 'county_fips')
	) %>%
	select(dataset_name, observation_id, state_fips, county_geoid, census_geoid, starts_with('is_'))

# Likewise, omit the five fossil fuels rows that don't match with our county dataset
FossilFuels %>%
	anti_join(CountyDims, by = c('state_fips', 'county_fips')) %>%
	nrow

FossilFuelsFinal <-
	FossilFuels %>%
	inner_join(CountyDims, by = c('state_fips', 'county_fips')) %>%
	select(dataset_name, observation_id, state_fips, county_geoid, starts_with('is_'))

CoalClosuresFinal %>%
	write_parquet('/Volumes/Extreme SSD/energy_communities/clean_input/observations/energy_communities_coal_closures.parquet')
FossilFuelsFinal %>%
	write_parquet('/Volumes/Extreme SSD/energy_communities/clean_input/observations/energy_communities_fossil_fuels.parquet')

# CoalClosuresFinal %>%
# 	write_csv('/Volumes/Extreme SSD/energy_communities/clean_input/observations/energy_communities_coal_closures.csv')
# FossilFuelsFinal %>%
# 	write_csv('/Volumes/Extreme SSD/energy_communities/clean_input/observations/energy_communities_fossil_fuels.csv')