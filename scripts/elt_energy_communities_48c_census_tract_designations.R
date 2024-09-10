library(tidyverse)
library(skimr)
# library(readxl)
library(arrow)

dir_clean <- '/Volumes/Extreme SSD/energy_communities/clean_input/observations/'
# CountyDims <- read_csv('/Volumes/Extreme SSD/energy_communities/clean_input/geography/county_dims.csv')
# CountyDims_UniqueCountyNames <- read_csv('/Volumes/Extreme SSD/energy_communities/clean_input/geography/county_dims_unique_county_names.csv') %>%
# 	mutate(county_name_lower = str_to_lower(county_name))
# StateDims <- read_csv('/Volumes/Extreme SSD/energy_communities/clean_input/geography/state_dims.csv')
CensusDims <- read_csv('/Volumes/Extreme SSD/energy_communities/clean_input/geography/bigquery_census_dims.csv')

FortyEightCRaw <- read_csv('/Volumes/Extreme SSD/energy_communities/raw_input/IWG/48C_data/48C_CensusTractDesignation.csv', 
													 col_types = cols(F48C_Statu = 'd', date_last_ = 'D', .default = col_character())
												 )

#### Munge ####
FortyEightCRaw %>% 
	glimpse

# OID works as a UID
FortyEightCRaw %>%
	count(OID_) %>%
	filter(n != 1)

FortyEightC <-
	FortyEightCRaw %>%
		mutate(
			observation_id = str_c('energy_communities_48c_census_tract_designations_', OID_),
			dataset_name = 'energy_communities_48c_census_tract_designations_'
		) %>%
		select(
			dataset_name,
			observation_id,
			census_geoid = CTract_GEO,
			# state_fips = State_FIP,
			# county_fips = County_FIP,
			# census_tract = Tract_FIP,
			f48c_tract = F48C_Tract,
			f48c_status = F48C_Statu,
			label = Label) %>%
	mutate(
		f48c_status = round(f48c_status),
		f48c_status = case_when(
			f48c_status == 1L ~ 'Census tract is eligible for 48C tax credit as a designated energy community',
			f48c_status == 0L ~ 'Census tract is eligible for 48C tax credit but NOT as a designated energy community',
			TRUE ~ 'DEFAULT_UNKNOWN'
			),
		f48c_tract = case_when(
			f48c_tract == 'N/A' ~ NA,
			TRUE ~ f48c_tract
		)
	) %>%
	relocate(f48c_status, .before = f48c_tract) %>%
	semi_join(CensusDims, by = 'census_geoid') 

num_bad_rows <-
	FortyEightC %>%
	anti_join(CensusDims, by = c('census_geoid')) %>%
	nrow
# No bad rows
num_bad_rows / nrow(FortyEightCRaw)

# Lots of missing data in f48c_tract
FortyEightC %>%
	is.na %>% colMeans

FortyEightC %>%
	write_parquet(file.path(dir_clean, 'energy_communities_48c_census_tract_designations.parquet'))