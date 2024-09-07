library(tidyverse)
library(skimr)
library(readxl)
library(arrow)

dir_clean <- '/Volumes/Extreme SSD/energy_communities/clean_input/observations/'
CountyDims <- read_csv('/Volumes/Extreme SSD/energy_communities/clean_input/geography/county_dims.csv')
CountyDims_UniqueCountyNames <- read_csv('/Volumes/Extreme SSD/energy_communities/clean_input/geography/county_dims_unique_county_names.csv') %>%
	mutate(county_name_lower = str_to_lower(county_name))
StateDims <- read_csv('/Volumes/Extreme SSD/energy_communities/clean_input/geography/state_dims.csv')

RawInvestGov <- readxl::read_excel('/Volumes/Extreme SSD/energy_communities/raw_input/Invest.gov_PublicInvestments_Map_Data_CURRENT.xlsx', sheet = 'ProjectsForMap')


# QC: `Unique ID` does function as a unique id
RawInvestGov %>%
	count(`Unique ID`) %>%
	filter(n > 1)

#### Clean up Invest.gov dataset ####
RawInvestGov %>%
	glimpse

# 1. initial munging-- get the right columns
InvestGov <-
	RawInvestGov %>%
	mutate(
		observation_id = str_c('Invest.gov_', `Unique ID`),
		dataset_name = 'Invest.gov'
	) %>%
	select(
		dataset_name,
		observation_id,
		county_name = County,
		state_name = State,
		agency_name = `Agency Name`,
		bureau_name = `Bureau Name`,
		program_name = `Program Name`,
		project_name = `Project Name`,
		category = Category,
		subcategory = Subcategory,
		funding_source = `Funding Source`,
		funding_amount = `Funding Amount`,
		project_name = `Project Name`,
	) %>%
	mutate(
		state_name = case_when(
			state_name == 'U.S. Virgin Islands' ~ 'United States Virgin Islands',
			TRUE ~ state_name
		)
	)

# 2. real munging-- make sure county_names conform to our datasets
MungedResults <-
	InvestGov %>%
	select(observation_id, state_name, county_name) %>%
	# Add state fips
	left_join(
		StateDims %>% select(state_name, state_fips), 
		by = 'state_name',
		relationship = 'many-to-one'
	) %>%
	# Create a cleaned up new column, county_name_lower, which can match easily
	# with the CountyDims table
	mutate(
		county_name_lower = str_to_lower(county_name),
		county_name_lower = str_replace(county_name_lower, '\\(.*\\)', ''),
		county_name_lower = str_replace(county_name_lower, '\\b(county)\\b', ''),
		county_name_lower = str_replace(county_name_lower, '\\b(borough)\\b', ''),
		county_name_lower = str_replace(county_name_lower, '\\b(census)\\b', ''),
		county_name_lower = str_replace(county_name_lower, '\\b(area)\\b', ''),
		county_name_lower = str_replace(county_name_lower, '\\b(municipal)\\b', ''),
		county_name_lower = str_replace(county_name_lower, '\\b(region)\\b', ''),
		county_name_lower = str_replace(county_name_lower, '\\b(parish)$', ''),
		county_name_lower = str_trim(county_name_lower),
		county_name_lower = str_replace_all(county_name_lower, '\\s{2,}', ' '),
		county_name_lower = case_when(
			state_fips == '01' & county_name_lower == 'chilton,jefferson' ~ 'chilton',
			state_fips == '02' & county_name_lower == 'anchorage municipality' ~ 'anchorage',
			state_fips == '02' & county_name_lower == 'chugach' ~ 'matanuska-susitna',
			state_fips == '02' & county_name_lower == 'copper river' ~ 'lake and peninsula',
			state_fips == '02' & county_name_lower == 'valdez cordova' ~ 'valdez-cordova',
			state_fips == '06' & str_detect(county_name_lower, 'los angeles') ~ 'los angeles',
			state_fips == '09' & county_name_lower == 'capitol' ~ 'hartford',
			state_fips == '09' & county_name_lower == 'naugatuck valley' ~ 'litchfield',  # ambiguous, could be an adjacent county
			state_fips == '09' & county_name_lower == 'western connecticut' ~ 'fairfield',  # ambiguous, could be an adjacent county
			state_fips == '12' & str_detect(county_name_lower, 'broward') ~ 'broward',
			# county_name_lower == 'chilton,jefferson' ~ 'chilton',
			# county_name_lower == 'los angeles  metropolitan transportation authority' ~ 'los angeles',
			# county_name_lower == 'valdez cordova' ~ 'chugach',
			# state_fips == '09' & county_name_lower == 'fairfield' ~ 'greater bridgeport',
			# state_fips == '09' & county_name_lower == 'hartford' ~ 'capitol',
			# state_fips == '09' & county_name_lower == 'tolland' ~ 'capitol',
			# state_fips == '09' & county_name_lower == 'litchfield' ~ 'northwest hills',
			# state_fips == '09' & county_name_lower == 'middlesex' ~ 'lower connecticut river valley',
			# state_fips == '09' & county_name_lower == 'new haven' ~ 'south central connecticut',
			# state_fips == '09' & county_name_lower == 'new london' ~ 'southeastern connecticut',
			# state_fips == '09' & county_name_lower == 'windham' ~ 'southeastern connecticut',
			# state_fips == '12' & county_name_lower == 'broward metropolitan planning organization' ~ 'broward',
			# state_fips == '12' & county_name_lower == 'dade' ~ 'miami-dade',
			state_fips == '13' & county_name_lower == 'athens-clarke' ~ 'clarke',
			state_fips == '13' & county_name_lower == 'augusta richmond' ~ 'richmond',
			state_fips == '13' & county_name_lower == 'gwinnett board of commissioners' ~ 'gwinnett',
			state_fips == '15' & county_name_lower == 'kaua‘i' ~ 'kauai',
			state_fips == '22' & county_name_lower == 'st james' ~ 'st. james',
			state_fips == '22' & county_name_lower == 'st mary' ~ 'st. mary',
			state_fips == '25' & county_name_lower == 'ashby' ~ 'middlesex',
			state_fips == '35' & county_name_lower == 'dona ana' ~ 'doña ana',
			state_fips == '35' & county_name_lower == 'of dona ana' ~ 'doña ana',
			state_fips == '37' & county_name_lower == 'durham government' ~ 'durham',
			state_fips == '39' & str_detect(county_name_lower, 'butler') ~ 'butler',
			state_fips == '39' & county_name_lower == 'dayton' ~ 'montgomery',
			state_fips == '42' & str_detect(county_name_lower, 'berks') ~ 'berks',
			state_fips == '42' & county_name_lower == 'schuykill' ~ 'schuylkill',
			state_fips == '51' & county_name_lower == 'southhampton' ~ 'southampton',
			TRUE ~ county_name_lower
			)
	) %>%
	left_join(
		CountyDims_UniqueCountyNames %>% select(state_fips, county_name_lower, county_fips),
		by = c('state_fips', 'county_name_lower'),
		relationship = 'many-to-one'
	)

# QC: Make sure we didn't get fanout, etc:
nrow(InvestGov) == nrow(MungedResults)


#### Get geoids for each munged record
MungedResults %>%
	select(state_fips, county_fips) %>%
	is.na %>% colMeans()

MungedWithGeoid <-
	MungedResults %>%
	left_join(
		CountyDims[c('state_fips', 'county_fips', 'county_geoid')],
		c('state_fips', 'county_fips')
	) %>%
	select(observation_id, state_fips, county_fips, county_geoid)

InvestGovFinal <-
	InvestGov %>%
	left_join(
		MungedWithGeoid,
		by = 'observation_id'
	) %>%
	relocate(dataset_name, observation_id, state_fips, county_geoid)

InvestGovFinal

InvestGovFinal %>%
	write_parquet(file.path(dir_clean, 'invest_gov.parquet'))
	# write_csv(file.path(dir_clean, 'invest_gov.csv'))
