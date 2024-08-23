library(tidyverse)
library(skimr)
library(readxl)

CountyDims <- read_csv('/Volumes/Extreme SSD/energy_communities/clean_input/county_dims.csv') %>%
	mutate(county_name_lower = str_to_lower(county_name))
StateDims <- read_csv('/Volumes/Extreme SSD/energy_communities/clean_input/state_dims.csv')

RawInvestGov <- readxl::read_excel('/Volumes/Extreme SSD/energy_communities/raw_input/Invest.gov_PublicInvestments_Map_Data_CURRENT.xlsx', sheet = 'ProjectsForMap')

InvestGov <-
	RawInvestGov %>%
	mutate(
		project_id = str_c('Invest.gov_', `Unique ID`),
	) %>%
	select(
		project_id,
		project_name = `Project Name`,
		county_name = County,
		state_name = State,
		funding_value = `Funding Amount`,
		funding_source_name = `Funding Source`,
	) %>%
	mutate(
		state_name = case_when(
			state_name == 'U.S. Virgin Islands' ~ 'United States Virgin Islands',
			TRUE ~ state_name
		)
	)

Munged <-
	InvestGov %>%
	select(project_id, state_name, county_name) %>%
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
		county_name_lower = str_replace(county_name_lower, '\\b(municipality)\\b', ''),
		county_name_lower = str_replace(county_name_lower, '\\b(region)\\b', ''),
		county_name_lower = str_replace(county_name_lower, '\\b(parish)$', ''),
		county_name_lower = str_trim(county_name_lower),
		county_name_lower = case_when(
			county_name_lower == 'chilton,jefferson' ~ 'chilton',
			county_name_lower == 'los angeles  metropolitan transportation authority' ~ 'los angeles',
			county_name_lower == 'valdez cordova' ~ 'chugach',
			state_fips == '09' & county_name_lower == 'fairfield' ~ 'greater bridgeport',
			state_fips == '09' & county_name_lower == 'hartford' ~ 'capitol',
			state_fips == '09' & county_name_lower == 'tolland' ~ 'capitol',
			state_fips == '09' & county_name_lower == 'litchfield' ~ 'northwest hills',
			state_fips == '09' & county_name_lower == 'middlesex' ~ 'lower connecticut river valley',
			state_fips == '09' & county_name_lower == 'new haven' ~ 'south central connecticut',
			state_fips == '09' & county_name_lower == 'new london' ~ 'southeastern connecticut',
			state_fips == '09' & county_name_lower == 'windham' ~ 'southeastern connecticut',
			state_fips == '12' & county_name_lower == 'broward metropolitan planning organization' ~ 'broward',
			state_fips == '12' & county_name_lower == 'dade' ~ 'miami-dade',
			state_fips == '13' & county_name_lower == 'athens-clarke' ~ 'clarke',
			state_fips == '13' & county_name_lower == 'augusta richmond' ~ 'richmond',
			state_fips == '13' & county_name_lower == 'gwinnett  board of commissioners' ~ 'gwinnett',
			state_fips == '15' & county_name_lower == 'kaua‘i' ~ 'kauai',
			state_fips == '22' & county_name_lower == 'st james' ~ 'st. james',
			state_fips == '22' & county_name_lower == 'st mary' ~ 'st. mary',
			state_fips == '25' & county_name_lower == 'ashby' ~ 'middlesex',
			state_fips == '35' & county_name_lower == 'dona ana' ~ 'doña ana',
			state_fips == '35' & county_name_lower == 'of dona ana' ~ 'doña ana',
			state_fips == '37' & county_name_lower == 'durham  government' ~ 'durham',
			state_fips == '39' & str_detect(county_name_lower, 'butler') ~ 'butler',
			state_fips == '39' & county_name_lower == 'dayton' ~ 'montgomery',
			state_fips == '42' & str_detect(county_name_lower, 'berks') ~ 'berks',
			state_fips == '42' & county_name_lower == 'schuykill' ~ 'schuylkill',
			state_fips == '51' & county_name_lower == 'southhampton' ~ 'southampton',
			TRUE ~ county_name_lower
			)
	) %>%
	left_join(
		CountyDims %>% select(state_fips, county_name_lower, county_fips),
		by = c('state_fips', 'county_name_lower'),
		relationship = 'many-to-one'
	)


JoinedData <-
	InvestGov %>%
	left_join(
		Munged %>%
			select(project_id, state_fips, county_fips),
		by = 'project_id',
		relationship = 'one-to-one'
	) %>%
	select(project_id, project_name, state_fips, county_fips, funding_source_name, funding_value)

JoinedData %>% 
	write_csv('/Volumes/Extreme SSD/energy_communities/clean_input/observations/investgov.csv')
