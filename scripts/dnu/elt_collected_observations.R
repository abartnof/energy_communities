# Map
# Histogram breakdown
# Triangle: ratio and distance
# https://cran.r-project.org/web/packages/alluvial/vignettes/alluvial.html

# Consolidated observations 
library(tidyverse)
library(skimr)
library(arrow)
library(sf)
library(alluvial)

fn_cr <- "/Volumes/Extreme SSD/energy_communities/clean_input/observations/ira/clean_repowering_clean.parquet"
fn_cim <- '/Volumes/Extreme SSD/energy_communities/clean_input/observations/clean_investment_monitor_manufacturing.parquet'
fn_state_dims <- "/Volumes/Extreme SSD/energy_communities/clean_input/geography/state_dims.csv"
fn_state_shapes <- "/Volumes/Extreme SSD/energy_communities/clean_input/geography/state_shapes.shp"


CrRaw <- read_parquet(fn_cr)
CimRaw <- read_parquet(fn_cim)
StateDims <- read_csv(fn_state_dims)
StateShapes <- read_sf(fn_state_shapes)



CrCoords <-
	CrRaw %>%
	select(dataset_name, capacity_mw, longitude, latitude) %>%
	drop_na
CrCoords <- st_as_sf(CrCoords, coords=c('longitude', 'latitude'))
st_crs(CrCoords) <- st_crs(StateShapes)

CimCoords <-
	CimRaw %>%
	select(dataset_name, longitude, latitude, total_facility_capex_estimated) %>%
	drop_na
CimCoords <- st_as_sf(CimCoords, coords=c('longitude', 'latitude'))
st_crs(CimCoords) <- st_crs(StateShapes)

PlotmeStateShapes <-
	StateShapes %>%
	left_join(StateDims, by = 'state_fips') %>%
	filter(state_name != 'Alaska', state_name != 'Puerto Rico', state_name != 'Hawaii') %>%
	select(state_name, geometry)

JoinmeCrCoords <-
	CrCoords %>%
	rename(value = capacity_mw) %>%
	mutate(label = 'Clean Repowering: Capacity MW') %>%
	select(geometry, label, value)

JoinmeCimCoords <-
	CimCoords %>%
	rename(value = total_facility_capex_estimated) %>%
	mutate(label = 'Clean Investment Monitor: Total Facility Capex (Estimated)') %>%
	select(geometry, label, value)

CollectedCoords <-
	JoinmeCrCoords %>%
	bind_rows(JoinmeCimCoords) %>%
	group_by(label) %>%
	mutate(value_std = as.vector(scale(value)))

ggplot(PlotmeStateShapes) +
	geom_sf() +
	geom_sf(data = CollectedCoords, aes(size = value_std, color = label), alpha = 0.5) +
	coord_sf(xlim = c(-125, -65), ylim = c(22, 50)) +
	theme(legend.position = 'bottom', 
				panel.grid = element_blank(),
				panel.background = element_blank(), 
				axis.ticks = element_blank()
	) +
	guides(color = guide_legend(title = "Dataset"), size = guide_legend(title = 'Std. Dev.'))
#
# Distributions
CollectedCoords %>%
	sf::st_drop_geometry() %>%
	select(label, value) %>%
	ggplot(aes(x = value)) +
	geom_histogram() +
	facet_wrap(~label, scales = 'free', ncol = 1) +
	scale_x_continuous(labels = scales::comma_format(1)) +
	scale_y_continuous(labels = scales::comma_format(1)) +
	labs(x = '', y = 'n', title = 'Histogram of key metrics')

# Breakdown
TableCim <-
	CimRaw %>%
	mutate(
		segment = fct_infreq(segment),
		project_type = fct_infreq(project_type),
		technology = fct_infreq(technology),
		technology = forcats::fct_lump_n(technology, 5),
	) %>%
	count(segment, project_type, technology)

TechnologyColors <-
	TableCim %>%
	distinct(technology) %>%
	mutate(color = RColorBrewer::brewer.pal(5, name='Set1'))

TableCimColored <-
	TableCim %>%
	left_join(TechnologyColors)

alluvial(TableCimColored[,1:3], freq=TableCim$n, 
				 col = TableCimColored$color)
#	
CrRaw %>%
	select(technology, capacity_mw) %>%
	ggplot(aes(x = technology, y = capacity_mw)) +
	geom_boxplot() +
	scale_y_continuous(labels = scales::comma_format(1)) +
	labs(x = 'Technology', y = 'Capacity MW', title = 'Clean Repowering')
#



Cim <-
	CimRaw %>%
	left_join(StateDims, by = 'state_name_abbrev') %>%
	select(dataset_name, state_name, observation_id,
				 census_tract,
				 segment, company, technology, subcategory, project_type, status_description, total_facility_capex_estimated
				 ) %>%
	drop_na

Cr %>%
	bind_rows(Cim) %>%
	write_parquet(fn_out)
