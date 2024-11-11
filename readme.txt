
##############################
# STATE SHAPE FILES: #
##############################
(low resolution):
1 : 20,000,000 (national)   shapefile [2.2 MB]  |  geodatabase [1.5 MB]
https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html

################
# County files #
################
https://www.census.gov/geographies/mapping-files/time-series/geo/carto-boundary-file.1990.html#list-tab-1556094155
https://www.census.gov/geographies/mapping-files/time-series/geo/carto-boundary-file.2010.html#list-tab-1556094155
https://www.census.gov/geographies/mapping-files/time-series/geo/carto-boundary-file.html

######################
# Census tract files #
######################
Using google census files:
SELECT DISTINCT 
  state_name,
  state_fips_code AS state_fips, 
  county_fips_code AS county_fips, 
  tract_ce AS census_tract,
  geo_id as census_geoid
FROM `bigquery-public-data.geo_census_tracts.us_census_tracts_national` 
ORDER BY state_fips_code, county_fips_code, geo_id



DNU:
https://www2.census.gov/geo/tiger/TIGER_RD18/LAYER/TRACT/
retrieved via:
wget -r -l1 -H -t1 -nd -N -np -A.zip -erobots=off https://www2.census.gov/geo/tiger/TIGER_RD18/LAYER/TRACT/
explained at:
https://stackoverflow.com/questions/13533217/how-to-download-all-links-to-zip-files-on-a-given-web-page-using-wget-curl

documentation:
https://www2.census.gov/geo/pdfs/maps-data/data/tiger/tgrshp2023/TGRSHP2023_TechDoc_F-S.pdf
appendix g3


###############################
# Clean Energy Tax Credit Map #
###############################
https://edx.netl.doe.gov/dataset/ira-energy-community-data-layers
layers:
Coal_Closures_EnergyComm_v2024_1.zip
MSA_NMSA_EC_FFE_v2024_1.zip


####################################
# IWG Report on Energy Communities #
####################################

https://edx.netl.doe.gov/dataset/?q=IWG

https://edx.netl.doe.gov/dataset/datasets-for-iwg-report-on-energy-communities
iwg_energycommunitiesdatasets_csv_april2021.zip

https://edx.netl.doe.gov/dataset/section-48c-tax-credits-designated-energy-communities
48c_data.zip



# CBSA and CSA #
2024https://www2.census.gov/geo/tiger/TIGER2024/

core based statistical area (CBSA) is an urban center with a lot of commuters coming in.
CBSAs can be combined if they overlap (combine statistical areas, CSA)
CBSAs are broken down based on size of central city: metropolitan/micropolitan statistical areas