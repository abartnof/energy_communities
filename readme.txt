
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
