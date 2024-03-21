import geopandas as gpd
# gpd.read_file('/Users/ldp/Downloads/Gv2CentroidsFinal.gpkg')
gdf = gpd.read_file('/Users/ldp/Downloads/Gv2CentroidsFinal.gpkg')
gdf.to_parquet('/Users/ldp/Downloads/gv2Centroids.parquet')
