import ee
import pandas as pd
import Satellite_Retro_Flow_Finder_v1
import geoglows
import geopandas as gpd
import s3fs
import xarray as xr


flow_occurance_df = pd.DataFrame(columns=['lat', 'lon', 'reach_id', '0_year', '2_year', "5_year", "10_year", "25_year", "50_year", "100_year"])
master_dates = pd.DataFrame(columns=['time', 'lat', 'lon', 'reach_id'])
latlons = pd.read_csv('/Users/ldp/Downloads/test_points.csv', header=None)

v2table = pd.read_parquet('/Users/ldp/Downloads/gv2Centroids.parquet')
# print(v2table)
def latlon_to_v2number(row):
    lat = row[0]
    lon = row[1]
    distance = (v2table['lat'] - lat)**2 + (v2table['lon'] - lon)**2
    v2 = v2table[distance == distance.min()]
    return v2['LINKNO'].values[0]

latlons['v2number'] = latlons.apply(latlon_to_v2number, axis=1)
v2numbers = latlons['v2number'].to_list()

bucket_uri = 's3://geoglows-v2-retrospective/retrospective.zarr'
region_name = 'us-west-2'
s3 = s3fs.S3FileSystem(anon=True, client_kwargs=dict(region_name=region_name))
s3store = s3fs.S3Map(root=bucket_uri, s3=s3, check=False)

# get historical simulation data
geohistzarr = xr.open_zarr(s3store)
geohistdf = geohistzarr['Qout'].sel(rivid=v2numbers).to_dataframe()
geohistdf = geohistdf.reset_index().set_index('time').pivot(columns='rivid', values='Qout')
geohistxr = geohistdf.to_xarray()
print(geohistxr)
# cache the historical simulation data
# gdf = gpd.read_file('/Users/ldp/Downloads/global_streams_simplified.gpkg')
# gdf[['lat', 'lon']] = gdf['geometry'].apply(lambda x: x.representative_point().coords[:][0])

ee.Initialize(project="fier-tana")

#inputs
for row in latlons.itertuples():
    lat = row[1]
    print(lat)
    lon = row[2]
    print(lon)
    v2number = row[3]
    print(v2number)
    return_period = 10

    # get streamflows from GeoGLOWS
    q_geo_xarray = geohistxr[v2number]
    q_geo_df = geohistdf[v2number]

    #get image dates
    dates_imgs = Satellite_Retro_Flow_Finder_v1.get_image_dates(lat, lon)

    print(dates_imgs)

    #match dates
    dates_flows_xr = Satellite_Retro_Flow_Finder_v1.match_dates(q_geo_xarray, dates_imgs)
    print(dates_flows_xr)

    #calculate return periods and filter
    flow_rp = Satellite_Retro_Flow_Finder_v1.filter_by_return_period(q_geo_df, dates_flows_xr, 10)

    #add flow occurances
    num_imgs = flow_rp[0]
    info = [lat, lon, v2number]
    info.extend(num_imgs)
    flow_occurance_df.loc[len(flow_occurance_df)] = info


    #add flow dates
    dates = flow_rp[1]
    if dates.empty == False:
        dates['flow'] = dates[v2number]
        dates.drop(v2number, axis=1, inplace=True)
        dates.loc[:, 'lat'] = lat
        dates.loc[:, 'lon'] = lon
        dates.loc[:, 'reach_id'] = v2number
        master_dates = pd.concat([master_dates, dates])
        print(dates)
    elif dates.empty == True:
        print('No dates found')

#export data
flow_occurance_df.to_csv('/Users/ldp/Downloads/flow_occurance.csv')
master_dates.to_csv('/Users/ldp/Downloads/master_dates.csv')
