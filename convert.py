import geopandas as gpd
import pandas as pd
# gpd.read_file('/Users/ldp/Downloads/Gv2CentroidsFinal.gpkg')
# gdf = gpd.read_file('/Users/ldp/Downloads/Gv2CentroidsFinal.gpkg')
# gdf.to_parquet('/Users/ldp/Downloads/gv2Centroids.parquet')
#pd = pd.read_csv('/Users/ldp/Downloads/USGSData.csv', header=0, dtype={'site_no': int, '65330_00060': str})
# rskip = list(range(0, 13535))
# rskip.remove(13533)
# pd = pd.read_csv('/Users/ldp/Downloads/download (1)', skiprows=rskip, skipfooter=1, engine='python', sep='	', usecols=['agency_cd', 'site_no', 'datetime', 'tz_cd', '65330_00060', '65330_00060_cd'], dtype={'site_no': int, '65330_00060': float}, na_values=[''])
# print(pd.columns)
# pd.columns = pd.columns.astype(str)
# pd.to_parquet('/Users/ldp/Downloads/USGSData.parquet')
# pd.read_csv('/Users/ldp/Downloads/USGSSitesDetailed.csv').to_parquet('/Users/ldp/Downloads/USGSSitesDetailed.parquet')
# rskip = list(range(0, 11613))
# rskip.remove(11611)
# pd = pd.read_csv('/Users/ldp/Downloads/peak', skiprows=rskip, engine='python', sep='	', usecols=['agency_cd', 'site_no', 'peak_dt', 'peak_tm', 'peak_va', 'peak_cd', 'gage_ht', 'gage_ht_cd', 'year_last_pk', 'ag_dt', 'ag_tm', 'ag_gage_ht'], dtype={'site_no': int, '65330_00060': float}, na_values=[''])
# print(pd.columns)
# pd.columns = pd.columns.astype(str)
# pd.to_csv('/Users/ldp/Downloads/USGSPeakData.csv', index=False)

# rskip = list(range(0, 30263))
# rskip.remove(30261)
# df = pd.read_csv('/Users/ldp/Downloads/USGSPeakDataComplete', skiprows=rskip, engine='python', sep='	', usecols=['agency_cd', 'site_no', 'peak_dt', 'peak_tm', 'peak_va', 'peak_cd', 'gage_ht', 'gage_ht_cd', 'year_last_pk', 'ag_dt', 'ag_tm', 'ag_gage_ht'], dtype={'site_no': str, '65330_00060': float}, na_values=[''])
# #convert site_no to string
# df['site_no'] = df['site_no'].astype(str)
# print(df.columns)
# df.columns = df.columns.astype(str)
# df.to_csv('/Users/ldp/Downloads/USGSPeakDataComplete.csv', index=False)
#
# rskip = list(range(0, 77))
# rskip.remove(75)
# dtypes = {'agency_cd': str, 'site_no': str, 'station_nm': str, 'site_tp_cd': str, 'lat_va': float, 'long_va': float, 'dec_lat_va': float, 'dec_long_va': float, 'coord_meth_cd': str, 'coord_acy_cd': str, 'coord_datum_cd': str, 'dec_coord_datum_cd': str, 'district_cd': int, 'state_cd': int, 'county_cd': int, 'country_cd': str, 'land_net_ds': str, 'map_nm': str, 'map_scale_fc': float, 'alt_va': float, 'alt_meth_cd': str, 'alt_acy_va': float, 'alt_datum_cd': str, 'huc_cd': float, 'basin_cd': float, 'topo_cd': str, 'data_types_cd': str, 'instruments_cd': object, 'construction_dt': float, 'inventory_dt': float, 'drain_area_va': float, 'contrib_drain_area_va': float, 'tz_cd': str, 'local_time_fg': str, 'reliability_cd': str, 'gw_file_cd': str, 'nat_aqfr_cd': str, 'aqfr_cd': str, 'aqfr_type_cd': str, 'well_depth_va': float, 'hole_depth_va': float, 'depth_src_cd': float, 'project_no': str, 'rt_bol': int, 'peak_begin_date': str, 'peak_end_date': str, 'peak_count_nu': int, 'qw_begin_date': str, 'qw_end_date': str, 'qw_count_nu': int, 'gw_begin_date': str, 'gw_end_date': str, 'gw_count_nu': int, 'sv_begin_date': str, 'sv_end_date': str, 'sv_count_nu': int, 'count': float, 'xbar': float, 'std': float, '2': float, '5': float, '10': float, '25': float, '50': float, '100': float}
# df = pd.read_csv('/Users/ldp/Downloads/USGSPeakDetailedSites', skiprows=rskip, engine='python', sep='	', usecols=['agency_cd', 'site_no', 'station_nm', 'site_tp_cd', 'lat_va', 'long_va', 'dec_lat_va', 'dec_long_va', 'coord_meth_cd', 'coord_acy_cd', 'coord_datum_cd', 'dec_coord_datum_cd', 'district_cd', 'state_cd', 'county_cd', 'country_cd', 'land_net_ds', 'map_nm', 'map_scale_fc', 'alt_va', 'alt_meth_cd', 'alt_acy_va', 'alt_datum_cd', 'huc_cd', 'basin_cd', 'topo_cd', 'data_types_cd', 'instruments_cd', 'construction_dt', 'inventory_dt', 'drain_area_va', 'contrib_drain_area_va', 'tz_cd', 'local_time_fg', 'reliability_cd', 'gw_file_cd', 'nat_aqfr_cd', 'aqfr_cd', 'aqfr_type_cd', 'well_depth_va', 'hole_depth_va', 'depth_src_cd', 'project_no', 'rt_bol', 'peak_begin_date', 'peak_end_date', 'peak_count_nu', 'qw_begin_date', 'qw_end_date', 'qw_count_nu', 'gw_begin_date', 'gw_end_date', 'gw_count_nu', 'sv_begin_date', 'sv_end_date', 'sv_count_nu'],  na_values=[''], dtype=dtypes)
# #convert site_no to string
# df['site_no'] = df['site_no'].astype(str)
# print(df.columns)
# df.columns = df.columns.astype(str)
# df.to_csv('/Users/ldp/Downloads/USGSPeakDetailedSites.csv', index=False)


# def fetch_job_data(row) -> None:
#     row = row[1]
#     save_dir = '/Users/ldp/Downloads/USGSData'
#     start_date = row['water_year'] + '-10-01'
#     end_date = str(int(row['water_year']) + 1) + '-09-30'
#     base_url = 'https://waterservices.usgs.gov/nwis/iv/?'
#     url = f'{base_url}sites={row["site_no"]}&startDT={start_date}&endDT={end_date}&format=rdb'
#     pd.read_csv(url, comment='#', delimiter='\t').iloc[1:, :].to_csv(f'{save_dir}/{row["site_no"]}.csv', index=False)
#
#
# from multiprocessing import Pool
#
#
# if __name__ == '__main__':
#     # read jobs df
#     jobs_df = pd.read_parquet('/Users/ldp/Downloads/USGS_Sample_Sites.parquet')
#     with Pool() as p:
#         p.map(fetch_job_data, jobs_df.iterrows())


# read netcdf files and print the first 5 row
import xarray as xr
ds = xr.open_dataset('/Users/ldp/Downloads/nwm_2018_2023_7469392_14073444.nc')
ds_bc = xr.open_dataset('/Users/ldp/Downloads/nwm_2018_2023_7469392_14073444_bias_corrected.nc')
print(ds)
