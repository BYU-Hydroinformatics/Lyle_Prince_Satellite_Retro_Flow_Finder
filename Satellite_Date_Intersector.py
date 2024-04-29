import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import yaml
import argparse
import statistics

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, help='Configuration file', default='configs.yaml')
    args = parser.parse_args()
    config_file = args.config

    if config_file is None:
        raise ValueError("Please provide a configuration file")

    # read the yaml config file
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
        reach_ids_path = config['reach_ids_path']
        return_period = config['return_period']
        SAR_dates_path = config['SAR_dates_path']
        Master_SAR_dates_path = config['Master_SAR_dates_path']

    latlons_df = pd.read_csv(reach_ids_path)
    points_gdf = gpd.GeoDataFrame(latlons_df, geometry=gpd.points_from_xy(latlons_df.lon, latlons_df.lat))
    export_gdf = gpd.GeoDataFrame()
    orbits = ['ASC', 'DES']
    years = [2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    #match the points with the images in every year
    for i in years:
        for j in months:
            imgs_dates = gpd.read_parquet(Master_SAR_dates_path + str(i) + '_' + str(j) + '.parquet')
            print(imgs_dates.head())
            print(points_gdf.columns)
            points_dates_gdf = gpd.sjoin(points_gdf, imgs_dates, how='inner', predicate='covered_by')
            points_dates_gdf = points_dates_gdf.rename(columns={'index_right': 'date'})
            export_gdf = pd.concat([export_gdf, points_dates_gdf])

    export_gdf['date'] = pd.to_datetime(export_gdf['date'])
    export_gdf.to_parquet(SAR_dates_path, index=False)
    # export_gdf.to_file(SAR_dates_path.replace('.parquet', '_' + j + '.gpkg'), driver='GPKG', index=False)
    print("SAR dates saved to ", SAR_dates_path)
    # test_gdf = gpd.read_parquet(SAR_dates_path)
    # print(test_gdf.columns)
    # print data for v2number 710625280
    v2numbers = export_gdf['v2number'].unique()
    count = []
    for i in v2numbers:
        count.append(export_gdf[export_gdf['v2number'] == i].shape[0])
    print(count)
    print(min(count), statistics.mean(count), statistics.median(count), max(count), statistics.quantiles(count, n=4))
    #print(test_gdf[test_gdf['v2number'] == 710625280].reset_index(drop=True))


