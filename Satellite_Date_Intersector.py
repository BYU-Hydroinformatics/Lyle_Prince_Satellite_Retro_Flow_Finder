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

    years = [2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
    #match the points with the images in every year
    for i in years:
        imgs_dates = gpd.read_parquet(Master_SAR_dates_path + str(i) + '_DES.parquet')
        imgs_dates.to_file(Master_SAR_dates_path + str(i) + '.gpkg', driver='GPKG', index=False)
        points_dates_gdf = gpd.sjoin(points_gdf, imgs_dates, how='inner', predicate='covered_by')
        points_dates_gdf = points_dates_gdf.rename(columns={'index_right': 'date'})
        export_gdf = pd.concat([export_gdf, points_dates_gdf])
        

    export_gdf.to_parquet(SAR_dates_path, index=False)
    export_gdf.to_file(SAR_dates_path.replace('.parquet', '.gpkg'), driver='GPKG', index=False)
    print("SAR dates saved to ", SAR_dates_path)
    test_gdf = gpd.read_parquet(SAR_dates_path)
    # print(test_gdf.columns)
    # print data for v2number 710625280
    v2numbers = test_gdf['v2number'].unique()
    count = []
    for i in v2numbers:
        count.append(test_gdf[test_gdf['v2number'] == i].shape[0])
    print(count)
    print(min(count), statistics.mean(count), statistics.median(count), max(count), statistics.quantiles(count, n=4))
    #print(test_gdf[test_gdf['v2number'] == 710625280].reset_index(drop=True))


