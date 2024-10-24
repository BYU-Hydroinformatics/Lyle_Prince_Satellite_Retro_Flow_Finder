import argparse
# import Satellite_Retro_Flow_Finder_v1
import pandas as pd
import yaml


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
        input_points_path = config['input_points_path']
        v2_table_path = config['v2_table_path']
        reach_ids_path = config['reach_ids_path']

    def latlon_to_v2number(row):
        lat = row[1]
        lon = row[2]
        distance = (v2table['lat'] - lat) ** 2 + (v2table['lon'] - lon) ** 2
        v2 = v2table[distance == distance.min()]
        return v2['LINKNO'].values[0]

    # read the inputs
    v2table = pd.read_parquet(v2_table_path)
    latlons = pd.read_csv(input_points_path, names=['lat', 'lon'])

    # apply the function to the latlons dataframe and save to list
    latlons["v2number"] = latlons.apply(latlon_to_v2number, axis=1)
    # filter to unique values
    latlons = latlons.drop_duplicates(subset=['v2number'])

    # drop column names
    latlons.to_csv(reach_ids_path, index=False)
    print("Reach IDs saved to ", reach_ids_path)
