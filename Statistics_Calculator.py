import yaml
import pandas as pd
import geopandas as gpd
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, help='Configuration file', default='configs.yaml')
    args = parser.parse_args()
    config_file = args.config

    if config_file is None:
        raise ValueError("Please provide a configuration file")

    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
        return_period = config['return_period']
        master_table_path = config['Master_Table_Path']
        flow_occurance_path = config['flow_occurance_path']
        master_dates_path = config['master_dates_path']

    master_table_df = pd.read_parquet(master_table_path)

    # flow_min_max_df = pd.DataFrame(master_table_df.groupby(['v2number', 'pass'])[['flow']].agg(['min', 'max'])['flow'])
    # flow_occurance_df = pd.DataFrame(master_table_df.groupby(['v2number', 'pass', 'return_period'])[['flow']].count())
    # flow_occurance_df = flow_occurance_df.reset_index().pivot(columns='return_period', values='flow', index=['v2number', 'pass'])
    # flow_occurance_df = flow_occurance_df.join(flow_min_max_df)
    # flow_occurance_df.to_parquet(flow_occurance_path)
    # print('Flow occurance saved to ', flow_occurance_path)

    master_dates_df = master_table_df.loc[master_table_df['return_period'] >= return_period]
    master_dates_df.to_parquet(master_dates_path)
    master_dates_df.to_csv(master_dates_path.replace('.parquet', '.csv'))
    print('Master dates saved to ', master_dates_path)
