import argparse
import pandas as pd
import numpy as np
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
        reach_ids_path = config['reach_ids_path']
        v2_river_table_path = config['v2_river_table_path']
        v2_table_path = config['v2_table_path']

    df = pd.read_parquet(v2_river_table_path)
    v2_table_df = pd.read_parquet(v2_table_path)
    # pseudo_random_rivers_by_vpu = {}
    pseudo_random_rivers_by_vpu = set()
    for vpu in df['VPUCode'].unique():
        strmorders = df[df['VPUCode'] == vpu]['strmOrder'].unique()
        strmorders = [x for x in strmorders if x >= 5]
        # random_ids = set()
        for strmorder in strmorders:
            rows_to_random_sample = (
                df
                [np.logical_and(df['VPUCode'] == vpu, df['strmOrder'] == strmorder)]
                .sample(10, replace=True)
            )
            pseudo_random_rivers_by_vpu.update(rows_to_random_sample['LINKNO'].unique())
            # random_ids.update(rows_to_random_sample['LINKNO'].unique())
        # pseudo_random_rivers_by_vpu[vpu] = random_ids
    pseudo_random_rivers_by_vpu_df = pd.DataFrame(pseudo_random_rivers_by_vpu, columns=['v2number'])
    reach_id_df = pseudo_random_rivers_by_vpu_df.join(v2_table_df.set_index('LINKNO'), on='v2number').drop(columns=['geometry'])
    reach_id_df = reach_id_df[['lat', 'lon', 'v2number']]
    reach_id_df.to_csv(reach_ids_path, index=False)
