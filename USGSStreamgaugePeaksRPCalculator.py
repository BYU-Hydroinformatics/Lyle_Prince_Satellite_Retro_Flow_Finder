import yaml
import argparse
import os
import pandas as pd
import numpy as np
import math

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
        USGShist = config['USGS_Hist_Path']
        USGS_Sites_Detailed_path = config['USGS_Sites_Detailed_Path']
        USGS_Sample_Sites_path = config['USGS_Sample_Sites_Path']

    #read data
    USGShist_df = pd.read_csv(USGShist, dtype={'site_no': str, '65330_00060': float})
    USGS_Sites_df = pd.read_csv(USGS_Sites_Detailed_path, dtype={'site_no': str})
    rps = [2, 5, 10, 25, 50, 100]

    #drop all rows with missing values in the peak_va column
    USGShist_df = USGShist_df.dropna(subset=['peak_va'])

    #calculate the return periods
    count = USGShist_df.groupby('site_no')['peak_va'].count().rename('count')
    mean = USGShist_df.groupby('site_no')['peak_va'].mean().rename('xbar')
    std = USGShist_df.groupby('site_no')['peak_va'].std().rename('std')

    #join the mean and std to the USGS_Sites_df
    USGS_Sites_df = USGS_Sites_df.merge(count, on='site_no', how='left').merge(mean, on='site_no', how='left').merge(std, on='site_no', how='left')
    USGS_Sites_df = USGS_Sites_df.dropna(subset=['std'])
    for rp in rps:
        USGS_Sites_df.loc[:, str(rp)] = round(-math.log(-math.log(1 - (1 / rp))) * USGS_Sites_df['std'] * 0.7797 + USGS_Sites_df['xbar'] - (0.45 * USGS_Sites_df['std']), 2)
    print(USGS_Sites_df)
    USGS_Sites_df.to_csv(USGS_Sites_Detailed_path.replace('.csv','RP.csv'), index=False)

