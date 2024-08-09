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

    #read and filterpeak data
    peak_df = pd.read_csv(USGShist, dtype={'site_no': str, 'peak_va': float})
    peak_df = peak_df[~peak_df['peak_dt'].str.endswith('-00')]
    peak_df['peak_dt'] = pd.to_datetime(peak_df['peak_dt'])
    peak_df = peak_df[peak_df['peak_dt'] > pd.to_datetime('2014-01-01')]
    peak_df['water_year'] = peak_df['peak_dt'].apply(lambda x: x.year + (x.month >= 10))

    # classify the peak flow data into the return periods classes from the sample sites data
    rps = [2, 5, 10, 25, 50, 100]
    sites_df = pd.read_csv(USGS_Sites_Detailed_path.replace('.csv','RP.csv'),dtype={'site_no': str, 'peak_va': float})
    peak_df = peak_df.merge(sites_df, on='site_no', how='left', suffixes=('_sample', '_peak'))

    # classify the return periods
    peak_df.loc[:, 'return_period'] = 0
    for rp in rps:
        peak_df.loc[peak_df['peak_va'] > peak_df[str(rp)], 'return_period'] = rp
    peak_df.columns = peak_df.columns.astype(str)
    peak_df = peak_df.drop(columns=['xbar', 'std', 'station_nm', 'site_tp_cd', 'lat_va', 'long_va', 'dec_lat_va', 'dec_long_va', 'coord_meth_cd', 'coord_acy_cd', 'coord_datum_cd', 'dec_coord_datum_cd', 'district_cd', 'state_cd'])

    #get max return period for each site
    max_return_period = peak_df.groupby('site_no')['return_period'].max().rename('max_return_period')
    #get list of sites with max return period greater or equal to 25
    sample_sites = max_return_period[max_return_period >= 25].index
    #filter peak_df to only include sites from sample sites
    peak_df = peak_df[peak_df['site_no'].isin(sample_sites)]
    print(peak_df['return_period'].value_counts())
    peak_df = peak_df[peak_df['return_period'] >= 10]
    jobs_df = peak_df[['site_no', 'peak_dt', 'peak_va', 'return_period', 'water_year']]

    jobs_df.to_parquet(USGS_Sample_Sites_path)
