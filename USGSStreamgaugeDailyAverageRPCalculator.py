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
        USGSROOT_path = config['USGS_Hist_Root_Path']
        USGS_Sites_Detailed_path = config['USGS_Sites_Detailed_Path']
        USGS_Sample_Sites_path = config['USGS_Sample_Sites_Path']

    rps = [2, 5, 10, 25, 50, 100]

    #loop through the USGSROOT_path to get the USGS sites
    USGS_Sites = []
    for root, dirs, files in os.walk(USGSROOT_path):
        for file in files:
            if file.endswith('.csv'):
                name = file.split('_')[0]
                USGS_Sites.append(int(name))

    #open the USGS sites detailed file
    USGS_Sites_Detailed = pd.read_csv(USGS_Sites_Detailed_path)
    for site in USGS_Sites:
        site_data = pd.read_csv(USGSROOT_path + str(site) + '_Q.csv', index_col='Datetime', na_values=-9999)
        site_data.index = pd.to_datetime(site_data.index)
        site_data = site_data.dropna()
        annual_max_flow_list = site_data.groupby(site_data.index.strftime('%Y')).max().values
        USGS_Sites_Detailed.loc[USGS_Sites_Detailed['site_no'] == site, 'xbar'] = np.mean(annual_max_flow_list)
        USGS_Sites_Detailed.loc[USGS_Sites_Detailed['site_no'] == site, 'std'] = np.std(annual_max_flow_list)
    for rp in rps:
        USGS_Sites_Detailed.loc[:, str(rp)] = round(-math.log(-math.log(1 - (1 / rp))) * USGS_Sites_Detailed['std'] * 0.7797 + USGS_Sites_Detailed['xbar'] - (0.45 * USGS_Sites_Detailed['std']), 2)

    USGS_Sites_Detailed = USGS_Sites_Detailed.loc[USGS_Sites_Detailed['site_no'].isin(USGS_Sites)]
    USGS_Sites_Detailed.to_csv(USGS_Sample_Sites_path, index=False)

