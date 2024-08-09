import pandas as pd
import yaml
import argparse
from multiprocessing import Pool
import os
from concurrent.futures import ThreadPoolExecutor
import urllib.request


def fetch_job_data(row, save_dir) -> None:
    #row = row[1]
    start_date = str(row['water_year']) + '-10-01'
    end_date = str(row['water_year'] + 1) + '-09-30'
    base_url = 'https://waterservices.usgs.gov/nwis/iv/?'
    url = f'{base_url}sites={row["site_no"]}&startDT={start_date}&endDT={end_date}&format=rdb'
    output_file_path = f'{save_dir}/{row["site_no"]}_{str(row['water_year'])}.csv'
    if os.path.exists(output_file_path):
        return
    try:
        (
            pd
            .read_csv(url, comment='#', delimiter='\t', dtype=str)
            .iloc[1:, :]
            .to_csv(output_file_path, index=False)
        )
    except Exception as e:
        print(e)
        print(f'Failed to fetch data for site {row["site_no"]}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, help='Configuration file', default='configs.yaml')
    args = parser.parse_args()
    config_file = args.config

    if config_file is None:
        raise ValueError("Please provide a configuration file")

    # read the yaml config file
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
        USGSROOT_path = os.path.join(config['USGS_Hist_Root_Path'], 'Sample')
        USGS_Sites_Detailed_path = config['USGS_Sites_Detailed_Path']
        USGS_Sample_Sites_path = config['USGS_Sample_Sites_Path']

    # read jobs df
    jobs_df = pd.read_parquet(USGS_Sample_Sites_path)
    jobs_df
    # number_of_workers = os.cpu_count()
    # with ThreadPoolExecutor(max_workers=number_of_workers) as executor:
    #     executor.map(fetch_job_data, [(x, USGSROOT_path) for x in jobs_df.iterrows()])

    with Pool() as p:
        p.starmap(fetch_job_data, [(x[1], USGSROOT_path) for x in jobs_df.iterrows()])
