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

    # read the yaml config file
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
        reach_ids_path = config['reach_ids_path']
        Geoglows_Hist_Path = config['Geoglows_Hist_Path']
        return_period_path = config['return_period_path']
        SAR_dates_path = config['SAR_dates_path']
        Master_Table_Path = config['Master_Table_Path']

    #open the datasets
    geohistdf = pd.read_parquet(Geoglows_Hist_Path)
    latlons = pd.read_parquet(reach_ids_path)
    return_periods = pd.read_parquet(return_period_path)
    date_img_df = gpd.read_parquet(SAR_dates_path)

    print(return_periods.index)
    #prep the datasets
    date_img_df['time'] = pd.to_datetime(date_img_df['date'], format='%Y-%m-%d')
    date_img_df['time'] = date_img_df['time'].dt.strftime('%Y-%m-%d')
    date_img_df['time'] = pd.to_datetime(date_img_df['time'])
    date_img_df = date_img_df.reset_index().set_index('time').drop(columns=['index'])
    date_img_df['v2number'] = date_img_df['v2number'].astype(str)

    # join the datasets
    date_flow_df = pd.concat([date_img_df.loc[date_img_df['v2number'] == col, :].join(geohistdf[col], how='left').rename(columns={col:'flow'}) for col in geohistdf.columns])
    date_flow_df['v2number'] = date_flow_df['v2number'].astype(int)
    date_flow_rp_df = date_flow_df.join(return_periods, on='v2number')

    # classify the return periods
    rps = [2, 5, 10, 25, 50, 100]
    date_flow_rp_df.loc[:,'return_period'] = 0
    print(date_flow_rp_df.columns)
    print(date_flow_rp_df.head(10))
    for rp in rps:
        date_flow_rp_df.loc[date_flow_rp_df['flow']>date_flow_rp_df[rp],'return_period'] = rp
    date_flow_rp_df.columns = date_flow_rp_df.columns.astype(str)
    date_flow_rp_df.to_parquet(Master_Table_Path)

