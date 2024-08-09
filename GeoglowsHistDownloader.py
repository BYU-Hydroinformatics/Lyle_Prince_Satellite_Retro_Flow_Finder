import argparse
import pandas as pd
import yaml
import s3fs
import xarray as xr
import geoglows
import os

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

    if os.path.exists(Geoglows_Hist_Path):
        geohistdf = pd.read_parquet(Geoglows_Hist_Path)
    else:
        # read the inputs
        latlons = pd.read_parquet(reach_ids_path)
        v2numbers = latlons['v2number']

        #set up AWS
        bucket_uri = 's3://geoglows-v2-retrospective/retrospective.zarr'
        region_name = 'us-west-2'
        s3 = s3fs.S3FileSystem(anon=True, client_kwargs=dict(region_name=region_name))
        s3store = s3fs.S3Map(root=bucket_uri, s3=s3, check=False)

        # get historical simulation data
        geohistzarr = xr.open_zarr(s3store)
        geohistdf = geohistzarr['Qout'].sel(rivid=v2numbers.values).to_dataframe()
        geohistdf = geohistdf.reset_index().pivot(columns='rivid', values='Qout', index='time')
        geohistdf.columns = geohistdf.columns.astype(str)
        geohistdf.to_parquet(Geoglows_Hist_Path)
        print("Historical simulation data saved to ", Geoglows_Hist_Path)

    if os.path.exists(return_period_path):
        return_periods = pd.read_parquet(return_period_path)
    else:
        return_periods = geoglows.data.return_periods(v2numbers.values)
        return_periods.index.name = 'v2number'
        return_periods.to_parquet(return_period_path)
        print("Return periods saved to ", return_period_path)
