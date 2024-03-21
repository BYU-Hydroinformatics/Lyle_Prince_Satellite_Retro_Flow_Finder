import argparse
import Satellite_Retro_Flow_Finder_v1
import pandas as pd
import yaml


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument( '--config', type=str, help='Configuration file')
    args = parser.parse_args()
    config_file = args.config

    # read the yaml config file
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
        reach_ids_path = config['reach_ids_path']
        Geoglows_Hist_Path = config['Geoglows_Hist_Path']

    # read the inputs
    latlons = pd.read_csv(reach_ids_path, header=None)

    #set up AWS
    bucket_uri = 's3://geoglows-v2-retrospective/retrospective.zarr'
    region_name = 'us-west-2'
    s3 = s3fs.S3FileSystem(anon=True, client_kwargs=dict(region_name=region_name))
    s3store = s3fs.S3Map(root=bucket_uri, s3=s3, check=False)

    # get historical simulation data
    geohistzarr = xr.open_zarr(s3store)
    geohistdf = geohistzarr['Qout'].sel(rivid=v2numbers).to_dataframe()
    geohistdf = geohistdf.reset_index().set_index('time').pivot(columns='rivid', values='Qout')
    geohistdf.to_csv(Geoglows_Hist_Path)
    print("Historical simulation data saved to ", Geoglows_Hist_Path)
