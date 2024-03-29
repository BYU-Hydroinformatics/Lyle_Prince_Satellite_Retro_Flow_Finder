import argparse
import Satellite_Retro_Flow_Finder_v1
import pandas as pd
import yaml
import xarray as xr
import ee


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
        return_period = config['return_period']
        SAR_dates_path = config['SAR_dates_path']

    ee.Initialize(project="fier-tana")

    #open the historical simulation data
    latlons = pd.read_csv(reach_ids_path)

    comb_dates = pd.DataFrame()

    # inputs
    for row in latlons.itertuples():
        lat = row[1]
        lon = row[2]
        v2number = row[3]

        # get image dates
        dates_imgs = Satellite_Retro_Flow_Finder_v1.get_image_dates_df(lat, lon)
        dates_imgs.rename(columns={"time":v2number}, inplace=True)

        # add dates to the xarray
        comb_dates= pd.concat([comb_dates, dates_imgs], axis=1)
    # save the xarray
    comb_dates.to_csv(SAR_dates_path, index=False)
    print("SAR dates saved to ", SAR_dates_path)
