import argparse
import Satellite_Retro_Flow_Finder_v1
import pandas as pd
import yaml
import xarray as xr
import ee


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
        return_period = config['return_period']
        SAR_dates_path = config['SAR_dates_path']

    ee.Initialize(project="fier-tana")

    #open the historical simulation data
    geohistdf = pd.read_csv(Geoglows_Hist_Path)
    latlons = pd.read_csv(reach_ids_path, header=None)

    dims = "time, v2number"
    date_img_xr_combined = xr.DataArray(
        dims=dims,
        coords={"time": geohistdf.index, "v2number": geohistdf.columns},
    )
    # inputs
    for row in latlons.itertuples():
        lat = row[1]
        lon = row[2]
        v2number = row[3]

        # get image dates
        dates_imgs = Satellite_Retro_Flow_Finder_v1.get_image_dates(lat, lon)

        # add dates to the xarray
        date_img_xr_combined.loc[{"v2number": v2number}] = dates_imgs

    # save the xarray
    date_img_xr_combined.to_netcdf(SAR_dates_path)
