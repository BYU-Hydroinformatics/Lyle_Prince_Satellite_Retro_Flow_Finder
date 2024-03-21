import argparse
import Satellite_Retro_Flow_Finder_v1
import pandas as pd
import yaml
import xarray as xr


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
        flow_occurance_path = config['flow_occurance_path']
        master_dates_path = config['master_dates_path']

    #open the historical simulation data
    geohistdf = pd.read_csv(Geoglows_Hist_Path)
    geohistxr = xr.DataArray(geohistdf)
    latlons = pd.read_csv(reach_ids_path, header=None)
    date_img_xr_combined = xr.open_dataarray(SAR_dates_path)
    flow_occurance_df = pd.DataFrame(
        columns=['lat', 'lon', 'reach_id', '0_year', '2_year', "5_year", "10_year", "25_year", "50_year", "100_year"])
    master_dates = pd.DataFrame(columns=['time', 'lat', 'lon', 'reach_id'])

    # inputs
    for row in latlons.itertuples():
        lat = row[1]
        lon = row[2]
        v2number = row[3]

        # get streamflows from GeoGLOWS
        q_geo_xarray = geohistxr[v2number]
        dates_imgs = date_img_xr_combined[v2number]
        q_geo_df = geohistdf[v2number]

        # match dates
        dates_flows_xr = Satellite_Retro_Flow_Finder_v1.match_dates(q_geo_xarray, dates_imgs)
        print(dates_flows_xr)

        # calculate return periods and filter
        flow_rp = Satellite_Retro_Flow_Finder_v1.filter_by_return_period(q_geo_df, dates_flows_xr, 10)

        # add flow occurances
        num_imgs = flow_rp[0]
        info = [lat, lon, v2number]
        info.extend(num_imgs)
        flow_occurance_df.loc[len(flow_occurance_df)] = info

        # add flow dates
        dates = flow_rp[1]
        if dates.empty == False:
            dates['flow'] = dates[v2number]
            dates.drop(v2number, axis=1, inplace=True)
            dates.loc[:, 'lat'] = lat
            dates.loc[:, 'lon'] = lon
            dates.loc[:, 'reach_id'] = v2number
            master_dates = pd.concat([master_dates, dates])
            print(dates)
        elif dates.empty == True:
            print('No dates found')

    # export data
    flow_occurance_df.to_csv(flow_occurance_path)
    master_dates.to_csv(master_dates_path)
