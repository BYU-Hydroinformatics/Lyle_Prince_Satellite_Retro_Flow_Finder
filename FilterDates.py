import argparse
import Satellite_Retro_Flow_Finder_v1
import pandas as pd
import yaml
import xarray as xr


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
        flow_occurance_path = config['flow_occurance_path']
        master_dates_path = config['master_dates_path']

    #open the historical simulation data
    geohistdf = pd.read_csv(Geoglows_Hist_Path)
    geohistdf['time'] = pd.to_datetime(geohistdf['time'])
    geohistdf = geohistdf.reset_index().set_index('time')
    geohistdf = geohistdf.astype("float32")
    geohistdf = geohistdf.drop(columns=['index'])
    geohistxr = geohistdf.to_xarray()
    latlons = pd.read_csv(reach_ids_path)
    date_img_df_full = pd.read_csv(SAR_dates_path)
    flow_occurance_df = pd.DataFrame(
        columns=['lat', 'lon', 'reach_id', '0_year', '2_year', "5_year", "10_year", "25_year", "50_year", "100_year"])
    master_dates = pd.DataFrame(columns=['time', 'lat', 'lon', 'reach_id'])

    # inputs
    for row in latlons.itertuples():
        lat = row[1]
        lon = row[2]
        v2number = row[3]

        # get streamflows from GeoGLOWS
        q_geo_xarray = geohistxr[str(v2number)]

        # get image dates
        date_img_df = date_img_df_full[str(v2number)]
        date_img_df = date_img_df.dropna()
        date_img_df = date_img_df.reset_index()
        date_img_df['time'] = pd.to_datetime(date_img_df[str(v2number)])
        date_img_df = date_img_df.reset_index().set_index('time').drop(columns=['index'])
        date_img_df = date_img_df.drop(columns='level_0')
        date_img_xr = date_img_df.to_xarray()
        q_geo_df = geohistdf[str(v2number)]

        # match dates
        dates_flows_xr = Satellite_Retro_Flow_Finder_v1.match_dates(q_geo_xarray, date_img_xr)

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
            dates['flow'] = dates[str(v2number)]
            dates.drop(str(v2number), axis=1, inplace=True)
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
    print("Flow occurances saved to ", flow_occurance_path)
    print("Master dates saved to ", master_dates_path)
    print("All done!")
