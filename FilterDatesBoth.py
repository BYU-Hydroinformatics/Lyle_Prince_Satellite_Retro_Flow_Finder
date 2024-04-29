import yaml
import pandas as pd
import geopandas as gpd
import argparse
from geoglows import analysis
from datetime import datetime as dt
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
    geohistdf = pd.read_parquet(Geoglows_Hist_Path)
    latlons = pd.read_csv(reach_ids_path)
    flow_occurance_df = pd.DataFrame(
        columns=['lat', 'lon', 'reach_id', 'pass', '0_year', '2_year', "5_year", "10_year", "25_year", "50_year", "100_year"])
    master_dates = pd.DataFrame(columns=['time', 'lat', 'lon', 'reach_id', 'id'])
    orbits = ['ASC', 'DES']
    date_img_gdf_full = gpd.read_parquet(SAR_dates_path)

    # inputs
    for row in latlons.itertuples():
        lat = row[1]
        lon = row[2]
        v2number = row[3]

        # get streamflows from GeoGLOWS
        q_geo_df = geohistdf[str(v2number)]
        date_img_df = date_img_gdf_full.loc[date_img_gdf_full['v2number'] == v2number]
        date_img_df = date_img_df.dropna()
        date_img_df['time'] = pd.to_datetime(date_img_df['date'], format='%Y-%m-%d')
        date_img_df['time'] = date_img_df['time'].dt.strftime('%Y-%m-%d')
        date_img_df['time'] = pd.to_datetime(date_img_df['time'])
        date_img_df = date_img_df.reset_index().set_index('time').drop(columns=['index'])
        for j in orbits:
            dates_img_pass_df = date_img_df.loc[date_img_df['pass'] == j]
            # preform join
            dates_flow_df = dates_img_pass_df.join(q_geo_df, how='left')
            dates_flow_df.rename(columns={str(v2number): 'flow'}, inplace=True)
            dates_flow_df['return_period'] = '0_year'
            dates_flow_df['return_period_int'] = 0

            # calculate return periods and filter
            rp = [2, 5, 10, 25, 50, 100]
            return_periods = analysis.compute_return_periods(q_geo_df)
            print(return_periods)
            dates = pd.DataFrame()
            rp_out = return_period
            for i in rp:
                dates_flow_df.loc[(dates_flow_df['flow'] > return_periods['return_period_' + str(i)]), ('return_period')] = str(i)+'_year'
                dates_flow_df.loc[(dates_flow_df['flow'] > return_periods['return_period_' + str(i)]), ('return_period_int')] = i
            count_df = pd.DataFrame(dates_flow_df.groupby('return_period')['flow'].count())
            count_df = count_df.transpose()
            count_df.loc[:, 'lat'] = lat
            count_df.loc[:, 'lon'] = lon
            count_df.loc[:, 'reach_id'] = v2number
            count_df.loc[:, 'pass'] = j
            flow_occurance_df = pd.concat([flow_occurance_df, count_df])

            dates = dates_flow_df.loc[dates_flow_df['return_period_int'] >= return_period]
            if dates.empty == False:
                dates.loc[:, 'lat'] = lat
                dates.loc[:, 'lon'] = lon
                dates.loc[:, 'reach_id'] = v2number
                dates.loc[:, 'pass'] = j
                dates.loc[:, 'id']
                master_dates = pd.concat([master_dates, dates])
            # for i in rp:
            #     flow_drop = dates_flows_xr.where(dates_flows_xr <= return_periods['return_period_' + str(i)], drop=True)
            #     flow_dates = dates_flows_xr.where(dates_flows_xr > return_periods['return_period_' + str(i)], drop=True)
            #     num_imgs.append(dates_flows_xr.sizes['time'])
            #     if i > return_period:
            #         flow_drop = flow_drop.to_dataframe()
            #         if flow_drop.empty == False:
            #             flow_drop.loc[:, 'return_period'] = rp_out
            #             dates = dates.append(flow_drop)
            #         if i == 100:
            #             flow_drop = flow_dates.to_dataframe()
            #             if flow_drop.empty == False:
            #                 flow_drop.loc[:, 'return_period'] = rp_out
            #                 dates = dates.append(flow_drop)
            #         rp_out = i

            # # add flow occurances
            # info = [lat, lon, v2number]
            # info.extend(num_imgs)
            # flow_occurance_df.loc[len(flow_occurance_df)] = info
            #
            # # add flow dates
            # if dates.empty == False:
            #     dates['flow'] = dates[str(v2number)]
            #     dates.drop(str(v2number), axis=1, inplace=True)
            #     dates.loc[:, 'lat'] = lat
            #     dates.loc[:, 'lon'] = lon
            #     dates.loc[:, 'reach_id'] = v2number
            #     # dates.loc[:, 'id']
            #     master_dates = pd.concat([master_dates, dates])
            #     # print(dates)
            # # elif dates.empty == True:
            #     # print('No dates found')

        # export data
    flow_occurance_df.to_csv(flow_occurance_path)
    master_dates.to_csv(master_dates_path)
    print("Flow occurances saved to ", flow_occurance_path)
    print("Master dates saved to ", master_dates_path)
    print("All done!")