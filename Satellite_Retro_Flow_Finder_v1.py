import xarray as xr
from typing import Tuple
from geoglows import streamflow
from geoglows import analysis
import ee
import hydrafloods as hf
import datetime
import pandas as pd

def get_streamflow(lat: float, lon: float) -> Tuple[xr.DataArray, int]:
    """Function to get histroical streamflow data from the GeoGLOWS server
    based on geographic coordinates

    args:
        lat (float): latitude value where to get streamflow data
        lon (float): longitude value where to get streamflow data

    returns:
        xr.DataArray: DataArray object of streamflow with datetime coordinates
    """
    # This gets the reach number from a point, will need to be rewritten for V2
    reach = streamflow.latlon_to_reach(lat, lon)
    # This gets the data, will need to be rewritten for V2
    q = streamflow.historic_simulation(reach['reach_id'])
    return q, reach['reach_id']

def format_flow_dates(dates: pd.DataFrame) -> xr.DataArray:
    dates.columns = ["discharge"]
    # rename index and drop the timezone value
    dates.index.name = "time"
    dates.index = dates.index.tz_localize(None)

    # return the series as a xr.DataArray
    return dates.discharge.to_xarray()

def get_image_dates(lat: float, lon: float):
    region = ee.Geometry.BBox(lon+.0001, lat+.0001, lon-.0001, lat-.0001)
    start = "2014-01-01"
    end = "2025-01-01"
    s1 = hf.Sentinel1(region, start, end)
    dates = s1.dates
    dates_df = pd.DataFrame(dates, columns=["time"])
    dates_df['time'] = dates_df['time'].str[:10]
    dates_df['time'] = pd.to_datetime(dates_df['time'])
    dates_array = xr.DataArray(
        coords={'time': dates_df['time']},
        dims=['time']
    )
    return dates_array

def match_dates(original: xr.DataArray, matching: xr.DataArray) -> xr.DataArray:
    """Helper function to filter a DataArray from that match the data values of another.
    Expects that each xarray object has a dimesion named 'time'

    args:
        original (xr.DataArray): original DataArray with time dimension to select from
        matching (xr.DataArray): DataArray with time dimension to compare against

    returns:
        xr.DataArray: DataArray with values that have been temporally matched
    """

    # return the DataArray with only rows that match dates
    return original.where(original.time.isin(matching.time), drop=True)

def filter_by_return_period(flow: xr.DataArray, flow_dates: xr.DataArray, return_period) -> xr.DataArray:
    """Function to filter a DataArray by a return period value

    args:
        flow (xr.DataArray): DataArray with time dimension to filter
        return_period (int): return period value to filter by

    returns:
        xr.DataArray: DataArray with values that have been temporally matched
    """
    num_imgs = [flow_dates.sizes['time']]
    rp = [2, 5, 10, 25, 50, 100]
    return_periods = analysis.compute_return_periods(flow)
    print(return_periods)
    dates = pd.DataFrame()
    rp_out = return_period
    for i in rp:
        flow_drop = flow_dates.where(flow_dates <= return_periods['return_period_' + str(i)], drop=True)
        flow_dates = flow_dates.where(flow_dates > return_periods['return_period_' + str(i)], drop=True)
        num_imgs.append(flow_dates.sizes['time'])
        if i > return_period:
            flow_drop = flow_drop.to_dataframe()
            if flow_drop.empty == False:
                flow_drop.loc[:, 'return_period'] = rp_out
                dates = dates.append(flow_drop)
            if i == 100:
                flow_drop = flow_dates.to_dataframe()
                if flow_drop.empty == False:
                    flow_drop.loc[:, 'return_period'] = rp_out
                    dates = dates.append(flow_drop)
            rp_out = i

    # return the DataArray with only rows that match dates
    return num_imgs, dates
