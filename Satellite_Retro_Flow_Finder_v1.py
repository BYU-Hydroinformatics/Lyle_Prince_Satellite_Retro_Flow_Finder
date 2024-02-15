import xarray as xr
from typing import Tuple
from geoglows import streamflow
import ee
import hydrafloods
import datetime

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
    reach = streamflow.latlon_to_reach(lat,lon)
    # This gets the data, will need to be rewritten for V2
    q = streamflow.historic_simulation(reach['reach_id'])

    # rename column name to something not as verbose as 'streamflow_m^3/s'
    q.columns = ["discharge"]

    # rename index and drop the timezone value
    q.index.name = "time"
    q.index = q.index.tz_localize(None)

    # return the series as a xr.DataArray
    return q.discharge.to_xarray(), reach['reach_id']

def get_image_dates(lat: float, lon: float):
    region = ee.Geometry.BBox(lat+.001, lon+.001, lat-.001, lon-.001)
    start = "2014-01-01"
    end = "2025-01-01"
    s1 = hf.Sentinel1Asc(region, start, end)
    dates = s1.dates
    dates_df = pd.DataArray(dates)
    dates_df = [datetime.datetime.utcfromtimestamp(i/1000).strftime('%Y-%m-%d') for i in dates_df]
    

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