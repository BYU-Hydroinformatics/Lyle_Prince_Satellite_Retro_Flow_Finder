import ee

import Satellite_Retro_Flow_Finder_v1
import geoglows

#inputs
lat = 0.7680
lon = -79.5532
return_period = 10

ee.Initialize(project="fier-tana")

# get streamflows from GeoGLOWS
q_geo = Satellite_Retro_Flow_Finder_v1.get_streamflow(lat, lon)
reach_id = q_geo[1]
q_geo = q_geo[0]
q_geo_xarray = Satellite_Retro_Flow_Finder_v1.format_flow_dates(q_geo)

#get image dates
dates_imgs = Satellite_Retro_Flow_Finder_v1.get_image_dates(lat, lon)

#match dates
dates_flows_xr = Satellite_Retro_Flow_Finder_v1.match_dates(q_geo_xarray, dates_imgs)
print(dates_flows_xr)
#calculate return period
flow_rp = Satellite_Retro_Flow_Finder_v1.filter_by_return_period(q_geo, dates_flows_xr, return_period)
print(flow_rp)


