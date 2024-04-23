import yaml
import argparse
import hsclient

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
        SAR_dates_path = config['SAR_dates_path']
        SAR_dates_path = config['SAR_dates_path']
        flow_occurance_path = config['flow_occurance_path']
        master_dates_path = config['master_dates_path']

    #read the inputs
    hs = hsclient.HydroShare()
    hs.sign_in()
    resource_id = hs.create()
    resource_id.file_upload(reach_ids_path, Geoglows_Hist_Path, SAR_dates_path, flow_occurance_path, master_dates_path)
    # resource_id.save()
    resource_id.metadata.title = "FIER Feasibility for points on a 2 degree grid"
    resource_id.metadata.abstract = "This resource contains the outputs of the FIER feasibility analysis for points on a 2 degree grid. It was generated using the FIER Feasability tools created by Lyle Prince as part of his master's reasearch. The code to replicate this study can be found at https://github.com/ldp48/Satellite_Retro_Flow_Finder"
    resource_id.metadata.subjects = ["FIER", "GEOGlows", "SAR", "Remote Sensing", "Hydrology"]
    for file in resource_id.files(search_aggregations=True):
        print(file.path)
    resource_id.save()


