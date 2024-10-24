import yaml
import pandas as pd
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, help='Configuration file', default='configs.yaml')
    args = parser.parse_args()
    config_file = args.config

    if config_file is None:
        raise ValueError("Please provide a configuration file")

    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
        master_dates_path = config['master_dates_path']
        flow_occurance_path = config['flow_occurance_path']
        lower_rp = config['lower_rp']
        upper_rp = config['upper_rp']

    option = "max"
    rps = ['0', '2', '5', '10', '25', '50', '100']
    flow_occurance_df = pd.read_parquet(flow_occurance_path)
    flow_occurance_df = flow_occurance_df.fillna(0)
    print(flow_occurance_df.head(10))
    if option == "suitability":
        #lower_dict = {'0': ['0'], '2': ['0', '2'], '5': ['0', '2', '5'], '10': ['0', '2', '5', '10'], '25': ['0', '2', '5', '10', '25'], '50': ['0', '2', '5', '10', '25', '50'], '100': ['0', '2', '5', '10', '25', '50', '100']}
        #upper_dict = {'0': ['0', '2', '5', '10', '25', '50', '100'], '2': ['2', '5', '10', '25', '50', '100'], '5': ['5', '10', '25', '50', '100'], '10': ['10', '25', '50', '100'], '25': ['25', '50', '100'], '50': ['50', '100'], '100': ['100']}
        flow_occurance_df['suitability'] = 0
        lower_list = rps[:rps.index(lower_rp)+1]
        inner_list = rps[rps.index(lower_rp):rps.index(upper_rp) + 1]
        upper_list = rps[rps.index(upper_rp):]
        flow_occurance_df.loc[(flow_occurance_df[lower_list].sum(axis=1) > 0) & (flow_occurance_df[upper_list].sum(axis=1) > 0), 'suitability'] = 1
        flow_occurance_df.loc[(flow_occurance_df['suitability'] == 1) & (flow_occurance_df[inner_list].min(axis=1) > 0), 'suitability'] = 2
        flow_occurance_df.loc[(flow_occurance_df['suitability'] == 2) & (flow_occurance_df[inner_list].sum(axis=1) > len(inner_list)), 'suitability'] = flow_occurance_df[inner_list].sum(axis=1)
        print(flow_occurance_df.loc[flow_occurance_df['suitability'] >= 2])

    if option == 'max':
        flow_occurance_df['suitability'] = 0
        rp_prev = 0
        for rp in rps:
            flow_occurance_df.loc[(flow_occurance_df[rp] != 0) & (flow_occurance_df['suitability'] == rp_prev), 'suitability'] = int(rp)
            rp_prev = int(rp)

    flow_occurance_df.to_parquet('/Users/ldp/Documents/max_rp.parquet')
    print("Suitability saved to max_rp.parquet")
    print(flow_occurance_df.head(10))

