import pandas as pd

#read peak data
peak_df = pd.read_csv('/Users/ldp/Downloads/USGSPeakData.csv')
#read USGS sites detailed data
sample_sites_df = pd.read_csv('/Users/ldp/Downloads/USGSSampleSites.csv')
sample_sites_df['site_no'] = sample_sites_df['site_no'].astype(str)
peak_df = peak_df[peak_df['site_no'].isin(sample_sites_df['site_no'])]

# classify the peak flow data into the return periods classes from the sample sites data
rps = [2, 5, 10, 25, 50, 100]
peak_df = peak_df.merge(sample_sites_df, on='site_no', how='left', suffixes=('_sample', '_peak'))

# classify the return periods
peak_df.loc[:, 'return_period'] = 0
for rp in rps:
    peak_df.loc[peak_df['peak_va'] > peak_df[str(rp)], 'return_period'] = rp
peak_df.columns = peak_df.columns.astype(str)
peak_df = peak_df.drop(columns=['xbar', 'std', 'station_nm', 'site_tp_cd', 'lat_va', 'long_va', 'dec_lat_va', 'dec_long_va', 'coord_meth_cd', 'coord_acy_cd', 'coord_datum_cd', 'dec_coord_datum_cd', 'district_cd', 'state_cd'])
#count number or each return period in peak_df
print(peak_df['return_period'].value_counts())

# peak_df.to_parquet('/Users/ldp/Downloads/USGSPeakDataClassified.parquet')