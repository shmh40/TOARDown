# This script merges the environmental and chemical data downloaded from the v1_scrape_dma8 and v1_scrape_env scripts, putting them into a single dataframe, one with nans and one without nans. The dataframes include datetimes and station attributes. 

# Our imports for this script

import pandas as pd
import numpy as np
import datetime
from datetime import datetime, timedelta


from urllib.request import urlopen
import json
import glob
import os
from functools import reduce

## Perhaps we should merge the two datasets here.
## Here reading in the dropna'ed data, and the raw data

# just need to swap the country in here!
# also need to have dma8 or dma8_non_strict for the chemical data, defined by the string sampling here...

country = 'Spain'
sampling = 'dma8_non_strict' # or 'dma8'


# read in both the dropnaed and total data for both env and dma8 

country_dma8_df = pd.read_csv('/home/jovyan/lustre_scratch/cas/european_data_new_temp/country/'+country+'/dma8/'+sampling+'_data.csv')
country_dma8_df_dropna = pd.read_csv('/home/jovyan/lustre_scratch/cas/european_data_new_temp/country/'+country+'/dma8/'+sampling+'_dropna_data.csv')

country_env_df = pd.read_csv('/home/jovyan/lustre_scratch/cas/european_data_new_temp/country/'+country+'/env/env_data.csv')
country_env_df_dropna = pd.read_csv('/home/jovyan/lustre_scratch/cas/european_data_new_temp/country/'+country+'/env/env_dropna_data.csv')

print('Data loading complete')
# merging non-dropnaed ones as before...

#define list of DataFrames
dfs = [country_dma8_df, country_env_df]

#merge all DataFrames into one
merged_env_dma8_df = reduce(lambda  left,right: pd.merge(left,right,on=['datetime', 'station_name', 'lat', 'lon', 'alt', 'station_etopo_alt', 
                                                                        'station_rel_etopo_alt', 'station_type', 'landcover', 'toar_category', 
                                                                        'pop_density', 'max_5km_pop_density', 'max_25km_pop_density', 
                                                                        'nightlight_1km', 'nightlight_max_25km', 'nox_emi', 'omi_nox'],
                                            how='outer'), dfs)

print('Merging complete')
# here replacing -1.0s and -999.0s with nans...allows easier dropping of nan values
# I think this has already been done, but we can do it here too, we expect the same result.

merged_env_dma8_df = merged_env_dma8_df.replace(-1.0, np.nan)
merged_env_dma8_df = merged_env_dma8_df.replace(-999.0, np.nan)


### just for reference - how are we doing on the NaNs front? Is our dataframe empty?

merged_env_dma8_df_dropna = merged_env_dma8_df.dropna()
print('Check on length of dropnaed data:', len(merged_env_dma8_df_dropna))
# Here we are setting up the time_idx aspect of the work.

# think here about whether we want the dropnaed version or the version with nans...we most likely want with nans
# then deal with it in prep for model ingestion

# merged_env_dma8_df_dropna.dtypes
merged_env_dma8_df['datetime'] = pd.to_datetime(merged_env_dma8_df['datetime'], format='%Y-%m-%d')
merged_env_dma8_df['raw_time_idx'] = merged_env_dma8_df['datetime'].apply(lambda x: x.toordinal())
print(max(merged_env_dma8_df['raw_time_idx']))
merged_env_dma8_df['time_idx_large_temp'] = merged_env_dma8_df['raw_time_idx'] + 1000000

# here we are doing the timeidx for each station.

new_data = pd.DataFrame()

for s in list(merged_env_dma8_df['station_name'].unique()):
    data_subset = merged_env_dma8_df[merged_env_dma8_df["station_name"] == s]
    data_subset['time_idx_new'] = data_subset['time_idx_large_temp'] - max(data_subset['raw_time_idx'])
    # print(max(data_subset['time_idx_new']))
    new_data = pd.concat([new_data, data_subset])
    
# set a new column, called time_idx, to act as the time_idx!

new_data["time_idx"] = new_data['time_idx_new']

# sort the dataframe by station_name and by time_idx, and temp and no2
# I think what I need to do here is to drop duplicates on the dataframe based on datetime and station_name...having sorted by temp and no2 
# (non-nan values go to the top, so we keep first)
# this way we get rid of duplicate records...but really i want to remove the row(s) with the most nans in it. This seems unclear in the pandas docs

new_data_sorted = new_data.sort_values(['station_name', 'time_idx', 'temp', 'no2'], ignore_index=True) 

new_data_sorted_drop_dups = new_data_sorted.drop_duplicates(subset=['datetime', 'station_name'], keep='first')

# I should do an assert here to check that we do no have duplicates of date...

assert new_data_sorted_drop_dups['station_name'].nunique() == new_data_sorted_drop_dups['time_idx'].value_counts().max()

# could create the directory here, but this has already been done!

new_data_sorted_drop_dups.to_csv('/home/jovyan/lustre_scratch/cas/european_data_new_temp/country/'+country+'/'+country+'_'+sampling+'_all_data_timeidx_drop_dups.csv', index=False)

print('Data saved to csv')

# this dataframe (which has nans), can then be dealt with before ingesting into algorithm, depending on need for NO or NO2 for example.