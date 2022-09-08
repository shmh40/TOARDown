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

country = 'Sweden'

country_dma8_df = pd.read_csv('/home/jovyan/lustre_scratch/cas/european_data_new_temp/country/'+country+'/dma8/dma8_data.csv')
country_dma8_df_dropna = pd.read_csv('/home/jovyan/lustre_scratch/cas/european_data_new_temp/country/'+country+'/dma8/dma8_dropna_data.csv')

country_env_df = pd.read_csv('/home/jovyan/lustre_scratch/cas/european_data_new_temp/country/'+country+'/env/env_data.csv')
country_env_df_dropna = pd.read_csv('/home/jovyan/lustre_scratch/cas/european_data_new_temp/country/'+country+'/env/env_dropna_data.csv')

# merging non-dropnaed ones as before...

#define list of DataFrames
dfs = [country_dma8_df, country_env_df]

#merge all DataFrames into one
merged_env_dma8_df = reduce(lambda  left,right: pd.merge(left,right,on=['datetime', 'station_name', 'lat', 'lon', 'alt', 'station_etopo_alt', 
                                                                        'station_rel_etopo_alt', 'station_type', 'landcover', 'toar_category', 
                                                                        'pop_density', 'max_5km_pop_density', 'max_25km_pop_density', 
                                                                        'nightlight_1km', 'nightlight_max_25km', 'nox_emi', 'omi_nox'],
                                            how='outer'), dfs)


# here replacing -1.0s and -999.0s with nans...allows easier dropping of nan values

merged_env_dma8_df = merged_env_dma8_df.replace(-1.0, np.nan)
merged_env_dma8_df = merged_env_dma8_df.replace(-999.0, np.nan)


### just for reference - how are we doing on the NaNs front? Is our dataframe empty?

merged_env_dma8_df_dropna = merged_env_dma8_df.dropna()
print('Check on length of dropnaed data:', len(merged_env_dma8_df_dropna))
# Here we are setting up the time_idx aspect of the work.

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
    print(max(data_subset['time_idx_new']))
    new_data = pd.concat([new_data, data_subset])
    
# set a new column, called time_idx, to act as the time_idx!

new_data["time_idx"] = new_data['time_idx_new']

# could create the directory here, but this has already been done!

new_data.to_csv('/home/jovyan/lustre_scratch/cas/european_data_new_temp/country/'+country+'/'+country+'_all_data_timeidx.csv', index=False)

# this dataframe (which has nans), can then be dealt with before ingesting into algorithm, depending on need for NO or NO2 for example.