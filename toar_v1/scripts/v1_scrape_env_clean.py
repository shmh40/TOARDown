# This script downloads all available DMA8 chemical data for ozone, NO, NO2 and PM2.5 on a daily scale from the TOAR v1 database for a particular country. The script also downloads and appends station attributes for each station in the country being downloaded.

# when we concat we do not change the dytpes in the same way I don't think...or maybe different version of pandas

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


# set the global variables: the country we are selecting, and today's date!

country_for_url = 'Sweden'
country = 'Sweden'
todays_date='080922'

# define the URLs of the TOAR v1 dataset that we are interested in.

BASEURL = "https://join.fz-juelich.de/services/rest/surfacedata/"

#URL1 = "search/?station_country=France&parameter_name=o3,no,no2,pm2p5&columns=id,network_name,station_id,station_country,station_lat,station_lon&format=json"

URL1 = "search/?station_country="+country+"&parameter_name=press,u,v,relhum,cloudcover,temp,totprecip,co,benzene,toluene,pblheight,ch4,aswdifu,aswdir,irradiance&columns=id,network_name,station_id,station_country,station_lat,station_lon&format=json"

# select whole day average

URL2 = "stats/?id=%i&sampling=daily&statistics=average_values&format=json"


# ***************************************************
# Find all the sites and their associated data series
# first: find all sites
# ***************************************************
print("Opening URL1...")
response = urlopen(BASEURL + URL1).read().decode('utf-8')
print("response = ", response[:200], " ... ")
metadata = json.loads(response)

# Here we are downloading all dataseries for our specified search. This takes a while to run.

# create an empty dataframe...
df_test = pd.DataFrame()

# loop to download the dataseries for the variable of interest, at all stations where it is present, alongside station metadata and static attributes for the station that the variable dataseries is coming from.

for s in metadata:
    # find all the dataseries
    all_dataseries = s[0]
    #print(s)
    #print(all_dataseries)
    for series in all_dataseries:
        # download each individual series separately here
        #print("Opening URL2...")
        #print(series)
    
        # isolate and download the particular series that we are interested in
        dresponse = urlopen(BASEURL + URL2  % series).read().decode('utf-8')
        data = json.loads(dresponse)
        print(data['metadata']['parameter_name'])
        print(data['metadata']['station_name'], data['metadata']['station_country'])
            
        # may need to change between average values and dma8, depending on whether we are looking for env or dma8
        new_row = pd.DataFrame({'series_id': series, 
                       'average_values': data['mean'], 
                       #'dma8': data['dma8eu_strict'],
                       'datetime':data['datetime'], 
                       #'data_capture': data['data_capture'],
                       'country':data['metadata']['station_country'], 
                       'variable_name':data['metadata']['parameter_name'],
                       'variable_label':data['metadata']['parameter_label'],               
                       'units':data['metadata']['parameter_original_units'],
                       'station_etopo_alt':data['metadata']['station_etopo_alt'],
                       'station_rel_etopo_alt':data['metadata']['station_etopo_relative_alt'],
                       'lat':s[4],'lon':s[5],'nox_emi':data['metadata']['station_nox_emissions'], 
                       'omi_nox':data['metadata']['station_omi_no2_column'],
                       'station_name':data['metadata']['station_name'], 'station_type':data['metadata']['station_type'],
                       'alt':data['metadata']['station_alt'], 'landcover':data['metadata']['station_dominant_landcover'],
                       'pop_density':data['metadata']['station_population_density'],
                       'max_5km_pop_density':data['metadata']['station_max_population_density_5km'],
                       'max_25km_pop_density':data['metadata']['station_max_population_density_25km'],
                       'nightlight_1km':data['metadata']['station_nightlight_1km'], 
                       'nightlight_max_25km':data['metadata']['station_max_nightlight_25km'],
                       'toar_category':data['metadata']['station_toar_category'], 
                       'measurement_method':data['metadata']['parameter_measurement_method']})
        #print(new_row)
            # append all these individual series to the initially empty dataframe that was instantiated earlier
        #df_test = df_test.append(new_row, ignore_index = True)
        df_test = pd.concat([df_test, new_row], axis=0, ignore_index = True)


df_temp = df_test[df_test['variable_name']=='temp']
df_press = df_test[df_test['variable_name']=='press']
df_u = df_test[df_test['variable_name']=='u']
df_v = df_test[df_test['variable_name']=='v']
df_totprecip = df_test[df_test['variable_name']=='totprecip']
df_pblheight = df_test[df_test['variable_name']=='pblheight']
df_relhum = df_test[df_test['variable_name']=='relhum']
df_cloudcover = df_test[df_test['variable_name']=='cloudcover']


df_temp['temp'] = df_temp['average_values']
df_press['press'] = df_press['average_values']
df_u['u'] = df_u['average_values']
df_v['v'] = df_v['average_values']
df_totprecip['totprecip'] = df_totprecip['average_values']
df_pblheight['pblheight'] = df_pblheight['average_values']
df_relhum['relhum'] = df_relhum['average_values']
df_cloudcover['cloudcover'] = df_cloudcover['average_values']


# drop unnecessary columns...

df_temp_dropped_cols = df_temp.drop(['average_values', 'series_id', 'variable_name', 'variable_label', 'units', 'measurement_method'], axis=1)
df_press_dropped_cols = df_press.drop(['average_values', 'series_id', 'variable_name', 'variable_label', 'units', 'measurement_method'], axis=1)
df_u_dropped_cols = df_u.drop(['average_values', 'series_id', 'variable_name', 'variable_label', 'units', 'measurement_method'], axis=1)
df_v_dropped_cols = df_v.drop(['average_values', 'series_id', 'variable_name', 'variable_label', 'units', 'measurement_method'], axis=1)
df_totprecip_dropped_cols = df_totprecip.drop(['average_values', 'series_id', 'variable_name', 'variable_label', 'units', 'measurement_method'], axis=1)
df_pblheight_dropped_cols = df_pblheight.drop(['average_values', 'series_id', 'variable_name', 'variable_label', 'units', 'measurement_method'], axis=1)
df_relhum_dropped_cols = df_relhum.drop(['average_values', 'series_id', 'variable_name', 'variable_label', 'units', 'measurement_method'], axis=1)
df_cloudcover_dropped_cols = df_cloudcover.drop(['average_values', 'series_id', 'variable_name', 'variable_label', 'units', 'measurement_method'], axis=1)

try:
    dfs = [df_temp_dropped_cols, df_press_dropped_cols, df_u_dropped_cols, df_v_dropped_cols, df_totprecip_dropped_cols, df_pblheight_dropped_cols, df_relhum_dropped_cols, df_cloudcover_dropped_cols]
except: 
    print('One or more of the dfs is missing')
    
    
#merge all DataFrames into one
final_df = reduce(lambda  left,right: pd.merge(left,right,on=['datetime', 'country', 'station_name', 'lat', 'lon', 'alt', 'station_etopo_alt', 
                                                              'station_rel_etopo_alt', 'station_type', 	'landcover', 
                                                              'toar_category', 'pop_density', 'max_5km_pop_density', 'max_25km_pop_density', 
                                                              'nightlight_1km', 'nightlight_max_25km', 'nox_emi', 'omi_nox'],
                                            how='outer'), dfs)




final_df_sorted = final_df.sort_values(['station_name', 'datetime'], ignore_index=True)

final_df_sorted['datetime'] = pd.to_datetime(final_df_sorted['datetime'], format='%Y-%m-%d')

#final_df_sorted = final_df_sorted.replace(-1.0, np.nan)
#final_df_sorted = final_df_sorted.replace(-999.0, np.nan)

final_df_sorted_dropna = final_df_sorted.dropna()


# make a directory to save files in if it doesn't already exist with path 
path = '/home/jovyan/lustre_scratch/cas/european_data_new_temp/country/'+country+'/env/'
                      
try: 
    os.makedirs(path) 
except OSError as error: 
    print(error)                        

                      

final_df_sorted.to_csv('/home/jovyan/lustre_scratch/cas/european_data_new_temp/country/'+country+'/env/env_data.csv', index=False)
final_df_sorted_dropna.to_csv('/home/jovyan/lustre_scratch/cas/european_data_new_temp/country/'+country+'/env/env_dropna_data.csv', index=False)