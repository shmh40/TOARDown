# This script downloads, on a daily scale, environmental data (ERA5 reanalysis) from the TOAR v1 database, and also some chemical species such as CO, methane, benzene and toluene if they are measured at a particular station. The script also downloads and appends station attributes for each station in the country being downloaded. 


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
country = 'Sweden'
todays_date='080922'

# define the URLs of the TOAR v1 dataset that we are interested in.

BASEURL = "https://join.fz-juelich.de/services/rest/surfacedata/"

#URL1 = "search/?station_country=France&parameter_name=o3,no,no2,pm2p5&columns=id,network_name,station_id,station_country,station_lat,station_lon&format=json"

URL1 = "search/?station_country="+country+"&parameter_name=press,u,v,relhum,cloudcover,temp,totprecip,co,benzene,toluene,pblheight,ch4,aswdifu,aswdir,irradiance&columns=id,network_name,station_id,station_country,station_lat,station_lon&format=json"

# select whole day average

URL2 = "stats/?id=%i&sampling=daily&statistics=average_values&format=json"

# select daytime average, typically for environmental data

#URL2 = "stats/?id=%i&sampling=daily&statistics=daytime_avg&format=json"

# select O3 dma8eu_strict...need different ones for China...
# dma8eu or dma8eu_strict here...

#URL2 = "stats/?id=%i&sampling=daily&statistics=dma8eu_strict,data_capture&format=json"

# ***************************************************
# Find all the sites and their associated data series
# first: find all sites
# ***************************************************

print("Opening URL1...")
response = urlopen(BASEURL + URL1).read().decode('utf-8')
print("response = ", response[:200], " ... ")
metadata = json.loads(response)

# here we are downloading all dataseries for our specified search - his takes a while to run.

# create an empty dataframe to append to
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
        try:
            # isolate and download the particular series that we are interested in
            dresponse = urlopen(BASEURL + URL2  % series).read().decode('utf-8')
            data = json.loads(dresponse)
            print(data['metadata']['parameter_name'])
            print(data['metadata']['station_name'], data['metadata']['station_country'])
            
            # may need to change between average values and dma8, depending on whether we are looking for env or dma8
            new_row = {'series_id': series, 
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
                       'lat':s[4],'lon':s[5],'NOx_emi':data['metadata']['station_nox_emissions'], 
                       'OMI_NOx':data['metadata']['station_omi_no2_column'],
                       'station_name':data['metadata']['station_name'], 'station_type':data['metadata']['station_type'],
                       'altitude':data['metadata']['station_alt'], 'landcover':data['metadata']['station_dominant_landcover'],
                       'pop_density':data['metadata']['station_population_density'],
                       'max_5km_pop_density':data['metadata']['station_max_population_density_5km'],
                       'max_25km_pop_density':data['metadata']['station_max_population_density_25km'],
                       'nightlight_1km':data['metadata']['station_nightlight_1km'], 
                       'nightlight_max_25km':data['metadata']['station_max_nightlight_25km'],
                       'toar_category':data['metadata']['station_toar_category'], 
                       'measurement_method':data['metadata']['parameter_measurement_method']}
            #print(new_row)
            # append all these individual series to the initially empty dataframe that was instantiated earlier
            df_test = df_test.append(new_row, ignore_index = True)
        except:
            print("This URL is bad")
            
            
# df_test.to_csv('/home/jovyan/lustre_scratch/cas/uk_avg_values_130622.csv')

# make a directory to save the file in if it doesn't already exist with path 
path = '/home/jovyan/lustre_scratch/cas/european_data_new_temp/raw_data_series/'+country+'/'
    
# Create the directory 
# 'GeeksForGeeks' in 
# '/home / User / Documents' 
try: 
    os.makedirs(path) 
except OSError as error: 
    print(error)  

# save the file    
df_test.to_csv('/home/jovyan/lustre_scratch/cas/european_data_new_temp/raw_data_series/'+country+'/'+country+'_env_'+todays_date+'.csv')

# read the file back in
toar_df = pd.read_csv('/home/jovyan/lustre_scratch/cas/european_data_new_temp/raw_data_series/'+country+'/'+country+'_env_'+todays_date+'.csv') 


# make a directory to save files in if it doesn't already exist with path 
path = '/home/jovyan/lustre_scratch/cas/european_data_new_temp/per_series/'+country+'/env/'
                      
try: 
    os.makedirs(path) 
except OSError as error: 
    print(error)                        

                      
for i in range(0,len(toar_df)):
    # instatiate an empty dataframe
    long_df_and_attributes = pd.DataFrame()
    try:
        # turn the datetimes into datetime as an index for the df
        datetime = toar_df['datetime'].iloc[i]
        datetime = datetime.replace('"', '')
        datetime = datetime.replace("[", "")
        datetime = datetime.replace("]", "")
        datetime = datetime.replace("'", "")
        datetime = datetime.replace(", ", ",")
        clean_datetime = datetime.split(",")
        clean_array_of_datetime = np.array(clean_datetime)
        datetime_df = pd.DataFrame(clean_array_of_datetime)
        datetime_df.columns = ['datetime']
        datetime_df['datetime'] = pd.to_datetime(datetime_df['datetime'], format="%Y-%m-%d %H:%M")
        #print(datetime_df)
        
        # get the data for the individual species in that row of the dataframe
        individual_variable = toar_df['average_values'].iloc[i]
        individual_variable = individual_variable.replace("[", "")
        individual_variable = individual_variable.replace("]", "")
        individual_variable = individual_variable.replace("'", "")
        individual_variable = individual_variable.replace(", ", ",")
        clean_individual_variable = individual_variable.split(",")
        clean_array_individual_variable = np.array(clean_individual_variable)
        #print(clean_array_individual_variable.shape)

        individual_variable_df = pd.DataFrame(clean_array_individual_variable)
        individual_variable_df.columns = [toar_df['variable_name'].iloc[i]]    
        
        # concatenate them
        datetime_df = pd.concat([datetime_df, individual_variable_df], axis=1) 
        
        # add all the extra information, which is static. I may need to add to this later.
        dict_of_stuff = {'station_name': toar_df['station_name'].iloc[i],
                     'lat': toar_df['lat'].iloc[i],
                     'lon': toar_df['lon'].iloc[i],
                     'alt': toar_df['altitude'].iloc[i],
                     'station_etopo_alt': toar_df['station_etopo_alt'].iloc[i],
                     'station_rel_etopo_alt': toar_df['station_rel_etopo_alt'].iloc[i],
                     'station_type': toar_df['station_type'].iloc[i],
                     'landcover': toar_df['landcover'].iloc[i],
                     'toar_category': toar_df['toar_category'].iloc[i],
                     'pop_density': toar_df['pop_density'].iloc[i],
                     'max_5km_pop_density': toar_df['max_5km_pop_density'].iloc[i],
                     'max_25km_pop_density': toar_df['max_25km_pop_density'].iloc[i],
                     'nightlight_1km': toar_df['nightlight_1km'].iloc[i],
                     'nightlight_max_25km': toar_df['nightlight_max_25km'].iloc[i],
                     'nox_emi': toar_df['NOx_emi'].iloc[i],
                     'omi_nox': toar_df['OMI_NOx'].iloc[i],
                    }    
        # assign this extra stuff
        long_df_and_attributes = datetime_df.assign(**dict_of_stuff)
        
        # here setting datetime as the index before saving. I think this is a waste of time.
        #long_df_and_attributes['datetime'] = pd.to_datetime(long_df_and_attributes['datetime'], format='%Y-%m-%d')
        #long_df_and_attributes = long_df_and_attributes.set_index('datetime')    
        
        # save each individual row, identified by the station, species, and series id?
        long_df_and_attributes.to_csv('/home/jovyan/lustre_scratch/cas/european_data_new_temp/per_series/'+country+'/env/'+toar_df['station_name'].iloc[i].replace("/", "")+'_'+toar_df['variable_name'].iloc[i]+'_'+clean_array_of_datetime[1][0:10]+'_df.csv', 
                                      index=False)

    ## I would like to write this loop so that I can actually stop the loop.
    ## I need something like an except: KeyboardInterrupt, but I am not sure how it works at the moment.
    
    except:
        print('No good')
        

## Let's try to concatenate everything into one dataframe...then do some kind of sorting, and concatenating to achieve what we want...
## this is gonna be slow

path = r'/home/jovyan/lustre_scratch/cas/european_data_new_temp/per_series/'+country+'/env' # use your path

# here we make lists of all files containing a particular species
all_temp_files = glob.glob(os.path.join(path, "*_temp_*.csv"))
all_press_files = glob.glob(os.path.join(path, "*_press_*.csv"))
all_u_files = glob.glob(os.path.join(path, "*_u_*.csv"))
all_v_files = glob.glob(os.path.join(path, "*_v_*.csv"))
all_totprecip_files = glob.glob(os.path.join(path, "*_totprecip_*.csv"))
all_pblheight_files = glob.glob(os.path.join(path, "*_pblheight_*.csv"))
all_relhum_files = glob.glob(os.path.join(path, "*_relhum_*.csv"))
all_cloudcover_files = glob.glob(os.path.join(path, "*_cloudcover_*.csv"))

# here we concatenate all the files containing a certain species
df_temp = pd.concat((pd.read_csv(f) for f in all_temp_files), ignore_index=False)
df_press = pd.concat((pd.read_csv(f) for f in all_press_files), ignore_index=False)
df_u = pd.concat((pd.read_csv(f) for f in all_u_files), ignore_index=False)
df_v = pd.concat((pd.read_csv(f) for f in all_v_files), ignore_index=False)
df_totprecip = pd.concat((pd.read_csv(f) for f in all_totprecip_files), ignore_index=False)
df_pblheight = pd.concat((pd.read_csv(f) for f in all_pblheight_files), ignore_index=False)
df_relhum = pd.concat((pd.read_csv(f) for f in all_relhum_files), ignore_index=False)
df_cloudcover = pd.concat((pd.read_csv(f) for f in all_cloudcover_files), ignore_index=False)


#define list of DataFrames
dfs = [df_temp, df_press, df_u, df_v, df_totprecip, df_pblheight, df_relhum, df_cloudcover]

#merge all DataFrames into one
final_df = reduce(lambda  left,right: pd.merge(left,right,on=['datetime', 'station_name', 'lat', 'lon', 'alt', 'station_etopo_alt',  
                                                              'station_rel_etopo_alt', 'station_type', 	'landcover', 
                                                              'toar_category', 'pop_density', 'max_5km_pop_density', 'max_25km_pop_density', 
                                                              'nightlight_1km', 'nightlight_max_25km', 'nox_emi', 'omi_nox'],
                                            how='outer'), dfs)


final_df_sorted = final_df.sort_values(['station_name', 'datetime'], ignore_index=True)

final_df_sorted['datetime'] = pd.to_datetime(final_df_sorted['datetime'], format='%Y-%m-%d')

final_df_sorted_dropna = final_df_sorted.dropna()
                      

# make a directory to save files in if it doesn't already exist with path 
path = '/home/jovyan/lustre_scratch/cas/european_data_new_temp/country/'+country+'/env/'
                      
try: 
    os.makedirs(path) 
except OSError as error: 
    print(error)                        

                      

final_df_sorted.to_csv('/home/jovyan/lustre_scratch/cas/european_data_new_temp/country/'+country+'/env/env_data.csv', index=False)

final_df_sorted_dropna.to_csv('/home/jovyan/lustre_scratch/cas/european_data_new_temp/country/'+country+'/env/env_dropna_data.csv', index=False)

