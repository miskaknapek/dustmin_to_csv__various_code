#!/usr/bin/env python
# coding: utf-8

# # Luftdaten data : data cleaning, resampling - mini version - cleaning v5
# 
# # - TRYING TO HEAD FOR FINAL… 
# ## Code builds a continuous time tabular version of the luftdaen data, such that the same time period is present for each sensor in the data, regardless of whether each sensor has data for all the time slots. 
# 

"""
TO DO :  TO  DO :  TO  DO :  TO  DO :  TO  DO
--------------------------

- FIX THE NAN! 

- Run on server 
    - Do psql query! 

-- do the time setting if one's calculating 24 hour of data 

"""


import pandas as pd
import numpy as np
import time
# db
import psycopg2






## --- --- --- --- --- --- SWITCH ON/OFF PRINTING?


## --- --- --- --- --- --- SWITCH ON/OFF PRINTING?

print2 = print

def print( x ):
    return 1


## --- --- --- --- --- --- KEY SWITCHES

do_sql_data_fetch = True

do_resampling_loop = True 

saving_data = False 




## --- --- --- --- --- --- PARAMETERS

# for TIMING the code 
total_start_time = time.time()



## -- file-paths and file-names

# which directory is the file in? 
basic_file_path_to_final_file = "" 

# filename beginning for files about the laest data

file_name__for__generate_data_since_midnight = "latest_data_since_midnight"

file_name__for__generate_given_24_houss = "24_hrs_pm_data_"

file_name_suffix = ".json"


## -- --- --- ---  DATA URLS 


nordic_midnight_24_hrs_data__url = "/Users/miska/Documents/open_something/luftdaten/luftdaten_code/luftdaten__make_tabular_data__from_db_data/ld_NYE_midnight_24hrs_nordics_all_data_01.csv"
## nordic_midnight_24_hrs_data__url = "/home/miska/documents/opensomething/luftdaten/dustmin_to_csv__various_code/ld_NYE_midnight_24hrs_nordics_all_data_01.csv"
# nordic_midnight_24_hrs_data__url = "/Users/miska/temp_temp_temp/sds011_files_wo_microsecond_nulling/2018-09_sds011.csv_cleaned.csv"
nordic_midnight_24_hrs_data__url = "/mnt/virtio-bbc6cf3a-042b-4410-9/luftdaten/luftdaten_daten/2019-01_sds011.csv_cleaned__FIRST_500k.csv"

# DATA URL 
curr_url = nordic_midnight_24_hrs_data__url


# where the BIG DATA goes! 
in_data = 0





## -- --- --- ---  BASIC VARIABLES ( set up )


# 
list_of_unique_sensor_ids = []

# time length of sample period
time_length_of_sample_period__in_seconds = 60*3

time_length_of_sample_period__in_seconds__as_pandas_resampleing_time = str(time_length_of_sample_period__in_seconds)+"S"
print(" time_length_of_sample_period__in_seconds__as_pandas_resampleing_time : "+time_length_of_sample_period__in_seconds__as_pandas_resampleing_time )

# how many time sample periods can fit in the current total data time period
num_of_sample_time_periods_in_whole_time_span__for_basic_data = 0 


## -- general operations - what kind of time length in the data, are we outputting

# PLACEHOLDER FOR SYSARGS! 
# PLACEHOLDER FOR SYSARGS! 
# PLACEHOLDER FOR SYSARGS! 
do_time_since_midnight = False
# PLACEHOLDER FOR SYSARGS! 
# PLACEHOLDER FOR SYSARGS! 
# PLACEHOLDER FOR SYSARGS! 


# PLACEHOLDER FOR SYSARGS! 
# PLACEHOLDER FOR SYSARGS! 
# PLACEHOLDER FOR SYSARGS! 
input_timedate = pd.Timestamp( 2019, 2, 21 )
start_date__if_doing_24_hours_data = input_timedate
# PLACEHOLDER FOR SYSARGS! 
# PLACEHOLDER FOR SYSARGS! 
# PLACEHOLDER FOR SYSARGS! 



# for when we're creating 24 hour time segements
default__generate_data_for_24_hour_period_starting_from_starttime = 'default__generate_data_for_24_hour_period_starting_from_starttime'

# for when we're creating data files for the period until now
default__generate_data_from_last_midnight_until_current_time =  "default__generate_data_from_last_midnight_until_current_time"

# which are we using? 
if do_time_since_midnight == True : 
    current_time_duration_in_data_generation = default__generate_data_from_last_midnight_until_current_time
else : 
    current_time_duration_in_data_generation = default__generate_data_for_24_hour_period_starting_from_starttime







## -- --- --- --- FUNCTION : How many sample periods fit into the time duration examined?


def figure_out_how_many_sample_time_periods_fit_in_desired_sample_time_duration( which_time_duration_are_we_using, time_length_of_sample_period__in_seconds ):
    
    print(">>> figure_out_how_many_sample_time_periods_fit_in_desired_sample_time_duration : checking time period : |"+which_time_duration_are_we_using+"| single sample time length : "+str(time_length_of_sample_period__in_seconds) )
    
    num_of_sample_time_periods_fit_in_total_sampled_period = 0
    
    if which_time_duration_are_we_using == 'default__generate_data_for_24_hour_period_starting_from_starttime':
        num_of_sample_time_periods_fit_in_total_sampled_period = int( ( 24*60*60 / time_length_of_sample_period__in_seconds ) ) 
    
    if which_time_duration_are_we_using == 'default__generate_data_from_last_midnight_until_current_time':
        ## - get time since midnight 
        
        # generate timestamp, to get time since midnight 
        end_timestamp = pd.Timestamp.now()
        hours_seconds_since_midnight = end_timestamp.hour*60*60
        min_seconds_since_midnight = end_timestamp.minute*60
        seconds_since_midnight = hours_seconds_since_midnight + min_seconds_since_midnight + end_timestamp.second
        
        print( "\n -- the number of seconds since midnight is "+str( seconds_since_midnight ) )
        
        num_of_sample_time_periods_fit_in_total_sampled_period = int( seconds_since_midnight / time_length_of_sample_period__in_seconds ) 
        
        
    print("\n - - - - FINALLY : num_of_sample_time_periods_fit_in_total_sampled_period : "+str( num_of_sample_time_periods_fit_in_total_sampled_period ))
    
    return num_of_sample_time_periods_fit_in_total_sampled_period


# --- Generate relevant timestamps


# make variables
end_timestamp = 0
start_timestamp = 0

# if generating data from last midnight until current time
if current_time_duration_in_data_generation == default__generate_data_from_last_midnight_until_current_time:
    # the start timestamp would be last midnight 
    timestamp_now = pd.Timestamp.now()
    start_timestamp = pd.Timestamp( timestamp_now.year, timestamp_now.month, timestamp_now.day, 0 )
    # timestamp last midnight 
    end_timestamp = pd.Timestamp.now()
# or, ir generating data from midnight to midnight
elif current_time_duration_in_data_generation == default__generate_data_for_24_hour_period_starting_from_starttime:
    pass
    # the start timestamp would be last midnight 
    start_timestamp = start_date__if_doing_24_hours_data
    # timestamp last midnight 
    end_timestamp = start_timestamp + pd.DateOffset(1)


# time feedback 
print2( "\n-- generating relevant start/end tiemstamps, in mode |"+str(current_time_duration_in_data_generation)+"| \n \t got start time "+str(start_timestamp )+" and end time "+str( end_timestamp ) )




# --- set up blank start and end rows

in_data__START_TIME__blank_row = pd.DataFrame( data={"p1": np.NaN, "p2" : np.NaN }, index=pd.DatetimeIndex( [start_timestamp] ) )

in_data__END_TIME__blank_row = pd.DataFrame( data={"p1": np.NaN, "p2" : np.NaN }, index=pd.DatetimeIndex( [end_timestamp] ) )



# -- -- -- FETCH DATA! 



if do_sql_data_fetch == True:

    mini_timing = time.time() 

    print2("-- -- attempting to connect to DB, executing query ")

    conn = psycopg2.connect("dbname='ld_realtime_data' user='postgres' password='secret' host='localhost' ")

    cur = conn.cursor()

    query = "select * from fill_me_w_ld_daten_02 where timestamp > '"+str( start_timestamp )+"' "
    print2( "-- -- running psql query |"+query+"|" )

    cur.execute( "select * from fill_me_w_ld_daten_02 where timestamp > '"+str( start_timestamp )+"'" )

    print2("-- --- query ready at "+str( time.time() - mini_timing ) ) 

    in_data = pd.DataFrame( cur.fetchall(), columns=['sensor_id', 'sensor_name', 'lat', 'lon', 'timestamp', 'p1', 'p2'] )

    print2("--- --- got table of shape "+str( in_data.shape )+" "+str( time.time() - mini_timing ) )    
    print2("--- --- got columns : |"+str( in_data.columns)+"|" )

 
# READ DATA FROM FILE
elif do_sql_data_fetch == False : 
 
    # READ DAtA 
    in_data = pd.read_csv( curr_url, sep=";", header=0 )
    # in_data.shape

    print2("-- loaded data, and got columns : "+str( in_data.columns ) )
    print2("-- loaded data, and it took "+str( time.time() - total_start_time )+" to load data of shape "+str( in_data.shape ) )


# --- POST READ DATA : set up data - timestamps and all! 

# aha - timestamp column not a timestamp column?
# - let's fix 
in_data['timestamp'] = pd.to_datetime( in_data['timestamp'] )

# set the timestamp column as the index 
in_data = in_data.set_index( 'timestamp' )

# sort data chronologically, by the index 
in_data = in_data.sort_index()





## -- --- --- --- set up data holders


# --- test fetching the number of sample periods in 24 hours 
num_of_sample_time_periods_in_whole_time_span__for_basic_data = 0

# if generating data from last midnight until current time
if current_time_duration_in_data_generation == default__generate_data_from_last_midnight_until_current_time:
    # test fetching the number of sample periods since last midnight 
    num_of_sample_time_periods_in_whole_time_span__for_basic_data = figure_out_how_many_sample_time_periods_fit_in_desired_sample_time_duration( default__generate_data_from_last_midnight_until_current_time, time_length_of_sample_period__in_seconds )
#
# or, ir generating data from midnight to midnight
elif current_time_duration_in_data_generation == default__generate_data_for_24_hour_period_starting_from_starttime:
    num_of_sample_time_periods_in_whole_time_span__for_basic_data = figure_out_how_many_sample_time_periods_fit_in_desired_sample_time_duration( default__generate_data_for_24_hour_period_starting_from_starttime, time_length_of_sample_period__in_seconds )



# --- Get the list of unique sensor ids

list_of_unique_sensor_ids = in_data['sensor_id'].unique()
print2( "-- got "+str( list_of_unique_sensor_ids.shape[0] )+" UNIQUE SENSOR IDS ")

# #### make OUT DATA variables

out_data__resampled_sensor_values_as_rows = np.array( np.zeros( list_of_unique_sensor_ids.shape[0] * num_of_sample_time_periods_in_whole_time_span__for_basic_data ) )

# reshape 
out_data__resampled_sensor_values_as_rows = out_data__resampled_sensor_values_as_rows.reshape( [ list_of_unique_sensor_ids.shape[0], num_of_sample_time_periods_in_whole_time_span__for_basic_data ] )

# make p1 and p2 versions of the out array 
out_data__resampled_sensor_values_as_rows__p1_values = out_data__resampled_sensor_values_as_rows
out_data__resampled_sensor_values_as_rows__p2_values = out_data__resampled_sensor_values_as_rows
out_data__resampled_sensor_values_as_rows__p1_values.shape, out_data__resampled_sensor_values_as_rows__p2_values.shape

# --- TEMPORARY out data variable 
# ( until we run with live data, the size of the outputted data will be a bit wrong )
temp_out_data = []

# --- make variable for LAT LONG coordinates of sensors
# x2 so lat lon can be on one row 
sensors_lat_lon_list = np.array( np.zeros( list_of_unique_sensor_ids.shape[0] *2 ) )
sensors_lat_lon_list = sensors_lat_lon_list.reshape( [ list_of_unique_sensor_ids.shape[0], 2 ] )



# --- ---- --- --- RESAMPLE LOOP


# --- LOOP AND RESAMPLE!
# for timing 
start_time = time.time()

print2("\n -- -- GETTING READY TO LOOP! at time "+str( time.time() - total_start_time ) )


# loop
if do_resampling_loop : 
    for current_sensor_id_i in range( len( list_of_unique_sensor_ids[:] )):

        if current_sensor_id_i % 100 == 0 : 
            print2(" -- -- working on sensor id "+str(current_sensor_id_i)+" / "+str( list_of_unique_sensor_ids.shape[0] )+" at time "+str( ( time.time() - start_time ) )  ) 

        # get current sensor id 
        current_sensor_id = list_of_unique_sensor_ids[ current_sensor_id_i ]
        
        # get all the values of the current sensor id 
        curr_sensor_id__in_data = in_data[ in_data['sensor_id'] == current_sensor_id ]
        
        print("- - the first row looks like this "+str( curr_sensor_id__in_data.iloc[0] ) )
        
        # save lat/lon data
        sensors_lat_lon_list[ current_sensor_id_i, 0 ] = curr_sensor_id__in_data.iloc[0]['lat'] 
        sensors_lat_lon_list[ current_sensor_id_i, 1 ] = curr_sensor_id__in_data.iloc[0]['lon'] 
        # test output 
        print("- - - - lat lon output test : ")
        print( str( sensors_lat_lon_list[ current_sensor_id_i, 0 ] )+", "+str( sensors_lat_lon_list[ current_sensor_id_i, 1 ] ) )


        
        #     print("- - - current sensor lat/on : "+str( sensors_lat_lon_list[ current_sensor_id_i, 0 ] )+","+str( sensors_lat_lon_list[ current_sensor_id_i, 1 ] ) )
        #     print("- - - - input lat lon : "+str( curr_sensor_id__in_data[0]['lat'] )+","+str( curr_sensor_id__in_data[0]['lon'] ) )    
        
        
        # minimise the in_data, so it's easier to resample the p1 and p2 values
        curr_sensor_id__in_data = curr_sensor_id__in_data[ ['p1', 'p2'] ]
        
        print("\n- - working on sensor id "+str( current_sensor_id)+" - - got "+str( curr_sensor_id__in_data.shape[0])+" rows of data" )
        
        print( "- - - got columns : "+str( curr_sensor_id__in_data.columns ) ) 
        
        ## INSERT bland start and end points 
        
        curr_sensor_id__in_data__with_blank_start_time = in_data__START_TIME__blank_row.append( curr_sensor_id__in_data )
        
        print("- - - current data length = "+str( curr_sensor_id__in_data__with_blank_start_time.shape ))
        
        curr_sensor_id__in_data__with_blank_start_AND_end_time = curr_sensor_id__in_data__with_blank_start_time.append( in_data__END_TIME__blank_row )
        
        print("- - - current data length = "+str( curr_sensor_id__in_data__with_blank_start_AND_end_time.shape ))   
        
        print( curr_sensor_id__in_data__with_blank_start_AND_end_time[:10] )
        print( curr_sensor_id__in_data__with_blank_start_AND_end_time[-10:] )    
        
        curr_sensor_id__in_data__with_blank_start_AND_end_time__RESAMPLED = curr_sensor_id__in_data__with_blank_start_AND_end_time.resample( time_length_of_sample_period__in_seconds__as_pandas_resampleing_time ).mean()
        
        print("\n- - - printing resampled data ( shape : "+str( curr_sensor_id__in_data__with_blank_start_AND_end_time__RESAMPLED.shape)+") ")
        
        print( curr_sensor_id__in_data__with_blank_start_AND_end_time__RESAMPLED[:10] )
        print( curr_sensor_id__in_data__with_blank_start_AND_end_time__RESAMPLED[-10:] )
        
        ## STORE DATA
        
        temp_out_data.append( curr_sensor_id__in_data__with_blank_start_AND_end_time__RESAMPLED )
        
    

    

## ----------- FINALIGNS 


print("\n ------- saving data? |"+str( saving_data )+"|" )


if saving_data == True : 

    ## --- --- prepare data for export 

    # convert lat lon array to a regular list, for expor 
    sensors_lat_lon_list = sensors_lat_lon_list.tolist()
    sensors_lat_lon_list


    # prepare p1 and p2 data … 





    # --- --- --- ---  finally prepare for export 
    # json : 
    # - data_time_period_start
    # - data_time_period_end
    # - sample_length
    # - num_of_sample_periods
    # - list_of_sensor_ids
    # - lat_lon # in the same order as sensor_ids
    # - p1_values # in the same order as sensor_ids
    # - p2_values # in the same order as sensor_ids



    exported_data = {}
    exported_data[ 'data_time_period_start' ] = "starttime_variable"
    exported_data[ 'data_time_period_end' ] = "endtime_variable"
    exported_data[ 'individual_data_sample_length_in_seconds' ] = time_length_of_sample_period__in_seconds
    exported_data[ 'num_of_sample_periods' ] = num_of_sample_time_periods_in_whole_time_span__for_basic_data
    exported_data[ 'sensor_ids' ] = list_of_unique_sensor_ids
    exported_data[ 'lat_lon' ] = sensors_lat_lon_list
    exported_data[ 'data__p1_values' ] = out_data__resampled_sensor_values_as_rows__p1_values
    exported_data[ 'data__p1_values' ] = out_data__resampled_sensor_values_as_rows__p2_values


    ## --- --- save data? 

    # ---  generate text version of data
    exported_data__as_json_string = json.dump( exported_data )

    # --- make filename 
    # 
    curr_filename = "nada.json"

    ## --  produce a different filename depending on which data generating mode we're in… 

    # if generating data from last midnight until current time
    if current_time_duration_in_data_generation == default__generate_data_from_last_midnight_until_current_time:
        curr_filename = file_name__for__generate_data_since_midnight+file_name_suffix
    #
    # or, ir generating data from midnight to midnight
    elif current_time_duration_in_data_generation == default__generate_data_for_24_hour_period_starting_from_starttime:
        curr_timedate = pd.Timestamp.now()
        curr_filename = file_name__for__generate_data_since_midnight+str( curr_timedate.yeah )+str( curr_timedate.month )+str( curr_timedate.date)+file_name_suffix

    print( "\n -- -- -- -- : saving filename |"+curr_filename+"|" )


    # save!
    fp = open( curr_filename, "w" )
    fp.write( exported_data__as_json_string )
    fp.close()

    # cleanup 
    del fp


###### --------- FINISHED for real?

print2("\n ||||||||||||||||||||||||||||| ENDE - finished after "+str( time.time() - total_start_time )+" seconds |||||||||||||||||||||||||||||||||" )


