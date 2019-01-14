



# ------------------------ intiialisation 

import psycopg2
import time
import pandas as pd
import numpy as np

# for exporting
import json

# for copying variables 
import copy



# ------------------ get going 

initial_sleeptime = 0.25 

print("||||||||| Starting soon! - Sleeping a bit to give the script to connect with the database - "+str(initial_sleeptime)+" seconds sleep  ||||||||||||||||||||||")
time.sleep( initial_sleeptime )


""" - moved to a function >> connect_to_db_return_cursor() <<
# connect to db
conn = psycopg2.connect("dbname='luftdaten_capture_01' user='postgres' password='secret' host='localhost' ")

# get cursor
cur = conn.cursor()
"""

# get basic start-time
original_start_time = time.localtime()





# ------------------------- parameters


# -- data

# 
## data_url = "/Users/miska/Documents/open_something/luftdaten/luftdaten_code/luftDaten__various_data_downloads/20190104_data_exploration/ld_NYE_midnight_24hrs_nordics_all_data_01.csv"
# smaller
data_url = "miskas-MacBook-Pro:~ en__various_data_downloads/20190104_data_exploration/ld_NYE_midnight_24hrs_nordics_100x_less_data_01.csv"

loaded_data = []

sensors_and_their_coords = [ [ "sensor_id", "lat", "lon"], [ "sensor_id", "lat", "lon"], [ "sensor_id", "lat", "lon"]  ]

unique_sensor_ids = [] 

first_df_of_unique_sensor_id = -1


#--- set some time parameters , for the data fetching

end_time = "2019-01-01 12:00:00"

number_of_hours_of_data_to_fetch = 24*1


#--- parameters for the output

time_interval_in_output_data__in_seconds = 60*5




# --- rounding 

output_measurement_values__significant_figures_length = 3










# ----------------------- functions etc


def connect_to_db_return_cursor():
	print(">>>>> connect_to_db_return_cursor () ")

	# connect to db
	conn = psycopg2.connect("dbname='luftdaten_capture_01' user='postgres' password='secret' host='localhost' ")

	# get cursor
	cur = conn.cursor()

	return conn, cur


# --- time data things

# FIX ME FIX ME FIX ME FIX ME 
# FIX ME FIX ME FIX ME FIX ME 
# FIX ME FIX ME FIX ME FIX ME 
# FIX ME FIX ME FIX ME FIX ME 

def construct_timeseries_of_relev_hour_time_period( number_of_hours_of_data_to_fetch, end_time  ):

	print(" >>>> construct_timeseries_of_relev_hour_time_period > number_of_hours_of_data_to_fetch : "+str( number_of_hours_of_data_to_fetch )+"   time_interval_in_output_data__in_seconds : "+str( time_interval_in_output_data__in_seconds) )



# --- data things 

def fetch_some_data_from_db():
	print( ">>>> fetch_some_data_from_db() ")

	data = pd.read_csv( data_url ) 

	print( "-- got data - of shape : "+str( data.shape ))

	return data 


def fetch_unique_sensor_ids( data_loaded ):
	print( ">>>> fetch_unique_sensor_ids() ")

	unique_sensor_ids = data_loaded['sensor_id'].unique()

	print(" -- got "+str( len(unique_sensor_ids) )+" unique sensor ids" )

	return unique_sensor_ids


def convert_timestamp_col_into_timestamp_col( loaded_data ):
	print( ">>>> convert_timestamp_col_into_timestamp_col() ")

	df_with_timestamp_col_as_timestamp_type = pd.to_datetime( loaded_data['timestamp'] )

	return df_with_timestamp_col_as_timestamp_type



def fetch_all_rows_of_random_unique_sensor_id( random_sensor_id_num, sensor_ids, loaded_data ):
	
	print( ">>>> fetch_all_rows_of_random_unique_sensor_id() - random_sensor_id_num === "+str( sensor_ids[ random_sensor_id_num] ))

	df__rows_of_unique_sensor_id = loaded_data[ loaded_data['sensor_id'] == sensor_ids[ random_sensor_id_num ]  ]

	print(" --- got rows of shape "+str( df__rows_of_unique_sensor_id.shape ) )	

	return df__rows_of_unique_sensor_id







# ----------------------- run

print(" Hallo! - Alles gut! ")


# disabled until we have a proper db connection
#### conn, cur = connect_to_db_return_cursor()

# temperary fix 
loaded_data = fetch_some_data_from_db()

# make sure the timestmap col is a timestamp col type
loaded_data['timestamp'] = convert_timestamp_col_into_timestamp_col( loaded_data )

unique_sensor_ids = fetch_unique_sensor_ids( loaded_data )

all_rows_of_one_sensor_id = fetch_all_rows_of_random_unique_sensor_id( 0, unique_sensor_ids, loaded_data )













