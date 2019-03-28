import pandas as pd
import time
import os 

print( "\n ---------- Hellow (world) there!\n ------------ " )



### ----- make some dates… 

def makedates( start_timestamp, how_many_days_dates_to_produce ):

	# make a timestamp starting at midnight 
	start_day_at_midnight = pd.Timestamp( start_timestamp.year, start_timestamp.month, start_timestamp.day )

	print(">>>> makedates : hellow : startig from "+str( start_day_at_midnight)+" we're making "+str( how_many_days_dates_to_produce)+" days " )

	# prepare to loop and make new days 
	out_days = []
	# 
	for day_offset_index in range( how_many_days_dates_to_produce ):
		out_days.append( start_day_at_midnight + pd.DateOffset( day_offset_index ) )

	print("---- finished, making "+str( len( out_days ))+" days … eg the following " )
	print( out_days )

	return out_days


## -- run?!

days_to_make = makedates( pd.Timestamp( 2019, 3, 1), 20 )






### ---- run the day generating script… with the days

def make_24_hour_files_of_tabular_luftdaten_data( days_to_make ):

	print(">>>> make_24_hour_files_of_tabular_luftdaten_data() : making tabular data from luftdaten data, for "+str( len( days_to_make) )+"days,\n\t starting with day "+str( days_to_make[0] )+" and finishing with day "+str( days_to_make[-1] )+" - \n ")

	start_time = time.time()

	for date_ in days_to_make:
		print("\n\n ==============================================================================================")
		print("===== make_24_hour_files_of_tabular_luftdaten_data() : making date "+str( date_ )+" at time "+str( time.time() -start_time ) )
		print("\n\n ==============================================================================================")

		date_with_only__year_month_day__as_str = str( date_.year )+"-"+str( date_.month )+"-"+str( date_.day )

		os.system( "/root/anaconda3/bin/python3 /mnt/virtio-bbc6cf3a-042b-4410-9/luftdaten/luftdaten_code/dustmin_to_csv__various_code/make_tabular_csv_data_from_luftdaten_data___TRY_5__using__PD_RESAMPLE__WITH_REAL_TEST_DATA_08.py "+str( date_with_only__year_month_day__as_str ) )

	# and in the end 
	print("\n\n ==============================================================================================")
	print("===== FIIIIIIIINNNNNIIIIISSSSSHHHHHEEEEEDDDDD in "+str( time.time() - start_time )+" seconds" )
	print("\n\n ==============================================================================================")


## -- run 

make_24_hour_files_of_tabular_luftdaten_data( days_to_make )