from __future__ import print_function, division
from argparse import ArgumentParser
import freesound
import os
import sys
import json
import mysql.connector
import math
import datetime
import time

parser = ArgumentParser()
parser.add_argument("-d", "--daily_throttle", help="Turn off the daily throttle limit")
args = parser.parse_args()

# Start time
start_time = time.time()
api_call_count = 0
minutes_passed = 0

# Set API minute limits based on throttling
# 60 seconds in a minute
api_minute_limit = 60
api_second_limit = api_minute_limit/60

# Set API daily limits based on throttling
# 1440 minutes in a day
api_daily_limit = 2000
api_daily_minute_limit = api_daily_limit/1440

def throttleCheck(api_call_count, elapsed_time, time_period):
    if(time_period == 'per_second'):
        throttle_check_per_second = api_call_count/elapsed_time
        if(throttle_check_per_second > api_second_limit):
            over_limit = throttle_check_per_second - api_second_limit
            # Sleep for needed time plus 5s as a buffer
            print("Performing per second throttling. " + str(over_limit) + " API calls over per second limit.")
            time_correction = throttle_check_per_second + 5
            print("Sleeping for " + str(time_correction) + " seconds to control throttling.")
            time.sleep(time_correction)

    if(time_period == 'per_minute'):
        throttle_check_per_minute = api_call_count/(elapsed_time/60)
        print("Per minute limit per day = " + str(api_daily_minute_limit))
        print("Current API calls per minute = " + str(throttle_check_per_minute))
        if(throttle_check_per_minute > api_daily_minute_limit):
            over_limit = throttle_check_per_minute - api_daily_minute_limit
            # Sleep for needed time plus 5s as a buffer
            print("Performing per minute throttling. " + str(over_limit) + " API calls over per minute limit.")
            time_correction = (throttle_check_per_minute/60) + 5
            print("Sleeping for " + str(time_correction) + " seconds to control throttling.")
            time.sleep(time_correction)

mydb = mysql.connector.connect(host="localhost",user="root",passwd="password",database="db_freesound")
mycursor = mydb.cursor()
mycursor.execute("SELECT item_name,current_page FROM search_keys where search_keys.status != 2 order by search_keys.current_page desc")
myresult = mycursor.fetchall()

api_key = os.getenv('FREESOUND_API_KEY', 'Q20UuCpItgvCIlTvzpoFsh9NxoNKXnaz9plBkw3X')
freesound_client = freesound.FreesoundClient()
freesound_client.set_token(api_key)

for keyStrg in myresult:
    key_string = keyStrg[0]
    key_page = keyStrg[1]
    print("===========Start key========== Page:",key_page,"========:",key_string)
    
    # Get text info details
    results_pager = freesound_client.text_search(
        page=key_page,
        page_size=150,
        query=key_string,
        fields="id,username"
    )
    api_call_count+=1

    print("Num results:", results_pager.count)
    total_page = math.ceil(results_pager.count / 150)
    print("Total pages:", total_page)

    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    sql = "UPDATE search_keys SET current_page=%s,total_page=%s,records=%s,status=%s,updated_at=%s WHERE item_name=%s"
    val = (key_page, total_page, results_pager.count, 1, now,key_string)
    mycursor.execute(sql, val)
    mydb.commit()

    # ---------Page wise data loop - start fetching-----------
    while total_page >= key_page: # print(vars(results_pager))
        for text_data in results_pager:
            sql = "SELECT * FROM tbl_sounds WHERE freesound_id = %s"
            adr = (text_data.id,)
            mycursor.execute(sql, adr)
            isDup = mycursor.fetchall()
            if isDup:
                print('Duplicate entry : skipped - ',text_data.id)
                continue
            else:
                # print('Start Now...',text_data.id)
                sound = freesound_client.get_sound(
                    text_data.id,
                    fields="id,name,tags,created,type,channels,filesize,bitrate,bitdepth,duration,samplerate,download,images,analysis_stats,ac_analysis"
                )
                api_call_count+=1

                sound_dict = sound.as_dict()
                # exit()
                sql = "INSERT INTO tbl_sounds (freesound_id,search_key,name,filesize,duration, json_dump, created) VALUES (%s,%s,%s,%s,%s,%s,%s)"
                val = (sound.id,key_string, sound.name, sound.filesize, sound.duration,(json.dumps(sound_dict)),sound.created)
                print("\nProcessing Freesound ID: ", sound.id)
                try:
                    mycursor.execute(sql, val)
                    mydb.commit()
                    print("Inserted row: ", sound.id)
                    # your code

                    # Print elapsed time
                    elapsed_time = time.time() - start_time
                    print("Time elapsed: ", elapsed_time)

                    # Print number of API calls
                    print("API calls: ", str(api_call_count))

                    ###
                    # After every API call, make a throttle adjustment per second
                    ###
                    throttleCheck(api_call_count, elapsed_time, 'per_second')

                    ###
                    # Only run if daily throttle has not been set to false
                    ###
                    if(args.daily_throttle != 'false'):
                        new_minutes_passed = int(elapsed_time/60)
                        print("Minutes passed: ", str(new_minutes_passed))

                        if new_minutes_passed > minutes_passed:
                            minutes_passed = new_minutes_passed

                            ###
                            # After every minute passed, make a throttle adjustment per minute
                            ###
                            throttleCheck(api_call_count, elapsed_time, 'per_minute')

                except mysql.connector.IntegrityError as err:
                    print("Duplicate data Error: {}".format(err))

        #----------- Update key serach table on next api call --------------------------
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sql = "UPDATE search_keys SET current_page=%s,status=%s,updated_at=%s WHERE item_name=%s"
        if total_page != key_page:
            key_page += 1
            val = (key_page,1,now,key_string)
            print("=========== Key:", key_string, "========== Page:", key_page, "========")
            results_pager = results_pager.next_page()
        else:
            val = (key_page, 2, now, key_string)
            print("============== End of page search ===================")
            key_page += 1
        mycursor.execute(sql, val)
        mydb.commit()
exit()