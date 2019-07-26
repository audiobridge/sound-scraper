from __future__ import print_function
import freesound
import os
import sys
import json
import mysql.connector
import math
import datetime
import time

# ------Start time--------
start_time = time.time()
api_call_count = 0

api_minute_limit = 60
api_second_limit = api_minute_limit/60

onedaysec = 1*24*60*60
oneminsec = 1*60

def throttleCheck(throttle_check):
    if(throttle_check > 0):
        # Sleep for needed time plus 5s as a buffer
        time_correction = throttle_check + 5
        print("Sleeping for " + str(time_correction) + " seconds to control throttling.")
        time.sleep(time_correction)

def sleepThrottle(response):
    if("60/minute" in response['detail']):
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print("\n----- Throttle Limit Occured: Run after 1min !! ----------", now)
        time.sleep(oneminsec)
    elif("2000/day" in response['detail']):
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print("\n----- Throttle Limit Occured: Run after 24hrs!! ----------", now)
        time.sleep(onedaysec)

mydb = mysql.connector.connect(host="localhost",user="root",passwd="",database="db_freesound")
mycursor = mydb.cursor()
mycursor.execute("SELECT item_name,current_page FROM search_keys where search_keys.status != 2 order by search_keys.current_page desc")
myresult = mycursor.fetchall()

api_key = os.getenv('FREESOUND_API_KEY', 'Q20UuCpItgvCIlTvzpoFsh9NxoNKXnaz9plBkw3X')
# api_key = os.getenv('FREESOUND_API_KEY', 'Q20UuCpItgvCIlTvzpoFsh9NxoNKXnaz9plBkw3X')
freesound_client = freesound.FreesoundClient()
freesound_client.set_token(api_key)

for keyStrg in myresult:
    key_string = keyStrg[0]
    key_page = keyStrg[1]
    print("===========Start key: ",key_string,"==========\n========== Page:",key_page,"========")

    while(True):
        # Get text info details
        results_pager = freesound_client.text_search(
            page=key_page,
            page_size=150,
            query=key_string,
            fields="id,username"
        )
        api_call_count +=1
        response = json.loads(results_pager)

        if('detail' not in response):
            break
        
        sleepThrottle(response)

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
                print('Duplicate entry : skipped - ', text_data.id)
                continue
            else:
                while(True):
                    sound = freesound_client.get_sound(
                        text_data.id,
                        fields="id,name,tags,created,type,channels,filesize,bitrate,bitdepth,duration,samplerate,download,images,analysis_stats,ac_analysis"
                    )
                    api_call_count +=1
                    response = json.loads(sound)

                    if('detail' not in response):
                        break
                    
                    sleepThrottle(response)

                sound_dict = sound.as_dict()
                # exit()
                sql = "INSERT INTO tbl_sounds (freesound_id,search_key,name,filesize,duration, json_dump, created) VALUES (%s,%s,%s,%s,%s,%s,%s)"
                val = (sound.id,key_string, sound.name, sound.filesize, sound.duration,(json.dumps(sound_dict)),sound.created)
                print("Processing ",key_string," Freesound ID: ", sound.id)

                try:
                    mycursor.execute(sql, val)
                    mydb.commit()
                    print("Inserted row: ", sound.id)
                    # your code

                    # Print elapsed time
                    elapsed_time = time.time() - start_time
                    print("Time elapsed: ", elapsed_time)

                    # Print number of API calls
                    print("API calls: ", str(api_call_count), "\n")

                    throttle_check = api_call_count - elapsed_time
                    throttleCheck(throttle_check)

                except mysql.connector.IntegrityError as err:
                    print("Duplicate data Error: {}".format(err))

        #----------- Update key serach table on next api call --------------------------
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sql = "UPDATE search_keys SET current_page=%s,status=%s,updated_at=%s WHERE item_name=%s"
        if total_page != key_page:
            key_page += 1
            val = (key_page,1,now,key_string)
            print("======= Key:", key_string, "=========\n========= Page:", key_page, "=======")
            results_pager = results_pager.next_page()
        else:
            val = (key_page, 2, now, key_string)
            print("============== End of", key_string, " page search ===================\n")
            key_page += 1
        mycursor.execute(sql, val)
        mydb.commit()
exit()