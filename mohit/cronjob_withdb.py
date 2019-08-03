from __future__ import print_function
import freesound
import throttle
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

mydb = mysql.connector.connect(host="localhost",user="root",passwd="",database="db_freesound")
mycursor = mydb.cursor()
mycursor.execute("SELECT item_name,current_page FROM search_keys where search_keys.status != 2 order by search_keys.current_page desc")
myresult = mycursor.fetchall()

throttleCheck = throttle.FreesoundThrottle()

apiKeyPointer = 0
api_key, apiKeyPointer = throttleCheck.apiKeyCycle(apiKeyPointer)

freesound_client = freesound.FreesoundClient()
freesound_client.set_token(api_key)

for keyStrg in myresult:
    key_string = keyStrg[0]
    if(keyStrg[1] > 1):
        key_page = keyStrg[1]
    else:
        key_page=1

    print("===========Start key: ",key_string,"==========\n========== Page:",key_page,"========")
    
    while(True):
        # Get text info details
        results_pager = freesound_client.text_search(
            page=key_page,
            page_size=150,
            query=key_string,
            fields="id,name,tags,created,type,channels,filesize,bitrate,bitdepth,duration,samplerate,download,images,analysis_stats,ac_analysis"
        )

        api_call_count +=1

        # Check to see if the response is a paged Freesound response
        # if not, continue
        if isinstance(results_pager, freesound.Pager): break
            
        response = json.loads(results_pager)

        # The 'detail' key in the response is only return on a 429 error.
        # We look for that key and if we do not find it, we know it was a successful API call and we break out of the while loop.
        # Otherwise we throttle check and repeat.
        if('detail' not in response):
            break
    
        # During the sleep throttle, if it is a single minute,
        # it just returns the currently used API key, if it is 24 hours,
        # it cycles to the next API key
        api_key, apiKeyPointer = throttleCheck.sleepThrottle(response, apiKeyPointer)
        freesound_client.set_token(api_key)

    print("Num results:", results_pager.count)
    total_page = int(math.ceil(results_pager.count / 150))
    print("Total pages:", total_page)

    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    sql = "UPDATE search_keys SET current_page=%s,total_page=%s,records=%s,status=%s,updated_at=%s WHERE item_name=%s"
    val = (key_page, total_page, results_pager.count, 1, now,key_string)
    mycursor.execute(sql, val)
    mydb.commit()

    # ---------Page wise data loop - start fetching-----------
    while total_page >= key_page: # print(vars(results_pager))
        for sound in results_pager:
            sql = "SELECT id FROM tbl_sounds WHERE freesound_id = %s"
            adr = (sound.id,)
            print("\nChecking database for duplicates...")
            mycursor.execute(sql, adr)
            isDup = mycursor.fetchall()
            if isDup:
                print('Duplicate entry : skipped - ', sound.id)
                continue
            else:
                sound_dict = results_pager.as_dict()
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
                    print("API calls: ", str(api_call_count))

                    throttle_check = api_call_count - elapsed_time
                    throttleCheck.cycleCheck(throttle_check)

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
            api_call_count +=1
        else:
            val = (key_page, 2, now, key_string)
            print("============== End of", key_string, " page search ===================\n")
            key_page += 1
        mycursor.execute(sql, val)
        mydb.commit()
exit()