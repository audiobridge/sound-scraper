from __future__ import print_function
import freesound
import os
import sys
import json

api_key = os.getenv('FREESOUND_API_KEY', 'Q20UuCpItgvCIlTvzpoFsh9NxoNKXnaz9plBkw3X')
freesound_client = freesound.FreesoundClient()
freesound_client.set_token(api_key)

# Get text info details
print("Search for harmonica Keyword:")
results_pager = freesound_client.text_search(
    query="harmonica",
    fields="id,username"
)
print("Num results:", results_pager.count)
f = open("myfile.txt", "a")
flag = 1
# Page wise data loop - start fetching...
while flag != None: # print(vars(results_pager))
    for text_data in results_pager:
        sound = freesound_client.get_sound(
            text_data.id,
            fields="id,name,tags,created,type,channels,filesize,bitrate,bitdepth,duration,samplerate,download,images,analysis_stats,ac_analysis"
        )
        sound_dict = sound.as_dict()
        # print(sound_dict)
        f.write(json.dumps(sound_dict))
        f.write('\n') # Next page data pass in ptr

    results_pager = results_pager.next_page()
    flag = results_pager.next # End - Page wise data fetching...

# Last page data fetch - Start fetching..
for text_data in results_pager:
        sound = freesound_client.get_sound(
            text_data.id,
            fields="id,name,tags,created,type,channels,filesize,bitrate,bitdepth,duration,samplerate,download,images,analysis_stats,ac_analysis"
        )
        sound_dict = sound.as_dict()
        # print(sound_dict)
        f.write(json.dumps(sound_dict))
        f.write('\n') # End - Last page data fetch End
f.close()
exit()







