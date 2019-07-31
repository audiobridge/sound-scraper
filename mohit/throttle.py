import datetime, time

class FreesoundThrottle():
	onedaysec = 1*24*60*60
	oneminsec = 1*60
	apiKeyList = ['Q20UuCpItgvCIlTvzpoFsh9NxoNKXnaz9plBkw3X','wLDvgpuiWsXZP8QVmSUIixeHDjmogiWH9k72PpDO','RI4iCamKAABlCVDXStAx49pnNVmb0XSzc6po1qbk']

	def cycleCheck(self, throttle_check):

	    if(throttle_check > 0):
	        # Sleep for needed time plus 5s as a buffer
	        time_correction = throttle_check + 5
	        print("Sleeping for " + str(time_correction) + " seconds to control throttling.")
	        time.sleep(time_correction)

	def sleepThrottle(self, response, pointer):
		api_key = self.apiKeyList[pointer]

		if("60/minute" in response['detail']):
			now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
			print("\n----- Throttle Limit Occured: Run after 1min (" + now + ") !! ----------")
			time.sleep(self.oneminsec)
		elif("2000/day" in response['detail']):
			print("\n----- 24 Hour Throttle Limit Occured; Cycling to next API Key ----------")
			pointer = pointer + 1
			api_key, pointer = self.apiKeyCycle(pointer)
			
			# If pointer is returned as 0, it means we have cycled through all available keys and need to take a quick rest
			if(pointer == 0):
				print("----- All API Keys used for the day; Resting for 24 hours ----------")
				time.sleep(self.onedaysec)

			print("----- API Key Switched to " + api_key + " ----------")

		return api_key, pointer

	def apiKeyCycle(self, pointer):
		apiKeyCount = len(self.apiKeyList)

		# If we have reached the end of the list, start back from the beginning and return 0 as the pointer
		if(pointer >= apiKeyCount):
			pointer = 0

		return self.apiKeyList[pointer], pointer