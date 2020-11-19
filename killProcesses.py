import os
import time

def kill_Proc():

	time.sleep(2)
	print("I slept for 2 seconds")
	os.system("for /f \"tokens=1\" %i in ('jps -m ^| find \"Worker\"') do ( taskkill /F /PID %i )")
