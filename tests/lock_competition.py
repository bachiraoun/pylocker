"""
Launch many consoles and run the following scipt and verify how lock can be aquired
acquisition works.
"""
import uuid, time, os
from pylocker import Locker

n = 10000 

FL = Locker(filePath='test.txt', lockPass=str(uuid.uuid1()),lockPath='lock.txt', wait=0, timeout=0.1)

codes = {0:'i am locked, no one can touch me. I will wait 2 seconds.',
         1:'lock was already set. I will wait 2 more seconds.',
         2:'Old and forgotten lock for more than %i seconds is removed. I am now locked, I will wait 2 seconds.'%FL.deadLock,
         3:'Exhau1ted my %s sec. timeout.'%FL.timeout}

t0 = time.time()
for i in range(n):  
    acquired, code = FL.acquire_lock()
    message = codes.get(code, "I was not able to set the lock, I encountered the following error (%s)."%code)
    if acquired:
        t0 = time.time()
        print message
        time.sleep(2)
    else:
        print message, "I've been waiting for %s sec."%(time.time()-t0)
    # release lock
    FL.release_lock() 