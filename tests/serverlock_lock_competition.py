"""
Launch many consoles and run the following scipt and verify how lock can be aquired
acquisition works.
"""
import os,time,datetime

from pylocker import ServerLocker

L = ServerLocker(name='locker_%i'%os.getpid(),password='no-password')

n = 10000


for i in range(n):
    acquired, lockId = L.acquire_lock(path='something', timeout=5)
    if acquired:
        print("%s - %s: lock '%s' is acquired"%(datetime.datetime.now().strftime("%Y-%m-%d|%H:%M:%S"), L.name, lockId))
    else:
        print('%s - %s: UNABLE TO ACQUIRE THE LOCK (%s)'%(datetime.datetime.now().strftime("%Y-%m-%d|%H:%M:%S"), L.name,lockId))
    time.sleep(2)
    # release lock
    if acquired:
        released, message = L.release_lock(lockId=lockId)
        if not released:
            print('%s - %s: UNABLE TO RELEASE THE LOCK (%s)'%(datetime.datetime.now().strftime("%Y-%m-%d|%H:%M:%S"), L.name,message))
