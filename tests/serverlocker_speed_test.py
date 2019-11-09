import  time

from pylocker import ServerLocker

server = ServerLocker(name='server',password='no-password')
client = ServerLocker(name='client',password='no-password')

n   = 1000
ats = {'server':[], 'client':[]}
rts = {'server':[], 'client':[]}

for FL in [server,client]:
    for i in range(n):
        t0 = time.time()
        acquired, lockId = FL.acquire_lock(path='something')
        if not acquired:
            print( lockId )
        t1 = time.time()
        ats[FL.name].append( t1-t0 )
        # release lock
        t2 = time.time()
        released, code = FL.release_lock(lockId=lockId)
        rts[FL.name].append( t2-t1 )

    print("%s acquiring lock mean time for %i times: "%(FL.name,n), float(sum(ats[FL.name])) / len(ats[FL.name]) )
    print("%s releasing lock mean time for %i times: "%(FL.name,n), float(sum(rts[FL.name])) / len(rts[FL.name]) )

server.stop()
client.stop()
