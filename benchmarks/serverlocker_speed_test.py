import  time

from pylocker import ServerLocker

import bench_stats

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

    bench_stats.print_summary(f'{FL.name} acquire', ats[FL.name], sum(ats[FL.name]))
    bench_stats.print_summary(f'{FL.name} release', rts[FL.name], sum(rts[FL.name]))

server.stop()
client.stop()
