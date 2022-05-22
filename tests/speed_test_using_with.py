import uuid, time, os

from pylocker import Locker


FL = Locker(filePath='test.txt', mode='a', lockPass=str(uuid.uuid4()), lockPath='lock.txt')

n   = 1000
ats = []
rts = []
for i in range(n):
    t0 = time.time()
    with FL as r:
        acquired, code, fd = r
        if not acquired:
            print(codes.get(code, "I was not able to set the lock, my code is '%s'."%code) )
    t1 = time.time()
    ats.append( t1-t0 )

    # release lock
    t2 = time.time()
    released, code = FL.release_lock(raiseError=True)
    rts.append( t2-t1 )

print("acquiring lock mean time for %i times: "%n, float(sum(ats)) / len(ats) )
print("releasing lock mean time for %i times: "%n, float(sum(rts)) / len(rts) )
