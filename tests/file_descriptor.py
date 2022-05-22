import uuid, time, os

from pylocker import Locker
         
FL = Locker(filePath='test.txt', mode='a', lockPass=str(uuid.uuid4()), lockPath='lock.txt')

# acquire the lock
with FL as r:
    # get the result
    acquired, code, fd  = r
    
    print('I am the file descriptor inside with statement: ',fd)
    # check if aquired.
    if fd is not None:
        fd.write("I have succesfuly aquired the lock !")

# no need to release anything or to close the file descriptor, 
# with statement takes care of that. let's print fd and verify that
print('I am the file descriptor outside with statement: ',fd)