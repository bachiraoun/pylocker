"""
Usage:
======
This example shows how to use pylocker in the most general way. We won't be locking 
a file but aquiring a lock and testing if for doing whatever we want.


.. code-block:: python
    
        import uuid
        from pylocker import Locker
        
        #  create a unique lock pass. This can be any string.
        lpass = str(uuid.uuid1())
        
        # create locker instance.
        FL = Locker(filePath=None, lockPass=lpass)
        
        # try to acquire the lock
        acquired, code = FL.acquire_lock()
        
        # check if acquired.
        if acquired:
            print "Lock acquired"
            print "In this if statement block I can do whatever I want before releasing the lock"
        else:
            print "Unable to acquire the lock. exit code %s"%code
            print "keep this block empty as the lock was not acquired"
        
        # now release the lock.
        FL.release_lock()



The above example can also be done using 'with' statement


.. code-block:: python

        import uuid
        from pylocker import Locker
        
        #  create a unique lock pass. This can be any string.
        lpass = str(uuid.uuid1())
        
        # create locker instance
        FL = Locker(filePath=None, lockPass=lpass)
        
        # acquire the lock
        with FL as r:
            # r is a tuple of three items. the acquired result, the aquiring code and 
            # a file descriptor fd. fd will always be None when filePath is None.  
            # Otherwise fd can be a real opened file descriptor when acquired is 
            # True. In this particular case fd is always None regardless whether 
            # the lock was successfully acquired or not because filePath is None.
            acquired, code, fd  = r
            
             
            # check if acquired.
            if acquired:
                print "Lock acquired, in this if statement do whatever you want"
            else:
                print "Unable to acquire the lock. exit code %s"%code
        
        # no need to release anything because with statement takes care of that.
        

Now let's lock a file using 'with' statement


.. code-block:: python

        import uuid
        from pylocker import Locker
        
        #  create a unique lock pass. This can be any string.
        lpass = str(uuid.uuid1())
        
        # create locker instance.
        FL = Locker(filePath='myfile.txt', lockPass=lpass, mode='w')
        
        # acquire the lock
        with FL as r:
            # get the result
            acquired, code, fd  = r
            
            # check if acquired.
            if fd is not None:
                print fd
                fd.write("I have succesfuly acquired the lock !")
        
        # no need to release anything or to close the file descriptor, 
        # with statement takes care of that. let's print fd and verify that.
        print fd
        
        

Locker main module:
===================
"""

# standard distribution imports
import os
import sys
import time
import atexit
import signal




class Locker(object):
    """
    This is pylocker main class. Locker can be used for general locking purposes and 
    more specifically to lock a file from reading or writing to whoever that doesn't  
    have the lock pass.
    
    :Parameters:
        #. filePath (None, path): The file that needs to be locked. When given and a lock 
           is acquired, the file will be automatically opened for writing or reading 
           depending on the given mode. If None is given, the locker can always be used
           for its general purpose as shown in the examples.
        #. lockPass (string): The locking pass.
        #. mode (string): This is file opening mode and it can be any of 
           'r','r+','w','w+','a','a+'. If filePath is None, this argument will not be
           discarded.
        #. lockPath (None, path): The locking file path. If None is given the locking file
           will be automatically created to '.lock' in the filePath directory. If 
           filePath is None, '.lock' will be created in the current working directory.
        #. timeout (number): The maximum delay or time allowed to successfully set the 
           lock. When timeout is exhausted before successfully setting the lock, 
           the lock ends up not acquired.
        #. wait (number): The time delay between each attempt to lock. By default it's
           set to 0 to keeping the aquiring mechanism trying to acquire the lock without 
           losing any time waiting. Setting wait to a higher value suchs as 0.05 seconds
           or higher can be very useful in special cases when many processes are trying 
           to acquire the lock and one of them needs to hold it and release it at a high
           frequency or rate.
        #. deadLock (number): The time delay judging if the lock was left out mistakenly 
           after a system crash or other unexpected reasons. Normally Locker is stable 
           and takes care of not leaving any locking file hanging even it crashes or it
           is forced to stop by a user signal.
    """
    def __init__(self, filePath, lockPass, mode='a', lockPath=None, timeout=10, wait=0, deadLock=20):
        # initialize fd
        self.__fd = None
        # set file path
        self.set_file_path(filePath)
        # set mode
        self.set_mode(mode)
        # set lock pass
        self.set_lock_pass(lockPass)
        # set lockPath
        self.set_lock_path(lockPath)
        # set wait
        self.set_wait(wait)
        # set timeout
        self.set_timeout(timeout)
        # set deadLock
        self.set_dead_lock(deadLock)
        # register to atexit to release the lock
        atexit.register(self.release_lock) 
        # capture exit signal to release the lock
        signal.signal(signal.SIGINT, self.__signal_handler) 
    
    def __enter__(self):
        acquired, code = self.acquire_lock()
        if acquired and self.__filePath is not None:
            self.__fd = open(self.__filePath, self.__mode)
        else:
            self.__fd = None
        return acquired, code, self.__fd 
    
    def __exit__(self, type, value, traceback):
        self.release_lock()
    
    def __del__(self):
       self.release_lock()
    
    def __signal_handler(self, signal, frame):
        self.release_lock()
        sys.exit(0)
       
    @property
    def filePath(self):
        """locker file path"""
        return self.__filePath
    
    @property
    def lockPass(self):
        """locker pass"""
        return self.__lockPass
    
    @property
    def lockPath(self):
        """locker lock path"""
        return self.__lockPath
    
    @property
    def timeout(self):
        """locker timeout in seconds"""
        return self.__timeout
    
    @property
    def wait(self):
        """locker wait in seconds"""
        return self.__wait
        
    @property
    def deadLock(self):
        """locker deadLock in seconds"""
        return self.__deadLock
        
    def set_mode(self, mode):   
        """
        Set file opening mode.
        
        :Parameters:
            #. mode (string): This is file opening mode and it can be any of 
               r , r+ , w , w+ , a , a+ . If filePath is None, this argument 
               will be discarded.
           
               *  r : Open text file for reading.  The stream is positioned at the
                  beginning of the file.
                
               *  r+ : Open for reading and writing.  The stream is positioned at the
                  beginning of the file.
                
               *  w : Truncate file to zero length or create text file for writing.
                  The stream is positioned at the beginning of the file.
                
               *  w+ : Open for reading and writing.  The file is created if it does not
                  exist, otherwise it is truncated.  The stream is positioned at
                  the beginning of the file.
                
               *  a : Open for writing.  The file is created if it does not exist.  The
                  stream is positioned at the end of the file.  Subsequent writes
                  to the file will always end up at the then current end of file,
                  irrespective of any intervening fseek(3) or similar.
                
               *  a+ : Open for reading and writing.  The file is created if it does not
                  exist. The stream is positioned at the end of the file.  Subsequent
                  writes to the file will always end up at the then current
                  end of file, irrespective of any intervening fseek(3) or similar.
        """
        assert mode in ('r','r+','w','w+','a','a+'), "mode must be any of 'r','r+','w','w+','a','a+', '%s' is given"%mode
        self.__mode = mode
                 
    def set_file_path(self, filePath):
        """
        Set the file path that needs to be locked.
        
        :Parameters:
            #. filePath (None, path): The file that needs to be locked. When given and a lock 
               is acquired, the file will be automatically opened for writing or reading 
               depending on the given mode. If None is given, the locker can always be used
               for its general purpose as shown in the examples.
        """
        if filePath is not None:
            assert isinstance(filePath, basestring), "filePath must be None or string"
            filePath = str(filePath)
        self.__filePath = filePath
        
    def set_lock_pass(self, lockPass):
        """
        Set the locking pass
        
        :Parameters:
            #. lockPass (string): The locking pass.
        """
        assert isinstance(lockPass, basestring), "lockPass must be string"
        lockPass = str(lockPass)
        assert '\n' not in lockPass, "lockPass must be not contain a new line"
        self.__lockPass = lockPass
        
    def set_lock_path(self, lockPath):
        """
        Set the managing lock file path.
        
        :Parameters:
            #. lockPath (None, path): The locking file path. If None is given the locking file
               will be automatically created to '.lock' in the filePath directory. If 
               filePath is None, '.lock' will be created in the current working directory.
        """
        if lockPath is not None:
            assert isinstance(lockPath, basestring), "lockPath must be None or string"
            lockPath = str(lockPath)
        self.__lockPath = lockPath
        if self.__lockPath is None:
            if self.__filePath is None:
                self.__lockPath = os.path.join(os.getcwd(), ".lock")
            else:
                self.__lockPath = os.path.join( os.path.dirname(self.__filePath), '.lock')
                
    def set_timeout(self, timeout):
        """
        set the timeout limit.
        
        :Parameters:
            #. timeout (number): The maximum delay or time allowed to successfully set the 
               lock. When timeout is exhausted before successfully setting the lock, 
               the lock ends up not acquired.
        """
        try:
            timeout = float(timeout)
            assert timeout>=0
            assert timeout>=self.__wait
        except:
            raise Exception('timeout must be a positive number bigger than wait')
        self.__timeout  = timeout
        
    def set_wait(self, wait):
        """
        set the waiting time.
        
        :Parameters:
            #. wait (number): The time delay between each attempt to lock. By default it's
               set to 0 to keeping the aquiring mechanism trying to acquire the lock without 
               losing any time waiting. Setting wait to a higher value suchs as 0.05 seconds
               or higher can be very useful in special cases when many processes are trying 
               to acquire the lock and one of them needs to hold it a release it at a higher
               frequency or rate.
        """
        try:
            wait = float(wait)
            assert wait>=0
        except:
            raise Exception('wait must be a positive number')
        self.__wait = wait
        
    def set_dead_lock(self, deadLock):
        """
        Set the dead lock time.
        
        :Parameters:
            #. deadLock (number): The time delay judging if the lock was left out mistakenly 
               after a system crash or other unexpected reasons. Normally Locker is stable 
               and takes care of not leaving any locking file hanging even it crashes or it
               is forced to stop by a user signal.
        """
        try:
            deadLock = float(deadLock)
            assert deadLock>=0
        except:
            raise Exception('deadLock must be a positive number')
        self.__deadLock = deadLock
    
    def acquire_lock(self):
        """
        Try to acquire the lock.
        
        :Parameters:
            #. result (boolean): Whether the lock is succesfully acquired.
            #. code (integer, Exception): Integer code indicating the reason how the
               lock was successfully set or unsuccessfully acquired. When setting the 
               lock generates an error, this will be catched and returned in a message
               Exception code.
               
               *  0: Lock is successfully set for normal reasons, In this case result 
                  is True.
               *  1: Lock was already set, no need to set it again. In this case result  
                  is True.
               *  2: Old and forgotten lock is found and removed. New lock is 
                  successfully set, In this case result is True.
               *  3: Lock was not successfully set before timeout. In this case result 
                  is False.
               *  Exception: Lock was not successfully set because of an unexpected error.
                  The error is catched and returned in this Exception. In this case 
                  result is False.
        """
        # set acquire flag
        code     = 0
        acquired = False
        t0 = t1  = time.time()
        LP       = self.__lockPass+'\n'
        # set general while loop with timeout condition
        while (t1-t0)<=self.__timeout:
            # try to set acquired to True by reading an empty lock file
            try:
                while not acquired and (t1-t0)<=self.__timeout:
                    if os.path.isfile(self.__lockPath):
                        lock = open(self.__lockPath).readlines()
                        # lock file is empty
                        if not len(lock):
                            acquired = True
                            break
                        # if it is already locked
                        if lock[0] == LP:
                            code     = 1
                            acquired = True
                            break
                        if t1-float(lock[1]) > self.__deadLock:
                            acquired = True
                            code     = 2
                            break
                        #print 'locked ',(t1-t0), t0, t1
                        # wait a bit
                        if self.__wait:
                            time.sleep(self.__wait)
                        t1  = time.time()
                    else:
                        acquired = True
                        break            
            except Exception as code:
                acquired = False
            # impossible to acquire because of an error or timeout.
            if not acquired:
                break
            # try to write lock
            try:    
                tic = time.time()
                with open(self.__lockPath, 'w') as f:
                    f.write( LP+'%.6f'%t1 )
                    f.flush()
                    os.fsync(f.fileno())
                toc = time.time()
            except Exception as code:
                acquired = False
                break
            # sleep for double tic-toc or 0.1 ms which ever one is higher
            s = max([2*(toc-tic), 0.0001])
            time.sleep(s)
            # check if lock is still acquired by the same lock pass
            lock = open(self.__lockPath,'r').readlines()
            if len(lock) >= 1:
                if lock[0] == LP:
                    acquired = True
                    break
                else:
                    acquired = False
                    t1  = time.time()
                    continue
            else:
                acquired = False
                t1 = time.time()
                continue 
        # return whether it is acquired or not
        if not acquired and not code:
            code = 3
        return acquired, code
    
    def release_lock(self):
        """
        Release the lock when set and close file descriptor if opened.
        """
        if self.__fd is not None:
            self.__fd.close()
        if not os.path.isfile(self.__lockPath):
            return
        lock = open(self.__lockPath,'r').readlines()
        if not len(lock):
            return
        if lock[0].rstrip() == self.__lockPass:
            with open(self.__lockPath, 'w') as f:
                f.write( '' )
                f.flush()
                os.fsync(f.fileno())  
            return  
        
    def acquire(self):
        """Alias to acquire_lock"""
        return self.acquire_lock()
    
    def release(self):
        """Alias to release_lock"""
        return self.release_lock()
    
       
       
       
       
       
       
       
       
       