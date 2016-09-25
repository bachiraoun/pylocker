"""
Usage:
======


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
           is aquired, the file will be automatically opened for writing or reading 
           depending on the given mode. If None is given, the locker can always be used
           for its general purpose as shown in the examples.
        #. lockPass (string): The locking pass.
        #. mode (string): This is file opening mode and it can be any of 
           'r','r+','w','w+','a','a+'. If filePath is None, this argument will not be
           discarded.
        #. lockPath (None, path): The locking file path. If None is given the locking file
           will be automatically created to '.lock' in the filePath directory. If 
           filePath is None, '.lock' will be created in the current working directory.
        #. timeout (number): The maximum delay or time allowed to successfuly lock. When
           passed, lock was not successfuly set, the lock ends not acquired.
        #. wait (number): The time delay between each attempt to lock
        #. deadLock (number): The time delay judging if the lock was left out mistakenly 
           after a system crash or other unexpected reasons. Normally Locker is stable 
           to not leaving any locking file hanging and has the necessary mechanisms to
           clean itself after any crash.
    """
    def __init__(self, filePath, lockPass, mode='a', lockPath=None, timeout=10, wait=.001, deadLock=20):
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
    
    def set_mode(self, mode):   
        """
        Set file opening mode.
        
        :Parameters:
            #. mode (string): This is file opening mode and it can be any of 
               'r','r+','w','w+','a','a+'. If filePath is None, this argument 
               will not be discarded.
           
               #. r : Open text file for reading.  The stream is positioned at the
                  beginning of the file.
                
               #. r+ : Open for reading and writing.  The stream is positioned at the
                  beginning of the file.
                
               #. w : Truncate file to zero length or create text file for writing.
                  The stream is positioned at the beginning of the file.
                
               #. w+ : Open for reading and writing.  The file is created if it does not
                  exist, otherwise it is truncated.  The stream is positioned at
                  the beginning of the file.
                
               #. a : Open for writing.  The file is created if it does not exist.  The
                  stream is positioned at the end of the file.  Subsequent writes
                  to the file will always end up at the then current end of file,
                  irrespective of any intervening fseek(3) or similar.
                
               #. a+ : Open for reading and writing.  The file is created if it does not
                  exist. The stream is positioned at the end of the file.  Subsequent
                  quent writes to the file will always end up at the then current
                  end of file, irrespective of any intervening fseek(3) or similar.
        """
        assert mode in ('r','r+','w','w+','a','a+'), "mode must be any of 'r','r+','w','w+','a','a+', '%s' is given"%mode
        self.__mode = mode
                 
    def set_file_path(self, filePath):
        """
        Set the file path that needs to be locked.
        
        :Parameters:
            #. filePath (None, path): The file that needs to be locked. When given and a lock 
               is aquired, the file will be automatically opened for writing or reading 
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
            #. timeout (number): The maximum delay or time allowed to successfuly lock. When
               passed, lock was not successfuly set, the lock ends not acquired.
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
            #. wait (number): The time delay between each attempt to lock
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
               to not leaving any locking file hanging and has the necessary mechanisms to
               clean itself after any crash.
        """
        try:
            deadLock = float(deadLock)
            assert deadLock>=0
        except:
            raise Exception('deadLock must be a positive number')
        self.__deadLock = deadLock
    
    def __signal_handler(self, signal, frame):
        self.release_lock()
        sys.exit(0)
    
    def acquire_lock(self):
        """
        Try to acquire the lock.
        
        :Parameters:
            #. result (boolean): Whether the lock is succesfully set.
            #. code (integer, Exception): Integer code indicating reason reason why lock was
               not successfuly set. When setting the lock generates an error, this will
               be catched and returned in a message.
               
               #. 0: lock is successfuly set for normal reasons, In this case result 
                  is True.
               #. 1: lock was already set, no need to reset it. In this case result is 
                  True.
               #. 2: Old and forgotten is removed and new lock is successfuly set, In 
                  this case result is True.
               #. 3: lock was not set before timeout. In this case result is False.
               #. Exception: any catched error. In this case result is False.
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
        Release the lock when set.
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
    
    def __enter__(self):
        acquired, code = self.acquire_lock()
        if acquired and self.__filePath is not None:
            self.__fd = open(self.__filePath, self.__mode)
        else:
            self.__fd = None
        return self.__fd, code 
    
    def __exit__(self, type, value, traceback):
        self.release_lock()
    
    def __del__(self):
       self.release_lock()
       
       
       
       
       
       
       
       
       