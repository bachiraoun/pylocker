# standard distribution imports
from __future__ import print_function
import os
import sys
import time
import atexit
import signal
import threading
import uuid

# make implementation python3 compatible
try:
    basestring
except:
    basestring=str

## only posix is supports silent rename of a file.
#assert os.name=='posix',Exception("pylocker only defined for posix platforms")
try:
    from psutil import pid_exists
except:
    # for unix systems
    if os.name == 'posix':
        import errno
        def pid_exists(pid):
            if pid < 0:
                return False
            if pid == 0:
                # According to "man 2 kill" PID 0 refers to every process
                # in the process group of the calling process.
                # On certain systems 0 is a valid PID but we have no way
                # to know that in a portable fashion.
                raise ValueError('invalid PID 0')
            try:
                os.kill(pid, 0)
            except OSError as err:
                if err.errno == errno.ESRCH: # ESRCH == No such process
                    return False
                elif err.errno == errno.EPERM: # EPERM clearly means there's a process to deny access to
                    return True
                else:
                    # According to "man 2 kill" possible error values are
                    # (EINVAL, EPERM, ESRCH)
                    raise
            else:
                return True
    else:
        import ctypes
        def pid_exists(pid):
            #PROCESS_QUERY_INFROMATION = 0x1000
            processHandle = ctypes.windll.kernel32.OpenProcess(0x1000, 0,pid)
            if processHandle == 0:
                return False
            else:
                ctypes.windll.kernel32.CloseHandle(processHandle)
            return True


## SET THOSE FLAGS TO TRUE IN ORDER TO DEBUG
VERBOSE     = False
RAISE_ERROR = False

class Locker(object):
    """
    This is the old Locker implemenetation. It's not removed for back compatibility.
    Using ServerLocker and SingleLocker is a much more robust implementation and
    is process, thread and OS safe. ServerLocker will also work between connected
    machines on the network. Locker can be used for general locking purposes and
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
                print("Lock acquired")
                print("In this if statement block I can do whatever I want before releasing the lock")
            else:
                print("Unable to acquire the lock. exit code %s"%code)
                print("keep this block empty as the lock was not acquired")

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
                    print("Lock acquired, in this if statement do whatever you want")
                else:
                    print("Unable to acquire the lock. exit code %s"%code)

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
                    print(fd)
                    fd.write("I have succesfuly acquired the lock !")

            # no need to release anything or to close the file descriptor,
            # with statement takes care of that. let's print fd and verify that.
            print fd
    """
    def __init__(self, filePath, lockPass, mode='ab', lockPath=None, timeout=60, wait=0, deadLock=120):
        # initialize fd
        self.__fd = None
        # process pid
        self.__pid = str(os.getpid())
        # create lock unique id
        self.__uniqueID = str(uuid.uuid1())+'_'+self.__pid
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
        # capture exit signal to release the lock (only in main thread)
        if threading.current_thread().__class__.__name__ == "_MainThread":
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
        assert mode in ('r','rb','r+','rb+','w','wb','w+','wb','wb+','a','ab','a+','ab+'), "mode must be any of 'r','rb','r+','rb+','w','wb','w+','wb','wb+','a','ab','a+','ab+' '%s' is given"%mode
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

    def acquire_lock(self, verbose=VERBOSE, raiseError=RAISE_ERROR):
        """
        Try to acquire the lock.

        :Parameters:
            #. verbose (bool): Whether to be verbose about errors when encountered
            #. raiseError (bool): Whether to raise error exception when encountered

        :Returns:
            #. result (boolean): Whether the lock is succesfully acquired.
            #. code (integer, Exception): Integer code indicating the reason how the
               lock was successfully set or unsuccessfully acquired. When setting the
               lock generates an error, this will be caught and returned in a message
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
                  The error is caught and returned in this Exception. In this case
                  result is False.
        """
        #### ON WINDOWS MUST LOOK INTO THAT IN ORDER TO GET RID OF os.rename
        #### http://code.activestate.com/recipes/577794-win32-named-mutex-class-for-system-wide-mutex/
        # set acquire flag
        code         = 0
        acquired     = False
        t0 = t1      = time.time()
        LP           = self.__lockPass+'\n'
        bytesLP      = LP.encode()
        _lockingText = LP+'%.6f'+('\n%s'%self.__pid)
        _timeout     = self.__timeout
        # set general while loop with timeout condition
        while (t1-t0)<=_timeout:
            # try to set acquired to True by reading an empty lock file
            try:
                while not acquired and (t1-t0)<=_timeout:
                    if os.path.isfile(self.__lockPath):
                        try:
                            with open(self.__lockPath, 'rb') as fd:
                                lock = fd.readlines()
                        except:
                            # for the few time when lockPath got deleted or not available for reading
                            pass
                        else:
                            # lock file is empty or wrong
                            if len(lock) != 3:
                                code     = 0
                                acquired = True
                                break
                            if lock[0] == bytesLP:
                                code     = 1
                                acquired = True
                                break
                            if t1-float(lock[1]) > self.__deadLock: # check dead lock
                                acquired = True
                                code     = 2
                                break
                            if not pid_exists(int(lock[2])): # check for dead process which means lock is dead
                                acquired = True
                                code     = 2
                                break
                        # wait a bit
                        if self.__wait:
                            time.sleep(self.__wait)
                        t1 = time.time()
                    else:
                        acquired = True
                        break
            except Exception as err:
                code     = Exception( "Failed to check the lock (%s)"%(str(err),) )
                acquired = False
                if verbose: print(str(code))
                if raiseError: raise code
            # impossible to acquire because of an error or timeout.
            if not acquired:
                break
            # try to write lock
            try:
                tic = time.time()
                tmpFile = self.__lockPath
                # this is how to avoid multiple file writing os lock
                # must be tried on nt (windows) and tested for solution
                # os.rename on nt will crash if destination exists.
                if os.name=='posix':
                    tmpFile = '%s_%s'%(self.__lockPath,self.__uniqueID)
                with open(tmpFile, 'wb') as fd:
                    fd.write( str(_lockingText%t1).encode() )
                    fd.flush()
                    os.fsync(fd.fileno())
                if os.name=='posix':
                    os.rename(tmpFile,self.__lockPath)
                toc = time.time()
                if toc-tic >1:
                    print("PID '%s' writing '%s' is delayed by os for %s seconds. Lock timeout adjusted. MUST FIND A WAY TO FIX THAT"%(self.__pid,self.__lockPath,str(toc-tic)))
                    _timeout += toc-tic
            except Exception as err:
                code     = Exception("Failed to write the lock (%s)"%(str(err),) )
                acquired = False
                if verbose: print(str(code))
                if raiseError: raise code
                break
            # sleep for double tic-toc or 0.1 ms which ever one is bigger
            s = max([(toc-tic), 0.0001])
            time.sleep(s)
            # check if lock is still acquired by the same lock pass
            try:
                with open(self.__lockPath, 'rb') as fd:
                    lock = fd.readlines()
            except:
                lock = []
            if len(lock) >= 1:
                if lock[0] == bytesLP:
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


    def release_lock(self, verbose=VERBOSE, raiseError=RAISE_ERROR):
        """
        Release the lock when set and close file descriptor if opened.

        :Parameters:
            #. verbose (bool): Whether to be verbose about errors when encountered
            #. raiseError (bool): Whether to raise error exception when encountered

        :Returns:
            #. result (boolean): Whether the lock is succesfully released.
            #. code (integer, Exception): Integer code indicating the reason how the
               lock was successfully or unsuccessfully released. When releasing the
               lock generates an error, this will be caught and returned in a message
               Exception code.

               *  0: Lock is not found, therefore successfully released
               *  1: Lock is found empty, therefore successfully released
               *  2: Lock is found owned by this locker and successfully released
               *  3: Lock is found owned by this locker and successfully released and locked file descriptor was successfully closed
               *  4: Lock is found owned by another locker, this locker has no permission to release it. Therefore unsuccessfully released
               *  Exception: Lock was not successfully released because of an unexpected error.
                  The error is caught and returned in this Exception. In this case
                  result is False.

        """
        if not os.path.isfile(self.__lockPath):
            released = True
            code     = 0
        else:
            try:
                with open(self.__lockPath, 'rb') as fd:
                    lock = fd.readlines()
            except Exception as err:
                code     = Exception( "Unable to read release lock file '%s' (%s)"%(self.__lockPath,str(err)) )
                released = False
                if verbose: print(str(code))
                if raiseError: raise code
            else:
                if not len(lock):
                    code     = 1
                    released = True
                elif lock[0].rstrip() == self.__lockPass.encode():
                    try:
                        with open(self.__lockPath, 'wb') as f:
                            #f.write( ''.encode('utf-8') )
                            f.write( ''.encode() )
                            f.flush()
                            os.fsync(f.fileno())
                    except Exception as err:
                        released = False
                        code     = Exception( "Unable to write release lock file '%s' (%s)"%(self.__lockPath,str(err)) )
                        if verbose: print(str(code))
                        if raiseError: raise code
                    else:
                        released = True
                        code     = 2
                else:
                    code     = 4
                    released = False
        # close file descriptor if lock is released and descriptor is not None
        if released and self.__fd is not None:
            try:
                if not self.__fd.closed:
                    self.__fd.flush()
                    os.fsync(self.__fd.fileno())
                    self.__fd.close()
            except Exception as err:
                code = Exception( "Unable to close file descriptor of locked file '%s' (%s)"%(self.__filePath,str(err)) )
                if verbose: print(str(code))
                if raiseError: raise code
            else:
                code = 3
        # return
        return released, code



    def acquire(self, *args, **kwargs):
        """Alias to acquire_lock"""
        return self.acquire_lock(*args, **kwargs)

    def release(self, *args, **kwargs):
        """Alias to release_lock"""
        return self.release_lock(*args, **kwargs)
