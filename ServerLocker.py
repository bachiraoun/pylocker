# -*- coding: utf-8 -*-
# standard libraries imports
from __future__ import print_function
import os,sys,time,atexit,signal,uuid,traceback
import socket, threading
from multiprocessing.connection import Listener, Client


IS2 = True
if sys.version_info.major == 3:
    assert sys.version_info.major != 2, "Only python 2 or 3 are supported. %s is used instead"%(sys.version_info.major, )
    IS2 = False

## TYPES ##
if IS2:
    # THIS IS PYTHON 2
    str        = str
    unicode    = unicode
    bytes      = str
    long       = long
    basestring = basestring
    xrange     = xrange
    range      = range
    maxint     = sys.maxint
else:
    # THIS IS PYTHON 3
    str        = str
    long       = int
    unicode    = str
    bytes      = bytes
    basestring = (str,bytes)
    xrange     = range
    range      = lambda *args: list( xrange(*args) )
    maxint     = sys.maxsize




def _full_stack():
    try:
        exc = sys.exc_info()[0]
        stack = traceback.extract_stack()[:-1]  # last one would be _full_stack()
        if not exc is None:  # i.e. if an exception is present
            del stack[-1]    # remove call of _full_stack, the printed exception
                             # will contain the caught exception caller instead
        trc = 'Traceback (most recent call last):\n'
        stackstr = trc + ''.join(traceback.format_list(stack))
        if not exc is None:
             stackstr += '  ' + traceback.format_exc().lstrip(trc)
    except Exception as err:
        stackstr  = 'Unable to pull full stack (%s)'%str(err)
        try:
            _stack   = traceback.extract_stack()[:-1]
            stackstr = "%s\n%s"%(stackstr, ''.join(traceback.format_list(_stack)))
        except:
            pass
    # return
    return stackstr

class _LockerThread(threading.Thread):
    def __init__(self, locker, target, args=(), kwargs={}, name=None, daemon=True):
        threading.Thread.__init__(self, group=None, target=None, name=name)
        # set executor
        self.locker_  = locker
        self.running_ = False
        self.started_ = False
        self.failed_  = False
        self.result_  = None
        self.error_   = None
        self.stack_   = None
        self.stop_    = False
        self.__target = target
        self.__args   = args
        self.__kwargs = kwargs
        self.setDaemon(daemon)

    def run(self):
        self.started_ = True
        self.running_ = True
        try:
            self.result_ = self.__target(*self.__args, **self.__kwargs)
        except Exception as err:
            self.failed_ = True
            self.error_  = err
            self.result_ = None
            try:
                self.stack_ = _full_stack()
            except:
                self.stack_ = None
            self.locker_._error(str(self.error_), stack=self.stack_)
        self.running_ = False


def _reconnect_server(method):
    """Internally used wrapper function"""
    def wrapper(self, *args, **kwargs):
        with self._reconnectCounterLock:
            self._reconnectCounter += 1
        # execute method
        try:
            result = method(self, *args,**kwargs)
        except Exception as err:
            result = None
            self._error(str(err), stack=_full_stack())
        with self._reconnectCounterLock:
            self._reconnectCounter -= 1
        # stop server
        self._stop_server(reconnect=True)
        return result
    return wrapper

def _reconnect_client(method):
    """Internally used wrapper function"""
    def wrapper(self, *args, **kwargs):
        with self._reconnectCounterLock:
            self._reconnectCounter += 1
        # execute method
        try:
            result = method(self, *args,**kwargs)
        except Exception as err:
            result = None
            self._error(str(err), stack=_full_stack())
        with self._reconnectCounterLock:
            self._reconnectCounter -= 1
        # stop client
        self._stop_client(reconnect=True)
        return result
    return wrapper

def _to_bytes(input, encode='utf-8', errors='ignore'):
    if not isinstance(input, bytes):
        input = input.encode(encode, errors=errors)
    return input

def _to_unicode(input, decode='utf-8', errors='ignore'):
    if not isinstance(input, unicode):
        input = input.decode(decode, errors=errors)
    return input


class ServerLocker(object):
    """
    Locker implementation that can be used to orchestrate locking and
    releasing string entities between threads and processes.
    ServerLocker is primarily implemented to distribute permissions
    between threads and processes to read and write system files.
    Once instanciated, if autoconnect is True, it will connect to the
    serving ServerLocker instance if existing otherwise if allowServing is
    True it will start serving itself and any other ServerLocker that is
    trying to connect. A serving locker will own and continuously update
    its fingerprint in 'serverFile' flat file. This file will contain
    address and port of the serving locker but not its password. Any
    newly instaciated locker can automatically connect to the serving locker
    if it has access to read 'serverFile' serving locker fingerprint along
    with the correct password. Otherwise remote lockers can connect to the
    serving locker using the ip address and the password.

    ServerLocker is serializable and hence pickle safe. But once loaded,
    user is responsible to call start upon locker to connect it to an existing
    serving locker or to have it serving if no other locker is serving.


    :Parameters:
        #. password (string): password to serve or to connect to an existing
           serving locker
        #. name (None, string): user defined name
        #. serverFile (boolean, string): If True it will be set to
           '.pylocker.serverlocker' in user's home directory. If False, this
           instance will never serve. If string is given, it's the path to the
           serving locker file if existing. When given whether as a string
           or as True, and if this instance is allowed to serve then whenever
           'start' is called, this instance will try to become the serving
           locker unless another instance is serving already then it will
           try to connect.
        #. defaultTimeout (integer): default timeout value to acquire the lock.
           This value can be changed at any time using 'set_default_timeout'
            method
        #. maxLockTime (integer): maximum allowed time for a lock to be
           acquired. This value will be used by serving lockers only. If
           this is too short, serving locker can update this value using
           'set_maximum_lock_time' method
        #. port (int): server port number. If this port is not available an
           active search for an available port will be made
        #. allowServing (boolean): whether to allow this instance to serve
           if it has the chance to serve
        #. autoconnect (boolean): whether to try to connect upon initialization
        #. reconnect (boolean): whether to reconnect if connection drops.
           This is only safe for clients. NOT IMPLEMENTED AT THIS POINT
        #. connectTimeout (integer): timeout limit for connection to create
           successfully
        #. blocking (boolean): Whether to block execution upon connecting. This
           is needed if the instance is launched as a seperate service in a
           seperate process
        #. debugMode (boolean): launch locker in debug mode. debugMode can
           be turned on an off at anytime


    .. code-block:: python

            from pylocker import ServerLocker

            # create locker instance.
            L = ServerLocker(password='server_password')

            # try to acquire the lock a single file path
            acquired, lockId = L.acquire_lock('my_path')

            # check if acquired.
            if acquired:
                print("Lock acquired for 'my_path'")
                print("In this if statement block I can safely do whatever I want with 'my_path' before releasing the lock")
            else:
                print("Unable to acquire the lock. exit code %s"%lockId)
                print("keep this block empty as the lock was not acquired")

            # now release the lock.
            L.release_lock(lockId)

            # try to acquire the lock multiple files
            acquired, lockId = L.acquire_lock(('path_to_file1', 'path_to_file2', 'path_to_directory'))

            # check if acquired.
            if acquired:
                print("Lock acquired for all of 'path_to_file1', 'path_to_file2' and  'path_to_directory'")
            else:
                print("Unable to acquire the lock. exit code %s"%lockId)
                print("keep this block empty as the lock was not acquired")

            # now release the lock.
            L.release_lock(lockId)

    """
    def __init__(self, password, name=None, serverFile=True,
                       defaultTimeout=20, maxLockTime=120, port=3000,
                       allowServing=True, autoconnect=True, reconnect=False,
                       connectTimeout=20, logger=False,
                       blocking=False, debugMode=False):
        # set debugMode
        self.debugMode = debugMode
        # autoconnect
        assert isinstance(autoconnect, bool), "locker autoconnect must be boolean"
        # create unique name
        self.__uniqueName = _to_unicode( str(uuid.uuid1()) )
        if name is None:
            name = self.__uniqueName
        else:
            assert isinstance(name, basestring), "locker server name must be None or a string"
            name = _to_unicode( name )
        assert ':' not in name, "':' not allowed in ServerLocker name"
        self.__name = name
        # check blocking flag
        assert isinstance(blocking, bool), "locker blocking must be boolean"
        self.__blocking = blocking
        assert isinstance(password, basestring), "locker password must be a string"
        if not isinstance(password, bytes):
            password = _to_bytes(password)
        self.__password = password
        # get pid
        try:
            self.__pid = os.getpid()
        except:
            self.__pid = 0
        # set maxLockTime
        self.set_maximum_lock_time(maxLockTime)
        # set default timeout
        self.set_default_timeout(defaultTimeout)
        # set ip address
        self.__address = self.__get_ip_address()
        # set reconnect
        if not isinstance(reconnect, bool):
            assert isinstance(reconnect, int), "reconnect must be boolean or integer"
            assert reconnect>=0, "reconnect must be >=0"
        elif reconnect is False:
            reconnect = 0
        self.__reconnect = False #reconnect # NOT IMPLEMENTED AS OF NOW
        assert isinstance(connectTimeout, int), "connectTimeout must be integer"
        assert connectTimeout>0, "connectTimeout must ber >0"
        self.__connectTimeout = connectTimeout
        assert isinstance(port, int), "port must be integer"
        assert 1<=port<=65535, "port must be 1<=port<=65535"
        self.__port = port
        assert isinstance(allowServing, bool), "allowServing must be boolean"
        self.__allowServing = allowServing
        # set all attributes by calling reset
        self.reset(raiseError=True)
        # set server file
        self.set_server_file(serverFile)
        # register to atexit
        atexit.register( self._on_atexit )
        # start serving loop if this instance can serve
        if autoconnect:
            self.__serve_or_connect()
            if not (self.isServer or self.isClient):
                self._warn("locker is not a serving nor connected as client yet autoconnect is set to True")


    ############################# make pickle safe #############################
    def __getstate__(self):
        # get current state
        current = {}
        current.update( self.__dict__ )
        _serverFile = self.__dict__.pop('_ServerLocker__serverFile')
        # reset to remove current state
        self.reset()
        state = {}
        state.update( self.__dict__ )
        state['_ServerLocker__serverFile'] = _serverFile
        for k in state:
            if k.endswith('Lock'):
                state[k] = None
            if k.endswith('Event'):
                state[k] = None
        # reset current state
        self.__dict__ = current
        return state

    def __setstate__(self, state):
        self.__dict__ = state
        _serverFile   = self.__dict__.pop('_ServerLocker__serverFile')
        # reset locker and make sure all locks and events are created
        self.reset()
        # reset serverFile
        self.__dict__['_ServerLocker__serverFile'] = _serverFile


    ############################# debugging methods ############################
    def _critical(self, message, force=False, stack=None):
        if self.__debugMode:
            print('%s - CRITICAL - %s'%(self.__class__.__name__,message))
            if stack is not None:
                print(stack)
        return message

    def _error(self, message, force=False, stack=None):
        if self.__debugMode:
            print('%s - ERROR - %s'%(self.__class__.__name__,message))
            if stack is not None:
                print(stack)
        return message

    def _warn(self, message, force=False, stack=None):
        if self.__debugMode:
            print('%s - WARNING - %s'%(self.__class__.__name__,message))
            if stack is not None:
                print(stack)
        return message

    def _info(self, message, force=False, stack=None):
        if self.__debugMode:
            print('%s - INFO - %s'%(self.__class__.__name__,message))
            if stack is not None:
                print(stack)
        return message


    ################# expose some properties in a private way ##################
    @property
    def _clientsLUT(self):
        return self.__clientsLUT

    @property
    def _pathsLUT(self):
        return self.__pathsLUT

    @property
    def _clientsQueue(self):
        return self.__clientsQueue

    @property
    def _ownRequests(self):
        return self.__ownRequests

    @property
    def _ownAcquired(self):
        return self.__ownAcquired

    @property
    def _publications(self):
        return self.__publications

    ########################## close and stop methods ##########################
    def _on_atexit(self, *args, **kwargs):
        try:
            uniqueName, timestamp, address, port, pid = self.get_running_server_fingerprint(serverFile=self.__serverFile, raiseNotFound=False, raiseError=False)
            if uniqueName == self.__uniqueName:
                with self.__serverFileLock:
                    open(self.__serverFile, 'w').close()
        except:
            pass
        self.stop()

    def _stop_server(self, reconnect=False):
        try:
            if not self._stopServing:
                self._stopServing = True
                uniqueName, timestamp, address, port, pid = self.get_running_server_fingerprint(serverFile=self.__serverFile, raiseNotFound=False, raiseError=False)
                if uniqueName == self.__uniqueName:
                    with self.__serverFileLock:
                        open(self.__serverFile, 'w').close()
        except:
            pass
        try:
            self._stopServing = True
            if self.__server is not None:
                self.__server.close()
        except Exception as err:
            self._warn('Unable to stop locker server (%s)'%err)
        finally:
            self.__server = None
        # kill all clients connection
        try:
            with self.__clientsLUTLock:
                if self.__clientsLUT is not None:
                    for cname in list(self.__clientsLUT):
                        try:
                            self.__clientsLUT.pop(cname)['connection'].close()
                        except Exception as err:
                            self._warn("Unable to close clocker client '%s' connection (%s)"%(cname, err))
                        else:
                            self._warn("locker closed connection to '%s'"%cname)
                self.__clientsLUT = None
        except Exception as err:
            self._warn("Unable to close locker clients connection (%s)"%err)
        # clear dead lock event
        self.__deadLockEvent.set()
        self.__deadLockEvent.clear()
        # clear clients queue event
        self.__clientsQueueEvent.set()
        self.__clientsQueueEvent.clear()
        # reconnect
        if self._reconnectCounter == 0 and reconnect and not self._killSignal:
            if self.__reconnect is True or self.__reconnect>0:
                if self.__reconnect is not True:
                    self.__reconnect -= 1
                self._warn('locker server stopped! Trying to reconnect')
                self.__serve_or_connect()
            else:
                raise Exception('locker server stopped! Aborting')

    def _stop_client(self, reconnect=False):
        try:
            self.__connection.close()
        except:
            pass
        finally:
            self.__connection = None
        self.__serverUniqueName = None
        self.__serverName       = None
        self.__serverAddress    = None
        self.__serverPort       = None
        # reconnect
        if self._reconnectCounter == 0 and reconnect and not self._killSignal:
            if self.__reconnect is True or self.__reconnect>0:
                if self.__reconnect is not True:
                    self.__reconnect -= 1
                self._warn('locker client connection stopped! Trying to reconnect')
                self.__serve_or_connect()
            else:
                raise Exception('locker client connection stopped! Aborting')




    ######################### server and client methods ########################
    def __process_server_response(self, received):
        if isinstance(received, dict):
            received = [received]
        assert isinstance(received, (list,set,tuple)), "received data must be a list"
        assert len(received), "received list is emtpy"
        for response in received:
            assert isinstance(response, dict), "received list items must be dictionaries"
            assert 'action' in response, "received dict items must have 'action' key"
            assert 'request_unique_id' in response, "received dict items must have 'request_unique_id' key"
            if response['action'] == 'publish':
                assert 'message' in response, "received action '%s' must have 'message' key"%response['action']
                message = response['message']
                assert isinstance(message, basestring), "received action '%s' message must be a string"%response['action']
            else:
                assert response['action'] in ('acquired','released','exceeded_maximum_lock_time',), "received dict items dict 'action' key value is not recognized"
                assert 'path' in response, "received action '%s' must have 'path' key"%response['action']
                path = response['path']
                if isinstance(path, basestring):
                    path = [path]
                assert isinstance(path, (list,set,tuple)), "received dict 'path' value must be a list"
                assert len(path), "received dict 'path' value list must be not empty"
                assert all([isinstance(p,basestring) for p in path]), "received dict 'path' list items must be all strings. %s is given"%path
        # loop list of received responses
        with self.__ownRequestsLock:
            utctimestamp = time.time()
            for response in received:
                _action = response['action']
                _ruid   = response['request_unique_id']
                if _action == 'acquired':
                    _req = self.__ownRequests.pop(_ruid, {'acquired_event':None})
                    if _req['acquired_event'] is not None:
                        _req['acquired_event'].set()
                    _req['acquired_utctime'] = utctimestamp
                    self.__ownAcquired[_ruid] = _req
                elif _action == 'released':
                    self._warn("action 'released' received at client !!! This is meaningless")
                elif _action == 'exceeded_maximum_lock_time':
                    if self.__ownAcquired.pop(_ruid, None) is not None:
                        _cname  = response['client_name']
                        _cuname = response['client_unique_name']
                        self._warn("Lock '%s' requested by client %s:%s for all '%s' is released by server because maximum lock time is exceed and the lock is required by another client"%(_ruid,_cname,_cuname,response['path']))
                elif _action == 'publish':
                    self.__add_publication(request=response)
                else:
                    raise Exception("Unkown 'action' '%s'. PLEASE REPORT"%(_action,))


    def __process_client_request(self, request, connection):
        ruid   = request['request_unique_id']
        cname  = request['client_name']
        cuname = request['client_unique_name']
        action = request['action']
        if action =='release':
            with self.__pathsLUTLock:
                path = request['path']
                self._warn('releasing request: %s'%request)
                for p in path:
                    if p not in self.__pathsLUT:
                        self._warn("requesting to release unlocked path '%s'"%(path,))
                    elif self.__pathsLUT[p]['client_unique_name'] == cuname and self.__pathsLUT[p]['request_unique_id'] == ruid:
                        self.__pathsLUT.pop(p,None)
                    else:
                        self._warn("requesting to release path '%s' locked by different locker"%(path,))
        elif action=='acquire':
            with self.__clientsQueueLock:
                req = {'connection':connection}
                req.update(request)
                self.__clientsQueue.append(req)
        elif action=='publish':
            with self.__clientsLUTLock:
                receivers = request['receivers']
                if receivers is None:
                    receivers = [self.__uniqueName]+list(self.__clientsLUT)
                for r in receivers:
                    if r == request['client_unique_name']:
                        continue
                    if r == self.__uniqueName:
                        self.__add_publication(request=request)
                    else:
                        conn = self.__clientsLUT[r]['connection']
                        conn.send(request)
        else:
            raise Exception("Unkown request id '%s' action '%s' from client '%s' (%s)"%(ruid, action,cname,cuname))
        # set the queue event to wake up the queue monitor
        with self.__clientsQueueLock:
            self.__clientsQueueEvent.set()
            self.__clientsQueueEvent.clear()
        with self.__pathsLUTLock:
            self.__deadLockEvent.set()
            self.__deadLockEvent.clear()

    def __serve_client(self, connection, clientName, clientUniqueName):
        self._warn("Client '%s:%s' connected to server"%(clientName,clientUniqueName))
        while not self._stopServing and not self._killSignal:
            lastTimeout = None
            try:
                received = connection.recv()
            ## MUST SPLIT BETWEEN MULTIPLE ERROR TYPES
            except socket.timeout as err:
                if lastTimeout is None:
                    lastTimeout = time.time()
                elif time.time()-lastTimeout < 1:
                    self._critical("Connection to locker client '%s:%s' has encountered unsuspected successive timeouts within 1 second."%(clientName,clientUniqueName))
                    break
                self._error("Connection timeout to locker client '%s:%s' this should have no effect on the locker if otherwise please report (%s)"%(clientName,clientUniqueName, err,))
                continue
            except Exception as err:
                self._critical("Connection error to locker client '%s:%s' (%s)"%(clientName,clientUniqueName,err))
                break
            # check request data
            try:
                lastTimeout = None
                assert isinstance(received, dict), "received data must be a dict. PLEASE REPORT"
                assert 'request_unique_id' in received, "received dict must have 'request_unique_id' key"
                assert isinstance(received['request_unique_id'], basestring), "received 'resquest_unique_id' value must be a string"
                assert 'client_unique_name' in received, "received dict must have 'client_unique_name' key"
                assert received['client_unique_name'] == clientUniqueName, "received dict 'client_name' key value '%s' does not match registered clientUniqueName '%s'"%(received['client_unique_name'], clientUniqueName)
                assert 'client_name' in received, "received dict must have 'client_name' key"
                assert received['client_name'] == clientName, "received dict 'client_name' key value '%s' does not match registered clientName '%s'"%(received['client_unique_name'], clientName)
                assert 'action' in received, "received dict must have 'action' key"
                if received['action'] == 'publish':
                    assert 'message' in received, "received action '%s' must have 'message' key"%response['action']
                    message = received['message']
                    assert isinstance(message, basestring), "received action '%s' message must be a string"%response['action']
                else:
                    assert received['action'] in ('acquire','release','publish'), "received dict must have 'action' key value must be either 'acquire', 'release' or 'publish'"
                    assert 'path' in received, "received dict must have 'path' key"
                    path = received['path']
                    if isinstance(path, basestring):
                        path = [path]
                        assert isinstance(path, (list,set,tuple)),  "received dict must have 'path' key value must be either a string or a list"
                        assert all([isinstance(p, basestring) for p in path]), "received dict path list items must be all strings"
                        path = tuple(set(path))
                if received['action'] == 'acquire':
                    assert 'request_utctime' in received, "received dict must have 'requestUTCTime' key"
                    received['request_utctime'] = float(received['request_utctime'])
                    received['received_utctime'] = time.time()
                    assert received['received_utctime']>received['request_utctime'], "request utctime is set before it is being received at server! PLEASE REPORT"
                    assert 'timeout' in received, "received dict must have 'timeout' key"
                    received['timeout'] = float(received['timeout'])
                    assert received['timeout']>0, "request timeout must be >0"
                # send for processing
                self.__process_client_request(request=received, connection=connection)
            except Exception as err:
                self._error("Unable to serve locker client request (%s)"%err)
                continue
        # remove client from LUT
        try:
            with self.__pathsLUTLock:
                if self.__pathsLUT is not None:
                    for p in list(self.__pathsLUT):
                        if self.__pathsLUT[p]['client_unique_name'] == clientUniqueName:
                            self._warn("Lock on path '%s' is released. Server no more serving client '%s:%s'"%(path, clientName, clientUniqueName),force=True)
                            self.__pathsLUT.pop(p)
        except Exception as err:
            self._error("Unable to clean locks after locker client '%s:%s' (%s)"%(clientName,clientUniqueName,err))
        # remove client acquire requests from queue
        try:
            with self.__clientsQueueLock:
                queue = []
                for req in self.__clientsQueue:
                    if req['client_unique_name']==clientUniqueName:
                        self._warn("Queued request to lock path '%s' is removed. locker server no more serving locker client '%s:%s'"%(path,clientName,clientUniqueName),force=True)
                    else:
                        queue.append(q)
                self.__clientsQueue = queue
                self.__clientsQueueEvent.set()
        except Exception as err:
            self._error("Unable to clean queue after locker client '%s:%s' (%s)"%(clientName,clientUniqueName,err))
        # pop client
        try:
            with self.__clientsLUTLock:
                if self.__clientsLUT is not None:
                    client = self.__clientsLUT.pop(clientUniqueName, None)
                    if client is not None:
                        client['connection'].close()
        except Exception as err:
            self._error("Unable to clean after locker client '%s:%s' (%s)"%(clientName,clientUniqueName,err))

    @_reconnect_client
    def __listen_to_server(self):
        assert self.__connection is not None, "connection must not be None. PLEASE REPORT"
        self.__wasClient = True
        error = None
        while not self._killSignal:
            try:
                received = self.__connection.recv()
            except EOFError as err:
                error = "Connection end of file encountered (%s)"%err
                break
            except IOError as err:
                error = "Connection IO Error encountered (%s)"%err
                break
            except Exception as err:
                error = "Connection recv Error encountered (%s)"%err
                break
            try:
                self.__process_server_response(received)
            except Exception as err:
                self._error("Received from locker server is not accepted (%s)"%err)
                continue
        if error is not None:
            self._critical(error)
        # stop client
        self._stop_client()

    @_reconnect_server
    def __wait_for_clients(self):
        ### run connections loop
        self._stopServing = False
        self.__pathsLUT   = {}
        self.__clientsLUT = {}
        while not self._stopServing and not self._killSignal:
            try:
                connection = self.__server.accept()
                if connection is None:
                    continue
                # get client information
                try:
                    information = connection.recv()
                except Exception as err:
                    self._critical("Unable to receive locker client name. Connection refused (%s)"%(err,))
                    connection.close()
                    continue
                if not isinstance(information, dict):
                    self._critical("locker client information must be a dictionary, '%s' is given. Connection refused"%(information,))
                    connection.close()
                    continue
                clientName       = information.get('name', None)
                if not isinstance(clientName, basestring):
                    self._critical("locker client name must be given")
                    connection.close()
                    continue
                clientUniqueName = information.get('unique_name', None)
                if not isinstance(clientUniqueName, basestring):
                    self._critical("Client unique name must be given")
                    connection.close()
                    continue
                # send server unique name
                try:
                    connection.send({'server_file':self.__serverFile,'server_name':self.__name, 'server_unique_name':self.__uniqueName, 'lock_maximum_acquired_time':self.__maxLockTime})
                except Exception as err:
                    self._critical("Unable to send locker client '%s:%s' the locker server unique name (%s). Connection refused"%(clientName,clientUniqueName,err))
                    connection.close()
                    continue
                # add client to LUT
                with self.__clientsLUTLock:
                    if clientUniqueName in self.__clientsLUT:
                        self._critical("locker client name '%s:%s' is already registered. Connection refused"%(clientName,clientUniqueName,))
                        connection.close()
                        continue
                    clientUTCTime = time.time()
                    trd = _LockerThread(locker=self, target=self.__serve_client, args=[connection,clientName,clientUniqueName], name='serve_client_%s'%clientUTCTime)
                    self.__clientsLUT[clientUniqueName] = {'connection':connection, 'thread':trd, 'client_accepted_utctime':clientUTCTime, 'client_name':clientName, 'client_unique_name':clientUniqueName}
                    trd.start()
            except socket.timeout as err:
                self._error("Connection timeout '%s' this should have no effect on the locker if otherwise please report"%(err,))
                continue
            except Exception as err:
                self._critical('locker server is down (%s)'%err)
                break

    @_reconnect_server
    def __acquired_locks_max_time_monitor(self):
        while not self._stopServing and not self._killSignal:
            expired = {}
            minimum = None
            with self.__pathsLUTLock:
                self.__deadLockEvent.set()
                self.__deadLockEvent.clear()
                if self.__pathsLUT is None:
                    self._warn("pathsLUT is found None, server is down!")
                    break
                exceeded = {}
                for key in list(self.__pathsLUT):
                    pldata = self.__pathsLUT[key]
                    remain = self.__maxLockTime - (time.time()-pldata['acquired_utctime'])
                    if remain<=0:
                        _ = self.__pathsLUT.pop(key)
                        d = exceeded.setdefault(pldata['client_unique_name'], {'client_name':pldata['client_name'],'connection':pldata['connection'], 'ruid':{}})
                        _ = d['ruid'].setdefault(pldata['request_unique_id'], pldata['path'])
                    elif minimum is None:
                        minimum = (remain, pldata)
                    elif minimum[0]>remain:
                        minimum = (remain, pldata)
                # create responses and release locks per client
                for _cuname in exceeded:
                    _cname = exceeded[_cuname]['client_name']
                    responses = []
                    for _reqId in exceeded[_cuname]['ruid']:
                        responses.append({'action':'exceeded_maximum_lock_time',
                                          'path':exceeded[_cuname]['ruid'][_reqId],
                                          'client_unique_name':_cuname,
                                          'client_name':_cname,
                                          'request_unique_id':_reqId})
                    _connection = exceeded[_cuname]['connection']
                    try:
                        if _connection is None:
                            self.__process_server_response(responses)
                        else:
                            _connection.send(responses)
                    except Exception as err:
                        self._critical("Unable to inform client '%s:%s' about releasing locks %s because it has been acquired longer than maximum locking time '%s'"%(_cname,_cuname,exceeded[_cuname]['request_unique_id'],self.__maxLockTime))
                    else:
                        _luids = [i['request_unique_id'] for i in responses]
                        _paths = [i['path'] for i in responses]
                        self._warn("Locks %s requested by client %s:%s for all paths %s are released by server because maximum lock time is exceed and the lock is required by another client"%(_luids,_cname,_cuname,_paths))

            # wait for timeout
            if minimum is None:
                self._warn("WATING for 100 sec. FOR NEW EVENT.")
                minimum = (100, None)
            else:
                self._warn("WATING FOR %s secs. for %s"%(minimum[0],minimum[1]))
            # free up clients queue event for searching
            with self.__clientsQueueLock:
                self.__clientsQueueEvent.set()
                self.__clientsQueueEvent.clear()
            # wait outside of acquire
            self.__deadLockEvent.wait(minimum[0])

    @_reconnect_server
    def __launch_queue_monitor(self):
        while not self._stopServing and not self._killSignal:
            self.__clientsQueueEvent.wait(100)
            with self.__clientsQueueLock:
                self.__clientsQueueEvent.set()
                self.__clientsQueueEvent.clear()
                # loop all requests in queue
                queueRemaining = []
                for request in self.__clientsQueue:
                    requestCName  = request['client_name']
                    requestCUName = request['client_unique_name']
                    requestRuid   = request['request_unique_id']
                    requestPath   = request['path']
                    if (time.time()-request['request_utctime'])>=request['timeout']:
                        self._warn("Request id '%s' from client '%s:%s' timeout has expired. Lock is not acquired and request is removed from the queue"%(requestRuid, requestCName,requestCUName))
                        continue
                    with self.__pathsLUTLock:
                        canLock = True
                        for p in requestPath:
                            if p in self.__pathsLUT:
                                canLock = False
                        if not canLock:
                            queueRemaining.append(request)
                            continue
                        else:
                            # lock paths
                            acquiredUTCTime = time.time()
                            # lock all paths in requestPath list
                            for p in requestPath:
                                _pathLockDict = {'lock_of_path':p,
                                                 'acquired_utctime':acquiredUTCTime}
                                _pathLockDict.update(request)
                                self.__pathsLUT[p] = _pathLockDict
                            # inform client that lock is acquired
                            try:
                                request['action'] = 'acquired'
                                connection = request.pop('connection')
                                if connection is None:
                                    self.__process_server_response(received=[request])
                                else:
                                    connection.send(request)
                            except Exception as err:
                                for p in requestPath:
                                    _ = self.__pathsLUT.pop(p)
                                self._error("Unable to inform client '%s:%s' about lock id '%s' locking %s being acquired (%s)"%(requestCName,requestCUName,requestRuid,requestPath,err))
                            else:
                                self._info("Client '%s:%s' acquired lock '%s' locking for all %s"%(requestCName,requestCUName,requestRuid,requestPath))
                # reset queue
                self.__clientsQueue = queueRemaining

    @_reconnect_server
    def __update_severfile_fingerprint(self):
        fingerprint      = self.fingerprint
        serverFile       = self.__serverFile
        now              = None
        server           = self.__server
        # forbid reconnecting
        self.__wasServer = True
        self._warn('Starting as a server @%s:%s'%(self.__address,self.__port))
        while not self._stopServing and not self._killSignal:
            try:
                if self.__server is None:
                    break
                if self.__server._listener is None:
                    break
                if now is not None:
                    uniqueName, timestamp, address, port, pid = self.get_running_server_fingerprint(serverFile=serverFile, raiseNotFound=False)
                    if uniqueName != self.uniqueName or address!=self.__address or port!=str(self.__port) or pid!=str(self.__pid) or timestamp!=now:
                        raise Exception("server fingerprint has changed. This shouldn't have happened unless you or another process mistakenly changed '%s'. PLEASE REPORT"%(serverFile,))
                with self.__serverFileLock:
                    with open(serverFile, 'w') as fd:
                        now = str(time.time())
                        fd.write(fingerprint.format(now=now, port=str(self.__port)))
                #time.sleep(1)
                time.sleep(2)
            except Exception as err:
                self._critical("Unable to update serverfile fingerprint (%s)"%(err,))
                break

    def __serve_or_connect(self, ntrials=3):
        if self.__wasServer:
            self._warn('Re-connecting previous server is not allowed.')
            return
        if not self.canServe:
            assert self.__server is None, "This locker instance is not allowed to serve but server is not None. PLEASE REPORT"
            if self.__serverFile is None:
                # user must connect using address and port
                self._warn("Unable to serve nor to connect. serverFile is not defined", force=True)
                return
        else:
            assert self.__pathsLUT is None, "paths look up table must be not defined"
        assert isinstance(ntrials, int), "LockerServer ntrials must be integer"
        assert ntrials>0, "LockerServer ntrials must be >0"
        # force stop serving
        self._stop_server()
        # get needed variables
        fingerprint  = self.fingerprint
        serverFile   = self.__serverFile
        connectStart = None
        nowTimestamp = None
        nowPort      = None
        serverTrials = ntrials
        clientTrials = ntrials
        while True:
            uniqueName, timestamp, address, port, pid = self.get_running_server_fingerprint(serverFile=serverFile, raiseNotFound=False, raiseError=False)
            try:
                diff = time.time()-float(timestamp)
            except:
                diff = 10
            if  self.canServe and ((uniqueName is None or address is None) or (uniqueName!=self.__uniqueName and diff>=2)):
                connectStart = None
                with self.__serverFileLock:
                    with open(serverFile, 'w') as fd:
                        fd.write(fingerprint.format(now='TIMESTAMP', port='PORT'))
                time.sleep(0.001)
                continue
            elif self.canServe and (uniqueName==self.__uniqueName):
                connectStart = None
                assert (address==self.__address or address is None) and (pid==str(self.__pid) or pid is None), "LockerServer uniqueName clash encountered"
                # first step, set timestamp
                if timestamp=='TIMESTAMP' or timestamp is None:
                    assert nowTimestamp is None, 'locker timestamp is assigned before initial serverFile is wrote. PLEASE REPORT'
                    nowTimestamp = time.time()
                    with self.__serverFileLock:
                        with open(serverFile, 'w') as fd:
                            fd.write(fingerprint.format(now=str(nowTimestamp), port='PORT'))
                    time.sleep(0.001)
                    continue
                # second step. assign port and create listener
                elif port=='PORT':
                    assert nowPort is None, 'locker timestamp is assigned before port write to servefFile. PLEASE REPORT'
                    assert timestamp == str(nowTimestamp), "locker timestamp '%s' not matching registered one '%s'. PLEASE REPORT"%(str(timestamp), nowTimestamp)
                    port     = self.__port
                    while serverTrials:
                        serverTrials -= 1
                        try:
                            port = self.__get_first_available_port(address='', start=port, end=65535, step=1)
                            self.__server = Listener((self.__address,port), family='AF_INET', authkey=self.__password)
                        except Exception as err:
                            if serverTrials:
                                self._warn("Unable to launch server @address:%s @port:%s (%s)"%(self.__address,port,err))
                                continue
                            else:
                                self._warn("Unable to launch server (%s)"%(err))
                                return
                                #raise Exception(self._error(err))
                        else:
                            break
                    ## write file
                    nowPort = time.time()
                    with self.__serverFileLock:
                        with open(serverFile, 'w') as fd:
                            fd.write(fingerprint.format(now=nowPort, port=str(port)))
                    # set port launch server
                    self.__port       = port
                    self._stopServing = False
                    trd = _LockerThread(locker=self, target=self.__wait_for_clients, name='wait_for_clients')
                    trd.start()
                    time.sleep(0.001)
                    continue
                # third step, validate port and run update server file
                elif port == str(self.__port):
                    assert timestamp == str(nowPort), "locker timestamp '%s' for port not matching registered one '%s'. PLEASE REPORT"%(str(nowPort), timestamp)
                    # launch clients queue monitor
                    trd = _LockerThread(locker=self, target=self.__launch_queue_monitor, name='launch_queue_monitor')
                    trd.start()
                    # locks maximum acquired time monitor
                    trd = _LockerThread(locker=self, target=self.__acquired_locks_max_time_monitor, name='acquired_locks_max_time_monitor')
                    trd.start()
                    # set server max lock time
                    self.__serverMaxLockTime = self.__maxLockTime
                    # start fingerprint serverfile
                    if self.__blocking:
                        self.__update_severfile_fingerprint()
                    else:
                        trd = _LockerThread(locker=self, target=self.__update_severfile_fingerprint, name='update_severfile_fingerprint')
                        trd.start()
                    return
                else:
                    self._stop_server()
                    raise Exception("locker port has changed! This shouldn't have happened!! PLEASE REPORT")
            # connect as client
            elif clientTrials>0:
                if self.__server is not None:
                    self._stop_server()
                if connectStart is None:
                    connectStart = time.time()
                assert time.time()-connectStart<=self.__connectTimeout, "locker exceeding allowed trying to connect timeout (%s)"%(self.__connectTimeout,)
                # other ServerLocker is attempting be the server
                if timestamp in ('TIMESTAMP', None) or port in ('PORT', None) or address is None:
                    time.sleep(0.01)
                    continue
                else:
                    # try to connect as client regardless of timestamp
                    connected = False
                    try:
                        clientTrials -= 1
                        connected     = self.connect(address=address, port=int(port), ntrials=ntrials)
                    except Exception as err:
                        self._error("Unable to connect to server (%s)"%err)
                    else:
                        if connected:
                            break
                        else:
                            continue
            # no more client trials allowed. Break and fail
            else:
                break

    def __get_ip_address(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            # doesn't even have to be reachable
            s.connect(('10.255.255.255', 1))
            IP = s.getsockname()[0]
        except:
            IP = '127.0.0.1'
        else:
            s.shutdown(socket.SHUT_RDWR)
            s.close()
        return IP


    def __is_port_open(self, address="", port=5555):
        isOpen = False
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.bind((address, port))
        except:
            isOpen = False
        else:
            isOpen = True
        finally:
            s.close()
        return isOpen


    def __get_first_available_port(self, address='', start=10000, end=65535, step=1):
        isOpen = False
        port   = start-step
        while not isOpen:
            port += step
            if port > end:
                break
            isOpen = self.__is_port_open(address=address, port=port)
        if not isOpen:
            port = None
        return port

    ################################ properties ################################
    @property
    def debugMode(self):
        """debug mode flag"""
        return self.__debugMode

    @debugMode.setter
    def debugMode(self, value):
        assert isinstance(value, bool), "debugMode value must be boolean"
        self.__debugMode = value

    @property
    def name(self):
        """locker user given name"""
        return self.__name

    @property
    def fingerprint(self):
        """server locker fingerprint"""
        return "%s({now})@%s:{port}[%s]"%(self.__uniqueName, self.__address, self.__pid)

    @property
    def canServe(self):
        """whether this instance can serve"""
        return self.__allowServing and self.__serverFile is not None and not self.__wasClient

    @property
    def uniqueName(self):
        """locker unique name"""
        return self.__uniqueName

    @property
    def serverFile(self):
        """serverlocker server file"""
        return self.__serverFile

    @property
    def pid(self):
        """python process pid"""
        return self.__pid

    @property
    def serverAddress(self):
        """this instance machine address"""
        return self.__serverAddress

    @property
    def serverPort(self):
        """this instance machine port"""
        return self.__serverPort

    @property
    def serverUniqueName(self):
        """server unique name"""
        return self.__serverUniqueName

    @property
    def serverName(self):
        """server user given name"""
        return self.__serverName

    @property
    def serverMaxLockTime(self):
        """server maximum allowed lock time"""
        return self.__serverMaxLockTime

    @property
    def address(self):
        """locker instance machine address"""
        return self.__address

    @property
    def port(self):
        """locker instance port"""
        return self.__port

    @property
    def password(self):
        """locker password"""
        return self.__password

    @property
    def defaultTimeout(self):
        """locker timeout in seconds"""
        return self.__defaultTimeout

    @property
    def maxLockTime(self):
        """locker maximum locking time in seconds"""
        return self.__maxLockTime

    @property
    def isServer(self):
        """Whether this instance is being the lock server or a client"""
        return self.__server is not None

    @property
    def isClient(self):
        """Whether this instance is being the lock client to a running server"""
        return self.__connection is not None

    @property
    def messages(self):
        """get list of received published messages"""
        return list(self.__publications)

    @property
    def lockedPaths(self):
        """dictionary copy of currently acquired locks by all clients including
        self. This will return None if this locker is not the locker server.
        Keys are paths and values are a dictionary of locks id and client
        name and client unique name"""
        locks = None
        with self.__pathsLUTLock:
            if self.__pathsLUT is not None:
                locks = {}
                for p in self.__pathsLUT:
                    pl = self.__pathsLUT[p]
                    locks[p] = {'lock_unique_id':pl['request_unique_id'],
                                'client_name':pl['client_name'],
                                'client_unique_name':pl['client_unique_name']}
        return locks

    @property
    def ownedLocks(self):
        """dictionary copy of currently acquired locks by this locker.
        keys are locks unique ids and value are path list"""
        with self.__ownRequestsLock:
            locks = dict([(lid, self.__ownAcquired[lid]['path']) for lid in self.__ownAcquired])
        return locks

    @property
    def clientLocks(self):
        """dictionary copy of currently acquired locks by all clients including
        self This will return None if this locker is not the locker server.
        keys are unique locks id and values are the list of paths"""
        locks = None
        with self.__pathsLUTLock:
            if self.__pathsLUT is not None:
                locks = {}
                for p in self.__pathsLUT:
                    pl = self.__pathsLUT[p]
                    ps = locks.setdefault(pl['request_unique_id'], {'client_name':pl['client_name'], 'client_unique_name':pl['client_unique_name'], 'path':[]})['path']
                    ps.append(p)
        return locks

    def _parse_fingerprint(self, fingerprint):
        """parse server fingerprint information"""
        uniqueName, s = fingerprint.split('(')
        timestamp,  s = s.split(')')
        address,    s = s[1:].split(':')
        port,       s = s.split('[')
        pid           = s.split(']')[0]
        return uniqueName, timestamp, address, port, pid


    def get_running_server_fingerprint(self, serverFile=None, raiseNotFound=False, raiseError=True):
        """get running server fingerprint information

        :Parameters:
           #. serverFile (None, string): Path to the locker server file. If
              None is given, this instance serverFile will be used unless it's
              not defined then an error will be raised
           #. raiseNotFound (boolean): Whether to raise an error if file was
              not found
           #. raiseError (boolean): Whether to raise an error upon reading
              and parsing the server file data

        :Returns:
           #. uniqueName (string): the running server unique name
           #. timestamp (string): the running server last saved utc timestamp.
              this must be float castable
           #. address (string): the ip address of the running locker server
           #. port (string): the running server port number. This must be integer
              castable
           #. pid (int): the running server process identification number


        **N.B All returned information can be None if serverFile was not found or if an error parsing the information occured**
        """
        if serverFile is None:
            serverFile = self.__serverFile
        assert serverFile is not None, "given serverFile is None while object serverFile is not defined"
        uniqueName = timestamp = address = port = pid = None
        if os.path.isfile(serverFile):
            try:
                with self.__serverFileLock:
                    with open(serverFile, 'r') as fd:
                        fingerprint = fd.readlines()[0].strip()
                uniqueName, timestamp, address, port, pid = self._parse_fingerprint(fingerprint)
            except Exception as err:
                assert not raiseError, str(err)
        elif raiseNotFound:
            raise Exception("Given serverFile '%s' is not found on disk"%serverFile)
        return uniqueName, timestamp, address, port, pid

    ############################## setter methods ##############################
    def set_maximum_lock_time(self, maxLockTime):
        """
        Set maximum allowed time for a lock to be acquired

        :Parameters:
            #. maxLockTime (number): The maximum number of seconds allowed for
               any lock to be acquired
        """
        try:
            maxLockTime = float(maxLockTime)
            assert maxLockTime>0
        except:
            raise Exception('maxLockTime must be a positive number')
        self.__maxLockTime = maxLockTime

    def set_default_timeout(self, defaultTimeout):
        """
        Set default timeout to acquire a lock

        :Parameters:
            #. maxLockTime (number): the default timeout in seconds for a lock
               to be acquired
        """
        try:
            defaultTimeout = float(defaultTimeout)
            assert defaultTimeout>0
        except:
            raise Exception('defaultTimeout must be a positive number')
        self.__defaultTimeout = defaultTimeout

    def set_server_file(self, serverFile):
        """set server file path

        :Parameters:
           #. serverFile (boolean, string): If True it will be set to
              '.pylocker.serverlocker' in user's home directory. If False, this
              instance will never serve. If string is given, it's the path to the
              serving locker file if existing. When given whether as a string
              or as True, and if this instance is allowed to serve then whenever
              'start' is called, this instance will try to become the serving
              locker unless another instance is serving already then it will
              try to connect.
        """
        assert not (self.isServer or self.isClient), "not allowed to set serverFile when instance is a server or a client"
        if serverFile is True:
            serverFile = os.path.join( os.path.expanduser('~'), '.pylocker.serverlocker')
        elif serverFile is False:
            serverFile = None
        else:
            assert isinstance(serverFile, basestring), "serverFile must be boolean or a string"
            directory = os.path.dirname(serverFile)
            if not os.path.exists(directory):
                os.makedirs(directory)
        self.__serverFile = serverFile


    ############################## connect methods #############################
    def stop(self):
        """Stop server and client connections"""
        self._killSignal = True
        self._stop_server(reconnect=False)
        self._stop_client(reconnect=False)

    def start(self, address=None, port=None, password=None, ntrials=3):
        """start locker as server (if allowed) or a client in case there
        is running server. If both, address and port are None and no server
        is found in the server file, the this instance is going to be the
        server

        :Parameters:
           #. address (None, string): ip address of server to connect to
           #. port (None, integer): port used by the server socket
           #. password (None, string): in case both address and port are not
              None, password is the server password. If None is given, the
              instanciation password is provided.
        """
        if self.isServer:
            self._warn("locker '%s:%s' is already a running server"%(self.__name,self.__uniqueName))
        elif self.isClient:
            self._warn("locker '%s' is already a client to the running server '%s:%s'"%(self.__uniqueName,self.__serverName,self.__serverUniqueName))
        else:
            self.reset()
            if address is not None or port is not None:
                assert address is not None and port is not None, "locker address and port can be either both None or both given"
                self.connect(address=adress, port=port, password=password, ntrials=ntrials)
            else:
                self.__serve_or_connect(ntrials=ntrials)

    def connect(self, address, port, password=None, ntrials=3):
        """connect to a serving locker whether it's local or remote

        :Parameters:
            #. address (string): serving locker ip address
            #. port (integer): serving locker connection port
            #. password (None, string): serving locker password. If None,
               this instance password will be used. If given, this instance
               password will be updated
            #. ntrials (integer): number of trials to connect

        :Returns:
            #. result (boolean): whether connection was successful
        """
        assert self.__connection is None, "locker unable to connect to server, logger is connected to server '%s:%s'"%(self.__serverName,self.__serverUniqueName)
        if password is None:
            password = self.__password
        if not isinstance(password, bytes):
            assert isinstance(password, str), "locker password must be string or bytes"
            password = _to_bytes(password)
        assert isinstance(port, int), "locker port must be integer"
        success  = False
        while ntrials:
            ntrials -= 1
            try:
                _start = time.time()
                connection = Client((address,port), authkey=self.__password)
                connection.send({'name':self.__name, 'unique_name':self.__uniqueName})
                params = connection.recv()
                assert isinstance(params, dict), "locker received server params must be a dictionary"
                assert 'server_name' in params, "locker received server params must have 'server_name' key"
                serverName = params['server_name']
                assert 'server_unique_name' in params, "locker received server params must have 'server_unique_name' key"
                serverUniqueName = params['server_unique_name']
                assert 'lock_maximum_acquired_time' in params, "locker received server params must have 'lock_maximum_acquired_time' key"
                serverMaxLockTime = params['lock_maximum_acquired_time']
                assert isinstance(serverName, basestring), "locker received serverName must be a string"
                assert isinstance(serverMaxLockTime, (float,int)), "locker received serverMaxLockTime must be a number"
                assert serverMaxLockTime>0, "locker received serverMaxLockTime must be >0"
                assert 'server_file' in params, "locker received server params must have 'server_file' key"
                serverFile = params['server_file']
                assert isinstance(serverFile, basestring), "locker received serverFile must be a string"
                serverMaxLockTime = float(serverMaxLockTime)
                serverName       = _to_unicode(serverName)
                serverUniqueName = _to_unicode(serverUniqueName)
                if self.__maxLockTime != serverMaxLockTime:
                    self._warn("Server maximum allowed lock time is '%s' is different than this locker '%s'"%(serverMaxLockTime,self.__maxLockTime))
                # update server File
                if self.__address == address:
                    if serverFile != self.__serverFile and self.__serverFile is not None:
                        self._warn("locker serverFile is updated from '%s' to '%s' after connecting to server %s:%s"%(self.__serverFile, serverFile, serverName, serverUniqueName))
                        self.__serverFile = serverFile
            except Exception as err:
                _dt = time.time()-_start
                success = False
                self._error(err, stack=_full_stack())
                self._warn("locker launch client failed in %.4f sec. %i trials remaining (%s)"%( _dt,ntrials, str(err)))
                continue
            else:
                success = True
                break
        if not success:
            return False
        self.__connection        = connection
        self.__password          = password
        self.__serverUniqueName  = serverUniqueName
        self.__serverName        = serverName
        self.__serverMaxLockTime = serverMaxLockTime
        self.__serverAddress     = address
        self.__serverPort        = port
        self._warn("locker connecting as a client to '%s' @%s:%s"%(serverName, address,port))
        if self.__blocking:
            # as a client, that doesn't make sense but let's allow it anyway
            self.__listen_to_server()
        else:
            trd = _LockerThread(locker=self, target=self.__listen_to_server, args=(), name='__listen_to_server')
            trd.start()
        return True


    ############################### reset method ###############################
    def reset(self, raiseError=False):
        """Used to recycle a disconnected client or serving locker that was
        shut down. Calling reset will insure resetting the state of the locker
        to a freshly new one. If Locker is still serving or still connected
        to a serving locker calling reset will be raise an error if raiseError
        is set to True.

        :Parameters:
            #. raiseError (boolean): whether to raise error if recyling is not
               possible

        :Returns:
            #. success (boolean): whether reset was successful
            #. error (None, string): reason why it failed.
        """
        assert isinstance(raiseError, bool), "raiseError must be boolean"
        if not hasattr(self, '_ServerLocker__serverFile'):
            # initialise signals
            self._killSignal  = False
            self._stopServing = False
            # initialise server
            self.__server = None
            # initialise client
            self.__connection        = None
            self.__serverUniqueName  = None
            self.__serverName        = None
            self.__serverAddress     = None
            self.__serverPort        = None
            self.__serverMaxLockTime = None
            # transfer and serverFile lock
            self.__transferLock   = threading.Lock()
            self.__serverFileLock = threading.Lock()
            self.__wasServer = False # this will be set to true if instance is client only
            self.__wasClient = False # this will be set to true to insure a client will only reconnect as client
            # intialize clients [registered ServerLocker] (only for server ServerLocker)
            self.__clientsLUTLock = threading.Lock()
            self.__clientsLUT     = None
            # intialize system paths (only for server ServerLocker)
            self.__pathsLUTLock = threading.Lock()
            self.__pathsLUT     = None
            # initialize requests queue (only for server ServerLocker)
            self.__clientsQueueLock  = threading.Lock()
            self.__clientsQueue      = []
            self.__clientsQueueEvent = threading.Event()
            # initialize this instance requests
            self.__ownRequestsLock  = threading.Lock()
            self.__ownRequests      = {}
            self.__ownAcquired      = {}
            # publications
            self.__publicationsLock = threading.Lock()
            self.__publications     = {}
            # reconnect threads
            self._reconnectCounterLock = threading.Lock()
            self._reconnectCounter     = 0
            # initialize deadlock event for all locks exeeding maximum allowed acquired time
            self.__deadLockEvent = threading.Event()
            return True, None
        elif self.isServer or self.isClient:
            message = "Not allowed to recycle a client or a serving locker"
            assert not raiseError, message
            return False, message
        else:
            self._killSignal  = False
            self._stopServing = False
            # initialise server
            self.__server = None
            # initialise client
            self.__connection        = None
            self.__serverUniqueName  = None
            self.__serverName        = None
            self.__serverAddress     = None
            self.__serverPort        = None
            self.__serverMaxLockTime = None
            # transfer and serverFile lock
            if self.__transferLock.locked():self.__transferLock.release()
            if self.__serverFileLock.locked():self.__serverFileLock.release()
            self.__wasServer = False # this will be set to true if instance is client only
            self.__wasClient = False # this will be set to true to insure a client will only reconnect as client
            # intialize clients [registered ServerLocker] (only for server ServerLocker)
            if self.__clientsLUTLock.locked():self.__clientsLUTLock.release()
            self.__clientsLUT     = None
            # intialize system paths (only for server ServerLocker)
            if self.__pathsLUTLock.locked():self.__pathsLUTLock.release()
            self.__pathsLUT     = None
            # initialize requests queue (only for server ServerLocker)
            if self.__clientsQueueLock.locked():self.__clientsQueueLock.release()
            self.__clientsQueue      = []
            self.__clientsQueueEvent.set()
            self.__clientsQueueEvent.clear()
            # initialize this instance requests
            if self.__ownRequestsLock.locked():self.__ownRequestsLock.release()
            self.__ownRequests      = {}
            self.__ownAcquired      = {}
            # reconnect threads
            if self._reconnectCounterLock.locked():self._reconnectCounterLock.release()
            self._reconnectCounter     = 0
            # initialize deadlock event for all locks exeeding maximum allowed acquired time
            self.__deadLockEvent.set()
            self.__deadLockEvent.clear()
            # return
            return True, None

    ############################## publish methods #############################
    def __monitor_publication_timeout(request):
        ruuid     = request['request_unique_id']
        message   = request['message']
        remaining = request['timeout']-time.time()+request['request_utctime']
        if remaining >0:
            time.sleep(remaining)
        with self.__publicationsLock:
            if message not in self.__publications:
                self._warn("monitored published message '%s' not found!"%message)
            else:
                reqList = [r for r in self.__publications[message] if r['request_unique_id'] != ruuid]
                if len(reqList) == len(self.__publications[message]):
                    self._warn("monitored published message '%s' from sender '%s' not found! Is it removed seperately by calling 'remove_published_message'?"%(message, ruuid))
                elif not len(reqList):
                    self.__publications.pop(message, None)
                else:
                    self.__publications[message] = reqList

    def __add_publication(self, request):
        message = request['message']
        unique  = request['unique']
        replace = request['replace']
        timeout = request['timeout']
        with self.__publicationsLock:
            if unique:
                assert message not in self.__publications, "Given message '%s' to publish already exist"%message
            if replace:
                self.__publications[message] = [request]
            else:
                self.__publications.setdefault(message, []).append(request)
        # set timeout monitor
        if timeout is not None:
            trd = _LockerThread(locker=self, target=self.__monitor_publication_timeout, args=(request,), name='__monitor_publication_timeout')
            trd.start()

    def remove_published_message(self, message, senders=None):
        """ Remove published message

        :Parameters:
            #. message (string): published message
            #. senders (None, list): list of senders of the message to remove
               publication from a particular sender. If None, published message
               from all senders will be removed
        """
        assert isinstance(message, basestring), "published message must be a string"
        if senders is not None:
            assert isinstance(senders, (list,set,tuple)), "senders must be None or a list of receivers unique name"
            assert all([isinstance(r,basestring) for r in senders]), "senders list items must be all strings"
            senders = dict([(s,True) for s in senders])
        with self.__publicationsLock:
            if message not in self.__publications:
                self._warn("published message '%s' not found!"%message)
            else:
                reqList = self.__publications[message]
                if senders is None:
                    reqList = []
                else:
                    reqDict = {}
                    remList = []
                    for r in reqList:
                        reqDict.setdefault(r['request_unique_id'], []).append(r)
                    for s in senders:
                        if s not in reqDict:
                            self._warn("published message '%s' from sender '%s' not found!"%(message, s))
                            remList.extend(reqDict.pop(s))
                        else:
                            _ = reqDict.pop(s)
                    reqList = remList
                if not len(reqList):
                    self.__publications.pop(message, None)
                else:
                    self.__publications[message] = reqList

    def remove_message(self, *args, **kwargs):
        """alias to remove_published_message"""
        return self.remove_published_message(*args, **kwargs)

    def has_message(self, message):
        """ Get whether a message exists in list of received published messages

        :Parameters:
            #. message (string): published message

        :Returns:
            #. exist (boolean): whether message exist
        """
        return message in self.__publications

    def get_message(self, message):
        """get message from received published messages

        :Parameters:
            #. message (string): published message to get

        :Returns:
            #. publication (None,m dict): the message publication dictionary.
               If message does not exit, None is returned
        """
        return self.__publications.get(message,None)


    def pop_message(self, message):
        """pop message from received published messages

        :Parameters:
            #. message (string): published message to pop

        :Returns:
            #. publication (None,m dict): the message publication dictionary.
               If message does not exit, None is returned
        """
        return self.__publications.pop(message,None)


    def publish_message(self, message, receivers=None, timeout=None, toSelf=True, unique=False, replace=True):
        """publish a message to connected ServerLocker instances. This method
        makes pylocker.ServerLocker more than a locking server but a message
        passing server between threads and processes.

        :Parameters:
            #. message (string): Any message to publish
            #. receivers (None, list): List of ServerLocker instances unique name
               to publish message to. If None, all connected ServerLocker instances
               to this server or to this client server will receive the message
            #. timeout (None, number): message timeout on the receiver side.
               If timeout exceeds, receiver will automatically remove the message
               from the list of publications
            #. toSelf (boolean): whether to also publish to self
            #. unique (boolean): whether message is allowed to exist in the list
               of remaining published message of every and each receiver
               seperately
            #. replace (boolean): whether to replace existing message at
               every and each receiver

        :Returns:
            #. success (boolean): whether publishing was successful
            #. publicationUniqueId (str, int): The publication unique Id.
               If success is False, this become the integer failure code

                *  1: Connection to serving locker is unexpectedly not found.
                *  2: This ServerLocker instance is neither a client nor a server.
                *  string: any other error message.
        """
        # check parameters
        assert isinstance(unique, bool), "unique must be a boolean"
        assert isinstance(toSelf, bool), "toSelf must be a boolean"
        assert isinstance(replace, bool), "replace must be a boolean"
        if receivers is not None:
            assert isinstance(receivers, (list,set,tuple)), "receivers must be None or a list of receivers unique name"
            assert all([isinstance(r,basestring) for r in receivers]), "receivers list items must be all strings"
            toSelf = toSelf or self.__uniqueName in receivers
            #if toSelf:
            #    receivers = [r for r in receivers if r != self.__uniqueName]
        assert isinstance(message, basestring), "publishing message must be a string"
        if timeout is not None:
            assert isinstance(timeout, (int,float)), "timeout must be a number"
            assert timeout>0, "timeout must be >0"
        # create request dictionary
        utcTime = time.time()
        ruuid   = str(uuid.uuid1())
        request = {'request_unique_id':ruuid,
                   'action':'publish',
                   'message':message,
                   'timeout':timeout,
                   'unique':unique,
                   'replace':replace,
                   'receivers':receivers,
                   'to_self':toSelf,
                   'client_unique_name':self.__uniqueName,
                   'client_name':self.__name,
                   'request_utctime':utcTime}
        # publish to self
        if toSelf:
            self.__add_publication(request=request)
        # send request
        try:
            if self.isServer:
                self.__process_client_request(request=request,  connection=None)
            elif self.isClient:
                with self.__transferLock:
                    assert self.__connection is not None, '1'
                    self.__connection.send(request)
            else:
                raise Exception('2')
        except Exception as err:
            code = str(err)
            try:
                code = int(code)
            except:
                pass
            return False, code
        # return result
        return True, ruuid

    def publish(self, *args, **kwargs):
        """alias to publish_message"""
        return self.publish_message(*args, **kwargs)


    ############################### lock methods ###############################
    def acquire_lock(self, path, timeout=None, lockGlobal=False):
        """ Acquire a lock for given path or list of paths. Each time the
        method a called a new lock will be acquired. This method is blocking,
        If lock on path is already acquired even from the same process the
        function will block. If lockGlobal is True, then acquiring the
        lock on a locked path by the same process won't block and will
        return successfully by all threads trying to acquire it.

        :Parameters:
            #. path (string, list): string path of list of strings to lock
            #. timeout (None, integer): timeout limit to acquire the lock. If
               None, defaultTimeout will be used
            #. lockGlobal (boolean): whether to make the acquire global to
               all threads of the same process. If True, until the lock
               expires, any thread of the same process can request the
               exact same lock path and acquire it without being blocked.
               THIS IS NOT IMPLEMENTED YET

        :Returns:
            #. success (boolean): whether locking was successful
            #. lockUniqueId (str, int): The lock unique Id. If success is False,
               this become the integer failure code

                *  0: Lock was not successfully set before timeout.
                *  1: Connection to serving locker is unexpectedly not found.
                *  2: This ServerLocker instance is neither a client nor a server.
                *  string: any other error message.
        """
        # check lockGlobal
        #assert isinstance(lockGlobal, boolean), "lockGlobal must be boolean"
        # check path
        if isinstance(path, basestring):
            path = [path]
        assert len(path), "path must be given"
        assert all([isinstance(p, basestring) for p in path]), "path must be a string or a list of string"
        path = [_to_bytes(p) for p in path]
        # check timeout
        if timeout is None:
            timeout = self.__defaultTimeout
        assert isinstance(timeout, (int,float)), "timeout must be a number"
        assert timeout>0, "timeout must be >0"
        # create request dictionary
        utcTime = time.time()
        ruuid   = str(uuid.uuid1())
        request = {'request_unique_id':ruuid,
                   'action':'acquire',
                   'path':path,
                   'timeout':timeout,
                   'client_unique_name':self.__uniqueName,
                   'client_name':self.__name,
                   'request_utctime':utcTime}
        # append to requests queue. Multiple threads of the same process can be requesting the lock for the same path
        with self.__ownRequestsLock:
            event  = threading.Event()
            ownReq = {'acquired_event':event}
            ownReq.update(request)
            self.__ownRequests[ruuid] = ownReq
        # send request
        try:
            if self.isServer:
                self.__process_client_request(request=request,  connection=None)
            elif self.isClient:
                with self.__transferLock:
                    assert self.__connection is not None, '1'
                    self.__connection.send(request)
            else:
                event.set()
                with self.__ownRequestsLock:
                    self.__ownRequests.pop(ruuid, None)
                raise Exception('2')
        except Exception as err:
            code = str(err)
            try:
                code = int(code)
            except:
                pass
            return False, code
        # wait for lock to be acquired and return result
        isAcquired = event.wait(timeout)
        with self.__ownRequestsLock:
            self.__ownRequests.pop(ruuid, None)
            # maybe in between acquiring the lock happened. checking gracefuly
            isAcquired = ruuid in self.__ownAcquired
            if not isAcquired:
                ruuid = 0
        # return result
        return isAcquired, ruuid

    def acquire(self, *args, **kwargs):
        """alias to acquire"""
        return self.acquire_lock(*args, **kwargs)

    def release_lock(self, lockId):
        """ release acquired lock given its id

        :Parameters:
            #. lockId (string): Lock id as returned from acquire_lock method

        :Returns:
            #. success (boolean): whether lock is released
            #. code (int, string): reason for graceful release or failing to
               release code.

               *  0: Lock is not found, therefore successfully released
               *  1: Lock is found owned by this locker and successfully released
               *  2: Connection to serving locker is unexpectedly not found.
               *  3: Locker is neither a client nor a server
               *  string: any other error message

        """
        with self.__ownRequestsLock:
            request = self.__ownAcquired.pop(lockId, None)
            if request is None:
                return True, 0
            else:
                request['action'] = 'release'
                _ = request.pop('acquired_event', None)
        # release lock
        try:
            if self.isServer:
                self.__process_client_request(request=request, connection=None)
            elif self.isClient:
                with self.__transferLock:
                    assert self.__connection is not None, '2'
                    self.__connection.send(request)
            else:
                raise Exception("3")
        except Exception as err:
            code = str(err)
            try:
                code = int(code)
            except:
                pass
            return False, code
        # release
        return True, 1

    def release(self, *args, **kwargs):
        """alias to release"""
        return self.release_lock(*args, **kwargs)






class SingleLocker(ServerLocker):
    """
    This is singleton implementation of ServerLocker class. It's better to
    create a single locker in a process.
    """
    __thisInstance = None
    def __new__(cls, *args, **kwds):
        if cls.__thisInstance is None:
            cls.__thisInstance = super(ServerLocker,cls).__new__(cls)
            cls.__thisInstance._isInitialized = False
        return cls.__thisInstance

    def __init__(self, *args, **kwargs):
        if (self._isInitialized): return
        # initialize
        super(SingleLocker, self).__init__(*args, **kwargs)
        # update flag
        self._isInitialized = True


class LockersFactory(object):
    """Locker factory is a helper implementation to help developping applications
    that require lockers cross referencing. This can create problems upon
    deserialization. Using lockers factory will solve that issue.

    Locker taken from factory is not guaranteed to be started especially
    if it is instanciated by factory. User must call locker.start()


    .. code-block:: python

            from pylocker import FACTORY

            # create or get existing locker instance
            # setting key as serverFile is good practice
            # all other arguments will be used only if locker does not exist
            # in factory and it must be created.
            L0 = FACTORY(key='my_unique_locker_key', password='my_password', autoconnect=False)
            L1 = FACTORY(key='my_unique_locker_key', password='another_password')

            # verify that L0 is L1
            print(L0 is L1)
            print(L0.password, L1.password)


    """
    __thisInstance = None
    def __new__(cls, *args, **kwds):
        if cls.__thisInstance is None:
            cls.__thisInstance = super(LockersFactory,cls).__new__(cls)
            cls.__thisInstance._isInitialized = False
        return cls.__thisInstance

    def __init__(self):
        if (self._isInitialized): return
        # update flag
        self._isInitialized = True
        # start lockers lut
        self.__lut = {}

    def __call__(self, key, *args, **kwargs):
        return self.get(key=key, *args, **kwargs)

    def get(self, key, *args, **kwargs):
        """get locker instance given a key.
        If locker is not found by key then it's created
        using *args and **kwargs and returned

        :Parameters:
            #. key (string): locker key. Usually it should be the serverFile path

        :Returns:
            #. locker (ServerLocker): the locker instance
        """
        assert isinstance(key, basestring), "key must be a string"
        key = _to_unicode(key)
        if key not in self.__lut:
            self.__lut[key] = ServerLocker(*args, **kwargs)
        return self.__lut[key]


FACTORY = LockersFactory()


#
