# -*- coding: utf-8 -*-
# standard libraries imports
from __future__ import print_function
import os,sys,time,atexit,signal,uuid,traceback
import socket, threading
from multiprocessing.connection import Listener, Client

# SET TO TRUE IF NEEDS DEBUGGING
DEBUG_MODE = False

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




def full_stack():
    try:
        exc = sys.exc_info()[0]
        stack = traceback.extract_stack()[:-1]  # last one would be full_stack()
        if not exc is None:  # i.e. if an exception is present
            del stack[-1]    # remove call of full_stack, the printed exception
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

class LockerThread(threading.Thread):
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
                self.stack_ = full_stack()
            except:
                self.stack_ = None
            self.locker_._error(str(self.error_), stack=self.stack_)
        self.running_ = False


def reconnect_server(method):
    def wrapper(self, *args, **kwargs):
        with self._reconnectCounterLock:
            self._reconnectCounter += 1
        # execute method
        try:
            result = method(self, *args,**kwargs)
        except Exception as err:
            result = None
            self._error(str(err), stack=full_stack())
        with self._reconnectCounterLock:
            self._reconnectCounter -= 1
        # stop server
        self._stop_server(reconnect=True)
        return result
    return wrapper

def reconnect_client(method):
    def wrapper(self, *args, **kwargs):
        with self._reconnectCounterLock:
            self._reconnectCounter += 1
        # execute method
        try:
            result = method(self, *args,**kwargs)
        except Exception as err:
            result = None
            self._error(str(err), stack=full_stack())
        with self._reconnectCounterLock:
            self._reconnectCounter -= 1
        # stop client
        self._stop_client(reconnect=True)
        return result
    return wrapper

def to_bytes(input, encode='utf-8', errors='ignore'):
    if not isinstance(input, bytes):
        input = input.encode(encode, errors=errors)
    return input

def to_unicode(input, decode='utf-8', errors='ignore'):
    if not isinstance(input, unicode):
        input = input.decode(decode, errors=errors)
    return input


class ServerLocker(object):
    """
    Locker implementation that can be used to orchestrate locking and
    releasing string entities between threads and processes.
    ServerLocker is primarily implemented to distribute permission
    between threads and processes to read and write system files.
    Once instanciated, it will connect to the serving ServerLocker instance
    if existing otherwise it will start server to server itself and other
    ServerLocker instances trying to connect.

    :Parameters:
        #. password (string): password to serve or to connect ot existing
          server locker
        #. name (None, string): user defined server locker name
        #. serverFile (Boolean, string): If True it will be set to
           '.pylocker.serverlocker' in user's home directory. If False, this
           instance will never serve. Otherwise if string is given, it's the
           path to the server file if existing or this instance will become
           a server if allowed
        #. defaultTimeout (integer): Default timeout to acquire the lock
        #. macLockTime (integer): Maximum allowed time to acquire the lock
        #. port (int): server port number or any available port will be fetched
           if this is busy
        #. allowServing (boolean): Whether to allow this instance to serve
        #. reconnect (boolean): whether to reconnect if connection drops.
           This is only safe for clients. If server drops every drops with
           it and no locker will reconnect
        #. connectTimeout (integer): timeout limit for connection to create
           successfully
        #. logger (None, boolean, object): If None, logging will be simple
           printing. If False, no logging will be performed. If True, logging
           will be a formatted printing. If object is given, it must have
           all of 'info', 'warn', 'debug', 'error', 'critical' callables
        #. blocking (boolean): Whether to block execution upon connecting. This
           is needed if the instance is launched as a seperate service in a
           seperate process
    """
    def __init__(self, password, name=None, serverFile=True,
                       defaultTimeout=20, maxLockTime=120, port=3000,
                       allowServing=True, reconnect=True,
                       connectTimeout=20, logger=False,
                       blocking=False):
        # set logger
        if not isinstance(logger, bool):
            #if logger is not None:
            for attr in ['info','warn','debug','error','critical']:
                assert hasattr(logger, attr), "Not boolean logger must have '%s' attribute"%(attr,)
                assert callable(getattr(logger, attr)), "Not boolean logger attribute '%s' must be callable"%(attr,)
        self.__logger = logger
        # create unique name
        self.__uniqueName = to_unicode( str(uuid.uuid1()) )
        if name is None:
            name = self.__uniqueName
        else:
            assert isinstance(name, basestring), "server name must be None or a string"
            name = to_unicode( name )
        assert ':' not in name, self._critical("':' not allowed in ServerLocker name")
        self.__name = name
        # initialise signals
        self._killSignal  = False
        self._stopServing = False
        # initialise server
        self._server      = None
        # initialise client
        self._connection        = None
        self.__serverUniqueName  = None
        self.__serverName        = None
        self.__serverAddress     = None
        self.__serverPort        = None
        self.__serverMaxLockTime = None
        # transfer and serverFile lock
        self.__transferLock   = threading.Lock()
        self.__serverFileLock = threading.Lock()
        # check blocking flag
        assert isinstance(blocking, bool), self._critical("blocking must be boolean")
        self._blocking = blocking
        assert isinstance(password, basestring), self._critical("password must be a string")
        if not isinstance(password, bytes):
            password = to_bytes(password)
        self.__password = password
        if serverFile is True:
            serverFile = os.path.join( os.path.expanduser('~'), '.pylocker.serverlocker')
        elif serverFile is False:
            serverFile = None
        else:
            assert isinstance(serverFile, basestring), self._critical("serverFile must be boolean or a string")
            directory = os.path.dirname(serverFile)
            if not os.path.exists(directory):
                os.makedirs(directory)
        self._serverFile = serverFile
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
            assert isinstance(reconnect, int), self._critical("reconnect must be Boolean or integer")
            assert reconnect>=0, self._critical("reconnect must be >=0",)
        elif reconnect is False:
            reconnect = 0
        self._reconnect  = reconnect
        self.__wasServer = False # this will be set to true if instance is client only
        self.__wasClient = False # this will be set to true to insure a client will only reconnect as client
        # get connectTimeout
        assert isinstance(connectTimeout, int), self._critical("connectTimeout must be integer")
        assert connectTimeout>0, self._critical("connectTimeout must ber >0")
        self.__connectTimeout = connectTimeout
        # get first available port
        assert isinstance(port, int), self._critical("port must be integer")
        assert 1<=port<=65535, self._critical("port must be 1<=port<=65535",)
        self.__port = port
        # set can serve
        assert isinstance(allowServing, bool), self._critical("allowServing must be boolean")
        self.__allowServing = allowServing
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
        # reconnect threads
        self._reconnectCounterLock = threading.Lock()
        self._reconnectCounter     = 0
        # initialize deadlock event for all locks exeeding maximum allowed acquired time
        self.__deadLockEvent = threading.Event()
        # register to atexit
        atexit.register( self._on_atexit )
        # start serving loop if this instance can serve
        self.__serve_or_connect()

    ############################# logging methods ##############################
    def _critical(self, message, force=False, stack=None):
        if DEBUG_MODE:
            print('DEBUG MODE: %s - CRITICAL - %s'%(self.__class__.__name__,message))
            return message
        if self.__logger is True or (force and self.__logger is False):
            print('%s - CRITICAL - %s'%(self.__class__.__name__,message))
        elif not isinstance(self.__logger, bool):
            self.__logger.critical(message)
        if stack is not None:
            print(stack)
        return message

    def _error(self, message, force=False, stack=None):
        if DEBUG_MODE:
            print('DEBUG MODE: %s - ERROR - %s'%(self.__class__.__name__,message))
            return message
        if self.__logger is True or (force and self.__logger is False):
            print('%s - ERROR - %s'%(self.__class__.__name__,message))
        elif not isinstance(self.__logger, bool):
            self.__logger.critical(message)
        if stack is not None:
            print(stack)
        return message

    def _warn(self, message, force=False, stack=None):
        if DEBUG_MODE:
            print('DEBUG MODE: %s - WARNING - %s'%(self.__class__.__name__,message))
            return message
        if self.__logger is True or (force and self.__logger is False):
            print('%s - WARNING - %s'%(self.__class__.__name__,message))
        elif not isinstance(self.__logger, bool):
            self.__logger.critical(message)
        if stack is not None:
            print(stack)
        return message

    def _info(self, message, force=False, stack=None):
        if DEBUG_MODE:
            print('DEBUG MODE: %s - INFO - %s'%(self.__class__.__name__,message))
            return message
        if self.__logger is True or (force and self.__logger is False):
            print('%s - INFO - %s'%(self.__class__.__name__,message))
        elif not isinstance(self.__logger, bool):
            self.__logger.critical(message)
        if stack is not None:
            print(stack)
        return message

    def _debug(self, message, force=False, stack=None):
        if DEBUG_MODE:
            print('DEBUG MODE: %s - DEBUG - %s'%(self.__class__.__name__,message))
            return message
        if self.__logger is True or (force and self.__logger is False):
            print('%s - DEBUG - %s'%(self.__class__.__name__,message))
        elif not isinstance(self.__logger, bool):
            self.__logger.critical(message)
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

    ########################## close and stop methods ##########################
    def _on_atexit(self, *args, **kwargs):
        try:
            uniqueName, timestamp, address, port, pid = self.get_running_server_fingerprint(serverFile=self._serverFile, raiseNotFound=False, raiseError=False)
            if uniqueName == self.__uniqueName:
                with self.__serverFileLock:
                    open(self._serverFile, 'w').close()
        except:
            pass
        self.stop()

    def _stop_server(self, reconnect=False):
        try:
            if not self._stopServing:
                self._stopServing = True
                uniqueName, timestamp, address, port, pid = self.get_running_server_fingerprint(serverFile=self._serverFile, raiseNotFound=False, raiseError=False)
                if uniqueName == self.__uniqueName:
                    with self.__serverFileLock:
                        open(self._serverFile, 'w').close()
        except:
            pass
        try:
            self._stopServing = True
            if self._server is not None:
                self._server.close()
        except Exception as err:
            self._debug('Unable to stop server (%s)'%err)
        finally:
            self._server = None
        # kill all clients connection
        try:
            with self.__clientsLUTLock:
                if self.__clientsLUT is not None:
                    for cname in list(self.__clientsLUT):
                        try:
                            self.__clientsLUT.pop(cname)['connection'].close()
                        except Exception as err:
                            self._debug("Unable to close client '%s' connection (%s)"%(cname, err))
                        else:
                            self._debug("Closed connection to '%s'"%cname)
                self.__clientsLUT = None
        except Exception as err:
            self._debug("Unable to close clients connection (%s)"%err)
        # clear dead lock event
        self.__deadLockEvent.set()
        self.__deadLockEvent.clear()
        # clear clients queue event
        self.__clientsQueueEvent.set()
        self.__clientsQueueEvent.clear()
        # reconnect
        if self._reconnectCounter == 0 and reconnect and not self._killSignal:
            if self._reconnect is True or self._reconnect>0:
                if self._reconnect is not True:
                    self._reconnect -= 1
                self._warn('Server stopped! Trying to reconnect')
                self.__serve_or_connect()
            else:
                raise Exception(self._critical('Server stopped! Aborting'))

    def _stop_client(self, reconnect=False):
        try:
            self._connection.close()
        except:
            pass
        finally:
            self._connection = None
        self.__serverUniqueName = None
        self.__serverName       = None
        self.__serverAddress    = None
        self.__serverPort       = None
        # reconnect
        if self._reconnectCounter == 0 and reconnect and not self._killSignal:
            if self._reconnect is True or self._reconnect>0:
                if self._reconnect is not True:
                    self._reconnect -= 1
                #print('Client connection stopped! Trying to reconnect')
                self._warn('Client connection stopped! Trying to reconnect')
                self.__serve_or_connect()
            else:
                #raise Exception('Client connection stopped! Aborting')
                raise Exception(self._critical('Client connection stopped! Aborting'))

    def stop(self):
        """Stop server and client connections"""
        self._killSignal  = True
        self._stop_server(reconnect=False)
        self._stop_client(reconnect=False)

    def start(self, address=None, port=None, password=None):
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
            self._debug("locker '%s' is already a running server"%self.__uniqueName)
        elif self.isClient:
            self._debug("locker '%s' is already a client to the running server '%s'"%(self.__uniqueName, self._serverUnique))
        elif address is not None or port is not None:
            assert address is not None and port is not None, self._error("address and port can be either both None or both given")
            self.connect_to_server_locker(address=adress, port=port, password=password)
        else:
            self.__serve_or_connect()


    ######################### server and client methods ########################
    def __process_server_response(self, received):
        if isinstance(received, dict):
            received = [received]
        assert isinstance(received, (list,set,tuple)), "received data must be a list"
        assert len(received), "received list is emtpy"
        for response in received:
            assert isinstance(response, dict), "received list items must be dictionaries"
            assert 'action' in response, "received dict items must have 'action' key"
            assert response['action'] in ('acquired','released','exceeded_maximum_lock_time',), "received dict items dict 'action' key value is not recognized"
            assert 'path' in response, "received dict items must have 'path' key"
            path = response['path']
            if isinstance(path, basestring):
                path = [path]
            assert isinstance(path, (list,set,tuple)), "received dict 'path' value must be a list"
            assert len(path), "received dict 'path' value list must be not empty"
            assert all([isinstance(p,basestring) for p in path]), "received dict 'path' list items must be all strings. %s is given"%path
            assert 'request_unique_id' in response, "received dict items must have 'request_unique_id' key"
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
                    self._debug("action 'released' received at client !!! This is meaningless")
                elif _action == 'exceeded_maximum_lock_time':
                    if self.__ownAcquired.pop(_ruid, None) is not None:
                        _cname  = response['client_name']
                        _cuname = response['client_unique_name']
                        self._warn("Lock '%s' requested by client %s:%s for all '%s' is released by server because maximum lock time is exceed and the lock is required by another client"%(_ruid,_cname,_cuname,response['path']))
                else:
                    raise Exception("Unkown 'action' '%s'. PLEASE REPORT"%(_action,))


    def __process_client_request(self, request, connection):
        path   = request['path']
        ruid   = request['request_unique_id']
        cname  = request['client_name']
        cuname = request['client_unique_name']
        action = request['action']
        if action =='release':
            with self.__pathsLUTLock:
                self._debug('releasing request: %s'%request)
                for p in path:
                    if p not in self.__pathsLUT:
                        self._debug("requesting to release unlocked path '%s'"%(path,))
                    elif self.__pathsLUT[p]['client_unique_name'] == cuname and self.__pathsLUT[p]['request_unique_id'] == ruid:
                        self.__pathsLUT.pop(p,None)
                    else:
                        self._debug("requesting to release path '%s' locked by different locker"%(path,))
        elif action=='acquire':
            with self.__clientsQueueLock:
                req = {'connection':connection}
                req.update(request)
                self.__clientsQueue.append(req)
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
        self._debug("Client '%s:%s' connected to server"%(clientName,clientUniqueName))
        while not self._stopServing and not self._killSignal:
            lastTimeout = None
            try:
                received = connection.recv()
            ## MUST SPLIT BETWEEN MULTIPLE ERROR TYPES
            except socket.timeout as err:
                if lastTimeout is None:
                    lastTimeout = time.time()
                elif time.time()-lastTimeout < 1:
                    self._critical("Connection to client '%s:%s' has encountered unsuspected successive timeouts within 1 second."%(clientName,clientUniqueName))
                    break
                self._error("Connection timeout to client '%s:%s' this should have no effect on the locker if otherwise please report (%s)"%(clientName,clientUniqueName, err,))
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
                assert received['action'] in ('acquire','release'), "received dict must have 'action' key value must be either 'acquire' or 'release'"
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
                self._error("Unable to serve client request (%s)"%err)
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
            self._error("Unable to clean locks after client '%s:%s' (%s)"%(clientName,clientUniqueName,err))
        # remove client acquire requests from queue
        try:
            with self.__clientsQueueLock:
                queue = []
                for req in self.__clientsQueue:
                    if req['client_unique_name']==clientUniqueName:
                        self._warn("Queued request to lock path '%s' is removed. Server no more serving client '%s:%s'"%(path,clientName,clientUniqueName),force=True)
                    else:
                        queue.append(q)
                self.__clientsQueue = queue
                self.__clientsQueueEvent.set()
        except Exception as err:
            self._error("Unable to clean queue after client '%s:%s' (%s)"%(clientName,clientUniqueName,err))
        # pop client
        try:
            with self.__clientsLUTLock:
                if self.__clientsLUT is not None:
                    client = self.__clientsLUT.pop(clientUniqueName, None)
                    if client is not None:
                        client['connection'].close()
        except Exception as err:
            self._error("Unable to clean after client '%s:%s' (%s)"%(clientName,clientUniqueName,err))

    @reconnect_client
    def __listen_to_server(self):
        assert self._connection is not None, self._critical("connection must not be None. PLEASE REPORT")
        self.__wasClient = True
        error = None
        while not self._killSignal:
            try:
                received = self._connection.recv()
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
                self._error("Received from server is not accepted (%s)"%err)
                continue
        if error is not None:
            self._critical(error)
        # stop client
        self._stop_client()

    @reconnect_server
    def __wait_for_clients(self):
        ### run connections loop
        self._stopServing = False
        self.__pathsLUT   = {}
        self.__clientsLUT = {}
        while not self._stopServing and not self._killSignal:
            try:
                connection = self._server.accept()
                if connection is None:
                    continue
                # get client unique name
                try:
                    names = connection.recv()
                except Exception as err:
                    self._critical("Unable to receive client name. Connection refused (%s)"%(err,))
                    connection.close()
                    continue
                if not isinstance(names, (list,tuple)):
                    self._critical("Client names must be a list, '%s' is given. Connection refused"%(names,))
                    connection.close()
                    continue
                if not len(names)==2:
                    self._critical("Client names list must have two items")
                    connection.close()
                    continue
                if not all([isinstance(i, basestring) for i in names]):
                    self._critical("Client names list items must be all strings")
                    connection.close()
                    continue
                clientName,clientUniqueName = names
                # send server unique name
                try:
                    connection.send({'server_name':self.__name, 'server_unique_name':self.__uniqueName, 'lock_maximum_acquired_time':self.__maxLockTime})
                except Exception as err:
                    self._critical("Unable to send client '%s:%s' the server unique name (%s). Connection refused"%(clientName,clientUniqueName,err))
                    connection.close()
                    continue
                # add client to LUT
                with self.__clientsLUTLock:
                    if clientUniqueName in self.__clientsLUT:
                        self._critical("client name '%s:%s' is already registered. Connection refused"%(clientName,clientUniqueName,))
                        connection.close()
                        continue
                    clientUTCTime = time.time()
                    trd = LockerThread(locker=self, target=self.__serve_client, args=[connection,clientName,clientUniqueName], name='serve_client_%s'%clientUTCTime)
                    self.__clientsLUT[clientUniqueName] = {'connection':connection, 'thread':trd, 'client_accepted_utctime':clientUTCTime, 'client_name':clientName, 'client_unique_name':clientUniqueName}
                    trd.start()
            except socket.timeout as err:
                self._error("Connection timeout '%s' this should have no effect on the locker if otherwise please report"%(err,))
                continue
            except Exception as err:
                self._critical('Server is down (%s)'%err)
                break

    @reconnect_server
    def __acquired_locks_max_time_monitor(self):
        while not self._stopServing and not self._killSignal:
            expired = {}
            minimum = None
            with self.__pathsLUTLock:
                self.__deadLockEvent.set()
                self.__deadLockEvent.clear()
                if self.__pathsLUT is None:
                    self._debug("pathsLUT is found None, server is down!")
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
                self._debug("WATING for 100 sec. FOR NEW EVENT.")
                minimum = (100, None)
            else:
                self._debug("WATING FOR %s secs. for %s"%(minimum[0],minimum[1]))
            # free up clients queue event for searching
            with self.__clientsQueueLock:
                self.__clientsQueueEvent.set()
                self.__clientsQueueEvent.clear()
            # wait outside of acquire
            self.__deadLockEvent.wait(minimum[0])

    @reconnect_server
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

    @reconnect_server
    def __update_severfile_fingerprint(self):
        fingerprint      = self.fingerprint
        serverFile       = self._serverFile
        now              = None
        server           = self._server
        # forbid reconnecting
        self.__wasServer = True
        self._debug('Starting as a server @%s:%s'%(self.__address,self.__port))
        while not self._stopServing and not self._killSignal:
            try:
                if self._server is None:
                    break
                if self._server._listener is None:
                    break
                if now is not None:
                    uniqueName, timestamp, address, port, pid = self.get_running_server_fingerprint(serverFile=serverFile, raiseNotFound=False)
                    if uniqueName != self.uniqueName or address!=self.__address or port!=str(self.__port) or pid!=str(self.__pid) or timestamp!=now:
                        raise Exception("server fingerprint has changed. PLEASE REPORT")
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
            assert self._server is None, self._error("This can't serve but server is not None. PLEASE REPORT")
            if self._serverFile is None:
                # user must connect using address and port
                return
        else:
            assert self.__pathsLUT is None, self._error("paths look up table must be not defined")
        assert isinstance(ntrials, int), self._error("LockerServer ntrials must be integer")
        assert ntrials>0, self._error("LockerServer ntrials must be >0")
        # force stop serving
        self._stop_server()
        # get needed variables
        fingerprint  = self.fingerprint
        serverFile   = self._serverFile
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
                assert (address==self.__address or address is None) and (pid==str(self.__pid) or pid is None), self._error("LockerServer uniqueName clash encountered")
                # first step, set timestamp
                if timestamp=='TIMESTAMP' or timestamp is None:
                    assert nowTimestamp is None, self._error('timestamp is assigned before initial serverFile write. PLEASE REPORT')
                    nowTimestamp = time.time()
                    with self.__serverFileLock:
                        with open(serverFile, 'w') as fd:
                            fd.write(fingerprint.format(now=str(nowTimestamp), port='PORT'))
                    time.sleep(0.001)
                    continue
                # second step. assign port and create listener
                elif port=='PORT':
                    assert nowPort is None, self._error('timestamp is assigned before port write to servefFile. PLEASE REPORT')
                    assert timestamp == str(nowTimestamp), self._error("timestamp '%s' not matching registered one '%s'. PLEASE REPORT"%(str(timestamp), nowTimestamp))
                    port     = self.__port
                    while serverTrials:
                        serverTrials -= 1
                        try:
                            port = self.__get_first_available_port(address='', start=port, end=65535, step=1)
                            self._server = Listener((self.__address,port), family='AF_INET', authkey=self.__password)
                        except Exception as err:
                            if serverTrials:
                                self._debug("Unable to launch server @address:%s @port:%s (%s)"%(self.__address,port,err))
                                continue
                            else:
                                self._debug("Unable to launch server (%s)"%(err))
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
                    trd = LockerThread(locker=self, target=self.__wait_for_clients, name='wait_for_clients')
                    trd.start()
                    time.sleep(0.001)
                    continue
                # third step, validate port and run update server file
                elif port == str(self.__port):
                    assert timestamp == str(nowPort), self._error("timestamp '%s' for port not matching registered one '%s'. PLEASE REPORT"%(str(nowPort), timestamp))
                    # launch clients queue monitor
                    trd = LockerThread(locker=self, target=self.__launch_queue_monitor, name='launch_queue_monitor')
                    trd.start()
                    # locks timeout monitor
                    #trd = LockerThread(locker=self, target=self.__requests_timeout_monitor, name='requests_timeout_monitor')
                    #trd.start()
                    # locks maximum acquired time monitor
                    trd = LockerThread(locker=self, target=self.__acquired_locks_max_time_monitor, name='acquired_locks_max_time_monitor')
                    trd.start()
                    # set server max lock time
                    self.__serverMaxLockTime = self.__maxLockTime
                    # start fingerprint serverfile
                    if self._blocking:
                        self.__update_severfile_fingerprint()
                    else:
                        trd = LockerThread(locker=self, target=self.__update_severfile_fingerprint, name='update_severfile_fingerprint')
                        trd.start()
                    return
                else:
                    self._stop_server()
                    raise Exception(self._critical("port has changed! This shouldn't have happened!! PLEASE REPORT"))
            # connect as client
            elif clientTrials>0:
                if self._server is not None:
                    self._stop_server()
                if connectStart is None:
                    connectStart = time.time()
                assert time.time()-connectStart<=self.__connectTimeout, self._error("Exceeding allowed trying to connect timeout (%s)"%(self.__connectTimeout,))
                # other ServerLocker is attempting be the server
                if timestamp in ('TIMESTAMP', None) or port in ('PORT', None) or address is None:
                    time.sleep(0.01)
                    continue
                else:
                    # try to connect as client regardless of timestamp
                    connected = False
                    try:
                        clientTrials -= 1
                        connected     = self.connect_to_server_locker(address=address, port=int(port), ntrials=ntrials)
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


    def connect_to_server_locker(self, address, port, password=None, ntrials=3):
        """connect to an existing server whether it's local or remote

        :Parameters:
            #. address (string): server ip address
            #. port (integer): server connection port
            #. password (None, string): server password. If None, this instance
               password will be used
            #. ntrials (integer): number of trials to connect

        :Returns:
            #. result (boolean): whether connection was successful
        """
        assert self._connection is None, self._error("Unable to connect to server, logger is connected to server '%s:%s'"%(self.__serverName,self.__serverUniqueName))
        if password is None:
            password = self.__password
        if not isinstance(password, bytes):
            assert isinstance(password, str), self._error("password must be string or bytes")
            password = to_bytes(password)
        assert isinstance(port, int), self._error("port must be integer")
        success  = False
        while ntrials:
            ntrials -= 1
            try:
                _start = time.time()
                connection = Client((address,port), authkey=self.__password)
                connection.send((self.__name, self.__uniqueName))
                params = connection.recv()
                assert isinstance(params, dict), "received server params must be a dictionary"
                assert 'server_name' in params, "received server params must have 'server_name' key"
                serverName = params['server_name']
                assert 'server_unique_name' in params, "received server params must have 'server_unique_name' key"
                serverUniqueName = params['server_unique_name']
                assert 'lock_maximum_acquired_time' in params, "received server params must have 'lock_maximum_acquired_time' key"
                assert isinstance(serverName, basestring), "serverName must be a string"
                serverMaxLockTime = params['lock_maximum_acquired_time']
                assert isinstance(serverMaxLockTime, (float,int)), "serverMaxLockTime must be a number"
                assert serverMaxLockTime>0, "serverMaxLockTime must be >0"
                serverMaxLockTime = float(serverMaxLockTime)
                serverName       = to_unicode(serverName)
                serverUniqueName = to_unicode(serverUniqueName)
                if self.__maxLockTime != serverMaxLockTime:
                    self._debug("Server maximum allowed lock time is '%s' is different than this locker '%s'"%(serverMaxLockTime,self.__maxLockTime))
            except Exception as err:
                _dt = time.time()-_start
                success = False
                self._error(err, stack=full_stack())
                self._debug("Launch client failed in %.4f sec. %i trials remaining (%s)"%( _dt,ntrials, str(err)))
                continue
            else:
                success = True
                break
        if not success:
            return False
        self._connection         = connection
        self.__password          = password
        self.__serverUniqueName  = serverUniqueName
        self.__serverName        = serverName
        self.__serverMaxLockTime = serverMaxLockTime
        self.__serverAddress     = address
        self.__serverPort        = port
        self._debug("Connecting as a client to '%s' @%s:%s"%(serverName, address,port))
        if self._blocking:
            # as a client, that doesn't make sense but let's allow it anyway
            self.__listen_to_server()
        else:
            trd = LockerThread(locker=self, target=self.__listen_to_server, args=(), name='__listen_to_server')
            trd.start()
        return True

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
        return self.__allowServing and self._serverFile is not None and not self.__wasClient

    @property
    def uniqueName(self):
        """locker unique name"""
        return self.__uniqueName

    @property
    def serverFile(self):
        """serverlocker server file"""
        return self._serverFile

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
        return self._server is not None

    @property
    def isClient(self):
        """Whether this instance is being the lock client to a running server"""
        return self._connection is not None

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


    def acquire_lock(self, path, timeout=None):
        """ Acquire a lock for given path

        :Parameters:
            #. path (string, list): string path of list of strings to lock
            #. timeout (None, integer): timeout limit to acquire the lock. If
               None, defaultTimeout will be used

        :Returns:
            #. success (boolean): whether locking was successful
            #. lockUniqueId (str): The lock unique Id. If success is False,
               this becomes the reason why the lock is not acquired
        """
        # check path
        if isinstance(path, basestring):
            path = [path]
        assert len(path), self._error("path must be given")
        assert all([isinstance(p, basestring) for p in path]), self._error("path must be a string or a list of string")
        path = [to_bytes(p) for p in path]
        # check timeout
        if timeout is None:
            timeout = self.__defaultTimeout
        assert isinstance(timeout, (int,float)), self._error("timeout must be a number")
        assert timeout>0, self._error("timeout must be >0")
        # create received dictionary
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
                    assert self._connection is not None, self._error("Locker client connection is not found")
                    self._connection.send(request)
            else:
                event.set()
                with self.__ownRequestsLock:
                    self.__ownRequests.pop(ruuid, None)
                raise Exception("Locker is not a server nor a client")
        except Exception as err:
            return False, str(err)
        # wait for lock to be acquired and return result
        isAcquired = event.wait(timeout)
        with self.__ownRequestsLock:
            self.__ownRequests.pop(ruuid, None)
            # maybe in between acquiring the lock happened. checking gracefuly
            isAcquired = ruuid in self.__ownAcquired
            if not isAcquired:
                ruuid = 'timeout exhausted'
        # return result
        return isAcquired, ruuid


    def release_lock(self, lockId):
        """ release acquired lock given its id

        :Parameters:
            #. lockId (string): Lock id as returned from acquire_lock method

        :Returns:
            #. success (boolean): whether lock is released
            #. message (str): reason for graceful release or failing to release
               error message
        """
        with self.__ownRequestsLock:
            request = self.__ownAcquired.pop(lockId, None)
            if request is None:
                return True, "Release not needed. Lock does not exist"
            else:
                request['action'] = 'release'
                _ = request.pop('acquired_event', None)
        # release lock
        try:
            if self.isServer:
                self.__process_client_request(request=request, connection=None)
            elif self.isClient:
                with self.__transferLock:
                    assert self._connection is not None, self._error("Locker client connection is not found")
                    self._connection.send(request)
            else:
                raise Exception("Locker is not a server nor a client")
        except Exception as err:
            return False, str(err)
        # release
        return True, ''






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
        super(ServerLocker, self).__init__(*args, **kwargs)
        # update flag
        self._isInitialized = True
#
