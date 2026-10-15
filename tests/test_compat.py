"""Tests pylocker the way pyrep, fullrmc and fullrmc_webapp use it; the pyrep tests are skipped when pyrep is missing."""
import importlib.util
import os
import subprocess
import sys
import unittest

from helpers import LockerTestCase, child_code

FULLRMC_PASSWORD = '8830844b-cdec-4005-ac25-215eedc72148'


def is_installed(name):
    """Tells whether a package can be imported.

    :Parameters:
        #. name (str): The package name.

    :Returns:
        #. isInstalled (bool): True when the package can be found.
    """
    return importlib.util.find_spec(name) is not None


@unittest.skipUnless(is_installed('pyrep'), 'needs the pyrep package')
class TestPyrepRepository(LockerTestCase):
    """Checks pyrep repositories that several processes share."""

    WORKER = """
    from pyrep import Repository
    path, index, password = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    start = time.time()
    try:
        repository = Repository(password=password)
        repository.load_repository(path, verbose=False)
        for step in range(5):
            repository.dump_file(value=index * 100 + step, relativePath=f'w{index}_{step}.pkl')
        isOk = all(repository.pull(relativePath=f'w{index}_{step}.pkl') == index * 100 + step for step in range(5))
        print(f'ok={isOk}', flush=True)
    except BaseException as error:
        print(f'{type(error).__name__}: {str(error)[:90]} t={time.time() - start:.1f}', flush=True)
    os._exit(0)
    """

    def worker(self, path, index, password):
        """Starts a process that uses the repository.

        :Parameters:
            #. path (str): The repository path.
            #. index (int): The number of the worker.
            #. password (str): The repository password the worker uses.

        :Returns:
            #. process (subprocess.Popen): The running worker.
        """
        return subprocess.Popen([sys.executable, '-u', '-c', child_code(self.WORKER), path, str(index), password],
                                stdout=subprocess.PIPE, text=True)

    def test_workers_share_a_repository(self):
        """Lets four processes write to one repository while the owner keeps reading it."""
        from pyrep import Repository  # inline: pyrep is optional, the class is skipped without it
        path = os.path.join(self.tempFolder, 'repository')
        repository = Repository(password='secret')
        repository.create_repository(path)
        repository.dump_file(value='hello', relativePath='base.pkl')
        workers = [self.worker(path, index, 'secret') for index in range(1, 5)]
        outputs = [worker.communicate(timeout=120)[0].strip() for worker in workers]
        self.assertEqual(outputs, ['ok=True'] * 4)
        self.assertEqual(repository.pull(relativePath='base.pkl'), 'hello')

    def test_wrong_password_is_refused_quickly(self):
        """Refuses a worker with the wrong password in a few seconds."""
        from pyrep import Repository
        path = os.path.join(self.tempFolder, 'repository')
        repository = Repository(password='secret')
        repository.create_repository(path)
        output = self.worker(path, 9, 'WRONG').communicate(timeout=120)[0].strip()
        self.assertIn('AssertionError', output)
        self.assertIn('bad password', output)
        seconds = float(output.rsplit('t=', 1)[1])
        self.assertLess(seconds, 5)

    def test_wrong_password_in_the_same_process_is_refused(self):
        """Refuses a second repository object that uses another password for the same repository."""
        from pyrep import Repository
        path = os.path.join(self.tempFolder, 'repository')
        Repository(password='right').create_repository(path)
        with self.assertRaises(AssertionError):
            Repository(password='wrong').load_repository(path)
        Repository(password='right').load_repository(path)


class TestFullrmcPatterns(LockerTestCase):
    """Checks how fullrmc talks to a running engine and how fullrmc_webapp stops it."""

    ENGINE = """
    from pylocker import FACTORY
    serverFile = sys.argv[1]
    locker = FACTORY(key=serverFile, password={password!r}, serverFile=serverFile, autoconnect=False, reconnect=False)
    locker.start()
    print('ENGINE-READY', os.getpid(), 'isServer =', locker.isServer, flush=True)
    start = time.time()
    while locker.isServer and time.time() - start < 15:
        if locker.has_message('stop_engine'):
            locker.pop_message('stop_engine')
            print('ENGINE-GOT-MESSAGE', flush=True)
        time.sleep(0.05)
    print('ENGINE-STOPPED isServer =', locker.isServer, flush=True)
    os._exit(0)
    """

    def start_engine(self, serverFile):
        """Starts a process that serves like a running fullrmc engine.

        :Parameters:
            #. serverFile (str): The server file path.

        :Returns:
            #. engine (subprocess.Popen): The engine process, already serving.
        """
        code = child_code(self.ENGINE.format(password=FULLRMC_PASSWORD))
        engine = subprocess.Popen([sys.executable, '-u', '-c', code, serverFile], stdout=subprocess.PIPE, text=True)
        self.addCleanup(engine.kill)
        self.assertIn('isServer = True', engine.stdout.readline())
        return engine

    def test_message_reaches_the_engine_and_the_engine_is_stopped(self):
        """Delivers a message to the engine, then stops it with a remote order like the web application."""
        from pylocker import FACTORY
        serverFile = os.path.join(self.tempFolder, '.espressolock')
        engine = self.start_engine(serverFile)
        locker = FACTORY(key=serverFile, password=FULLRMC_PASSWORD, serverFile=serverFile,
                         autoconnect=False, reconnect=False)
        self.track(locker)
        locker.start()
        self.assertTrue(locker.isClient)
        self.assertTrue(locker.publish_message('stop_engine')[0])
        self.assertIn('ENGINE-GOT-MESSAGE', engine.stdout.readline())
        _, _, address, _, pid = locker.get_running_server_fingerprint(
            serverFile=locker.serverFile, raiseNotFound=False, raiseError=False)
        self.assertEqual(int(pid), engine.pid)
        locker.stop_remote(password=locker.password, killRemotePID=False, stopSelf=True)
        self.assertFalse(locker.isClient)
        self.assertIn('ENGINE-STOPPED isServer = False', engine.stdout.readline())
        engine.wait(timeout=20)
        self.assertEqual(engine.returncode, 0)


if __name__ == '__main__':
    unittest.main()
