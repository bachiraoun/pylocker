"""
This script will work from within the main package directory.

python setup.py sdist bdist_wheel
twine upload dist/pylocker-...
"""
from setuptools import setup

# set package name
PACKAGE_NAME = 'pylocker'

# automatically create MANIFEST.in
MANIFEST_COMMANDS = [# include MANIFEST.in
            '# include this file, to ensure we can recreate source distributions',
            'include MANIFEST.in',
            # exclude all .log files
            '\n# exclude all logs',
            'global-exclude *.log',
            # exclude all other non necessary files
            '\n# exclude all other non necessary files ',
            'global-exclude .project',
            'global-exclude .pydevproject',
            # exclude all of the subversion metadata
            '\n# exclude all of the subversion metadata',
            'global-exclude *.svn*',
            'global-exclude .svn/*',
            'global-exclude *.git*',
            'global-exclude .git/*',
            # include all LICENCE files
            '\n# include all license files found',
            'global-include ./*LICENSE.*',
            # include all README files
            '\n# include all readme files found',
            'global-include ./*README.*',
            'global-include ./*readme.*']
with open('MANIFEST.in','w') as fileHandle:
    for line in MANIFEST_COMMANDS:
        fileHandle.write(line)
        fileHandle.write('\n')


# declare classifiers
CLASSIFIERS = """\
Development Status :: 4 - Beta
Intended Audience :: Science/Research
Intended Audience :: Developers
License :: OSI Approved :: GNU Affero General Public License v3
Programming Language :: Python
Programming Language :: Python :: 3
Programming Language :: Python :: 3.9
Programming Language :: Python :: 3.10
Programming Language :: Python :: 3.11
Programming Language :: Python :: 3.12
Programming Language :: Python :: 3.13
Topic :: Software Development
Topic :: Software Development :: Build Tools
Topic :: Scientific/Engineering
Operating System :: Microsoft :: Windows
Operating System :: POSIX
Operating System :: Unix
Operating System :: MacOS
"""

# create descriptions
DESCRIPTION = ("PYthon LOCKER or pylocker package provides a pythonic way to "
               "lock files and strings between threads, processes and machines.")
LONG_DESCRIPTION = (DESCRIPTION + " One locker instance serves and the others "
                    "connect, discovering it through a small fingerprint file. "
                    "Synchronous and asyncio callers are both supported.")


## get package info
PACKAGE_INFO={}
INFO_PATH = '__pkginfo__.py'
with open(INFO_PATH) as fileHandle:
    exec(fileHandle.read(), PACKAGE_INFO)

# create meta data
METADATA = dict(name = PACKAGE_NAME,
                packages=[PACKAGE_NAME],
                package_dir={PACKAGE_NAME: '.'},
                version= PACKAGE_INFO['__version__'] ,
                author=PACKAGE_INFO['__author__'],
                author_email=PACKAGE_INFO['__email__'],
                description = DESCRIPTION,
                long_description = LONG_DESCRIPTION,
                url = PACKAGE_INFO['__onlinedoc__'],
                download_url = PACKAGE_INFO['__repository__'],
                license = 'GNU Affero General Public License v3',
                classifiers=[classifier for classifier in CLASSIFIERS.split('\n') if classifier],
                platforms = ["Windows", "Linux", "Mac OS-X", "Unix"],
                python_requires = ">=3.9",
                install_requires = ["psutil"],
                extras_require = {"msgpack": ["msgpack"]})

# setup
setup(**METADATA)
