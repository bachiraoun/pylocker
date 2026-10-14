"""
This script will work from within the main package directory.

python setup.py sdist bdist_wheel
twine upload dist/pylocker-...
"""
from setuptools import setup

# set package path and name
PACKAGE_PATH = '.'
PACKAGE_NAME = 'pylocker'

# automatically create MANIFEST.in
commands = [# include MANIFEST.in
            '# include this file, to ensure we can recreate source distributions',
            'include MANIFEST.in'
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
with open('MANIFEST.in','w') as fd:
    for l in commands:
        fd.write(l)
        fd.write('\n')


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
LONG_DESCRIPTION = ["PYthon LOCKER or pylocker package provides a pythonic way to create ",
                    "locking mechanism that can be used for general purposes as well as ",
                    "for locking files upon reading or writing. ",
                    "The locking system works by creating and updating a general locking ",
                    "file anytime a lock is requested with a certain pass. Lock pass is ",
                    "used to specify the user who sets the lock and who can have access ",
                    "to whatever is locked. Any user who knows the lock pass can access ",
                    "whatever is locked."]
DESCRIPTION      = [ LONG_DESCRIPTION[0] + LONG_DESCRIPTION[1] + LONG_DESCRIPTION[2] ]


## get package info
PACKAGE_INFO={}
infoPath = '__pkginfo__.py'
with open(infoPath) as fd:
    exec(fd.read(), PACKAGE_INFO)

# create meta data
metadata = dict(name = PACKAGE_NAME,
                packages=[PACKAGE_NAME],
                package_dir={PACKAGE_NAME: '.'},
                version= PACKAGE_INFO['__version__'] ,
                author="Bachir AOUN",
                author_email="bachir.aoun@e-aoun.com",
                description = "\n".join(DESCRIPTION),
                long_description = "\n".join(LONG_DESCRIPTION),
                url = "https://bachiraoun.github.io/pylocker/",
                download_url = "https://github.com/bachiraoun/pylocker",
                license = 'GNU',
                classifiers=[_f for _f in CLASSIFIERS.split('\n') if _f],
                platforms = ["Windows", "Linux", "Mac OS-X", "Unix"],
                python_requires = ">=3.9",
                install_requires = ["psutil"],
                extras_require = {"msgpack": ["msgpack"]})

# setup
setup(**metadata)
