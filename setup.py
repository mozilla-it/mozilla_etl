# Generated by Medikit 0.6.3 on 2018-10-03.
# All changes will be overriden.
# Edit Projectfile and run "make update" (or "medikit update") to regenerate.

from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Py3 compatibility hacks, borrowed from IPython.
try:
    execfile
except NameError:

    def execfile(fname, globs, locs=None):
        locs = locs or globs
        exec(compile(open(fname).read(), fname, "exec"), globs, locs)


# Get the long description from the README file
try:
    with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
        long_description = f.read()
except:
    long_description = ''

# Get the classifiers from the classifiers file
tolines = lambda c: list(filter(None, map(lambda s: s.strip(), c.split('\n'))))
try:
    with open(path.join(here, 'classifiers.txt'), encoding='utf-8') as f:
        classifiers = tolines(f.read())
except:
    classifiers = []

version_ns = {}
try:
    execfile(path.join(here, 'mozilla_etl/_version.py'), version_ns)
except EnvironmentError:
    version = 'dev'
else:
    version = version_ns.get('__version__', 'dev')

setup(
    author='Philippe M. Chiasson',
    author_email='gozer@mozilla.com',
    description='Mozilla ETL Jobs',
    license='MPL, Version 2.0',
    name='mozilla_etl',
    version=version,
    long_description=long_description,
    classifiers=classifiers,
    packages=find_packages(exclude=['ez_setup', 'example', 'test']),
    include_package_data=True,
    install_requires=[
        'bonobo==0.6.4',
        'bonobo-sqlalchemy==0.6.1',
        'dateparser==0.7.0',
        'fs-s3fs==0.1.9',
        'fs-gcsfs==0.4.1',
        'fs.sshfs==0.8.0',
        'fs==2.0.27',
        'google-api-python-client==1.7.4',
        'google-auth-oauthlib==0.3.0',
        'lxml==4.2.5',
        'mysqlclient==1.3.13',
        'oauth2client==4.1.3',
        'pendulum==1.4.4',
        'psycopg2-binary==2.7.5',
        'psycopg2==2.7.5',
        'requests==2.20.0',
        'six==1.10.0',
        'sqlalchemy-vertica-python==0.3.4',
        'sqlalchemy-redshift==0.7.1',
        'untangle==1.1.1',
        'vertica-python==0.8.0',
        'zeep==3.1.0',
    ],
    extras_require={'dev': []},
    url='',
    download_url=''.format(version=version),
)
