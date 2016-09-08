#!/usr/bin/env python

from setuptools import setup, find_packages
from pip.req import parse_requirements
from distutils.util import convert_path
import codecs
import sys

# Load version information
main_ns = {}
ver_path = convert_path('twistes/version.py')
with codecs.open(ver_path, 'rb', 'utf8') as ver_file:
    exec (ver_file.read(), main_ns)

install_requires = ['twisted', 'treq']

if sys.version_info < (2, 7):
    # python 2.6 isn't supported
    raise RuntimeError('This version requires Python 2.7+')

setup(
    name='twistes',
    version=main_ns['__version__'],
    author='Avihad Menahem',
    author_email='avihad87@gmail.com',
    description='Asynchronous elastic search client using Twisted and Treq',
    license='Apache License Version 2.0',
    platforms=['Any'],
    keywords=[
        'elasticsearch', 'async', 'asynchronous', 'twisted', 'treq'
    ],
    url='https://github.com/avihad/twistes',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Software Development :: Testing'
    ],
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    download_url='https://github.com/avihad/twistes/tarball/v{version}'.format(version=main_ns['__version__']),
    zip_safe=False,
    include_package_data=True,
    install_requires=install_requires,
    test_suite='tests'
)
