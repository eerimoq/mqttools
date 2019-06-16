#!/usr/bin/env python3

import re
import setuptools
from setuptools import find_packages


def find_version():
    return re.search(r"^__version__ = '(.*)'$",
                     open('mqttools/version.py', 'r').read(),
                     re.MULTILINE).group(1)


setuptools.setup(
    name='mqttools',
    version=find_version(),
    description='MQTT tools.',
    long_description=open('README.rst', 'r').read(),
    author='Erik Moqvist',
    author_email='erik.moqvist@gmail.com',
    license='MIT',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
    ],
    keywords=['mqtt'],
    url='https://github.com/eerimoq/mqttools',
    packages=find_packages(exclude=['tests']),
    install_requires=[
        'bitstruct',
        'humanfriendly',
        'windows-curses;platform_system=="Windows"'
    ],
    test_suite="tests",
    entry_points={
        'console_scripts': ['mqttools=mqttools.__init__:main']
    })
