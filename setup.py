#!/usr/bin/env python
from setuptools import setup, find_packages
from measuring_tools import __version__

setup(
    name='measuring-tools',
    version=__version__,
    packages=find_packages(),
    license='GNU GPL3',
    author='GISCE-TI, S.L.',
    author_email='devel@gisce.net',
    description='Tools for measurement'
)
