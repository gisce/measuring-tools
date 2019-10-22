#!/usr/bin/env python
from setuptools import setup, find_packages
from measuring_tools import __version__

with open('requirements.txt') as f:
    data = f.read()
reqs = data.split()

setup(
    name='measuring-tools',
    version=__version__,
    packages=find_packages(),
    install_requires=reqs,
    license='GNU GPL3',
    author='GISCE-TI, S.L.',
    author_email='devel@gisce.net',
    description='Tools for measurement'
)
