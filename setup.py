#!/usr/bin/env python

import os
from setuptools import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='sqlexecutor',
    version='0.1.0',
    description='Execute SQL queries in an isolated database',
    long_description=read("README"),
    author='Michael Williamson',
    url='http://github.com/mwilliamson/sqlexecutor',
    packages=['sqlexecutor'],
    install_requires=[
    ],
)
