#!/usr/bin/env python

import os
from setuptools import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='sqlexecutor',
    version='0.1.1',
    description='Execute SQL queries in an isolated database',
    long_description=read("README"),
    author='Michael Williamson',
    url='http://github.com/mwilliamson/sqlexecutor',
    packages=['sqlexecutor'],
    install_requires=[
        "spur.local>=0.3.5,<0.4",
        "MySQL-python==1.2.4",
    ],
    license='BSD 2-Clause',
)
