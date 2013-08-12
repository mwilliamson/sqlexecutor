import os

from nose.tools import istest, assert_equal, assert_regexp_matches

import sqlexecutor
from sqlexecutor import tempdir

import mysql_tests


# TODO: should create a proper base class, rather than overriding

@istest
class SubProcessTests(mysql_tests.MySqlTests):
    @classmethod
    def setup_class(cls):
        cls._executor = None
        
        working_dir_path = os.path.join(os.path.dirname(__file__), "../_tests-working-dir")
        sqlexecutor.prepare("mysql", working_dir=working_dir_path)
        cls._executor = sqlexecutor.subprocess_executor("mysql", working_dir=working_dir_path)
    
    @classmethod
    def teardown_class(cls):
        if cls._executor is not None:
            cls._executor.close()

