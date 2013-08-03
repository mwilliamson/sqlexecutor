import os

from nose.tools import istest, assert_equal, assert_regexp_matches

import sqlexecutor
from sqlexecutor import tempdir


@istest
class MySqlTests(object):
    @classmethod
    def setup_class(cls):
        if os.environ.get("FULL_TEST", False):
            cls._working_dir = tempdir.create_temporary_dir()
            working_dir_path = cls._working_dir.path
        else:
            cls._working_dir = None
            working_dir_path = os.path.join(os.path.dirname(__file__), "../_tests-working-dir")
        sqlexecutor.prepare("mysql", working_dir=working_dir_path)
        cls._executor = sqlexecutor.executor("mysql", working_dir=working_dir_path)
    
    @classmethod    
    def teardown_class(cls):
        try:
            cls._executor.close()
        finally:
            if cls._working_dir is not None:
                cls._working_dir.close()
    
    @istest
    def original_query_is_included_in_result(self):
        result = self._executor.execute(
            "",
            "SELECT 1"
        )
        assert_equal("SELECT 1", result.query)


    @istest
    def running_query_returns_result_with_column_names_and_rows(self):
        result = self._executor.execute(
            [
                """create table books (
                    title varchar(255),
                    author varchar(255)
                );""",
                """insert into books (title, author) values (
                    'Dirk Gently''s Holistic Detective Agency',
                    'Douglas Adams'
                );""",
                
                """insert into books (title, author) values (
                    'Orbiting the Giant Hairball',
                    'Gordon MacKenzie'
                );""",
            ],
            "SELECT * FROM books"
        )
        assert_equal(None, result.error)
        assert_equal(["title", "author"], result.table.column_names)
        assert_equal(
            [
                ["Dirk Gently's Holistic Detective Agency", "Douglas Adams"],
                ["Orbiting the Giant Hairball", "Gordon MacKenzie"],
            ],
            result.table.rows
        )


    @istest
    def query_results_in_error_if_query_is_empty(self):
        result = self._executor.execute(
            "",
            ""
        )
        assert_equal("Query is empty", result.error)



    @istest
    def query_results_in_error_if_query_is_malformed(self):
        result = self._executor.execute(
            "",
            "SELECTEROO"
        )
        assert_equal('You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near \'SELECTEROO\' at line 1', result.error)



    @istest
    def query_results_in_error_if_query_references_non_existant_table(self):
        result = self._executor.execute(
            "",
            "SELECT 1 FROM books"
        )
        assert_regexp_matches(result.error, r"Table '\S+.books' doesn't exist")
