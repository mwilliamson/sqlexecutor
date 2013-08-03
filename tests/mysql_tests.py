from nose.tools import istest, assert_equal

import sqlexecutor


@istest
class MySqlTests(object):
    @classmethod
    def setup_class(cls):
        cls._executor = sqlexecutor.executor("mysql")
    
    @classmethod    
    def teardown_class(cls):
        cls._executor.close()
    
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
        assert_equal("Table 'test.books' doesn't exist", result.error)
