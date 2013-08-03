__all__ = ["executor"]


import sqlite3
import contextlib

from .mysqlexecutor import MySqlDialect
from .results import ResultTable, Result


def executor(name):
    return QueryExecutor(_dialects[name]())


class QueryExecutor(object):
    def __init__(self, dialect):
        self._dialect = dialect
        
    def execute(self, creation_script, query):
        if not query:
            return Result(query=query, error="Query is empty", table=None)
            
        with self._dialect.connect() as connection:
            cursor = connection.cursor()
            for statement in creation_script:
                cursor.execute(statement)
            
            try:
                cursor.execute(query)
            except self._dialect.DatabaseError as error:
                error_message = self._dialect.error_message(error)
                return Result(query=query, error=error_message, table=None)
            
            column_names = [
                column[0]
                for column in cursor.description
            ]
            
            rows = map(list, cursor.fetchall())
            
            table = ResultTable(column_names, rows)
            
            return Result(
                query=query,
                error=None,
                table=table,
            )


class Sqlite3Dialect(object):
    DatabaseError = sqlite3.Error
    
    @contextlib.contextmanager
    def connect(self):
        yield sqlite3.connect(":memory:")
        
    def error_message(self, error):
        return error.message


_dialects = {
    "sqlite3": Sqlite3Dialect,
    "mysql": MySqlDialect,
}
