__all__ = ["executor"]


import sqlite3
import contextlib

from .mysqlexecutor import MySqlDialect
from .results import ResultTable, Result


def prepare(name):
    dialect = _dialects[name]()
    dialect.prepare()
    

def executor(name):
    dialect = _dialects[name]()
    server = dialect.start_server()
    return QueryExecutor(dialect, server)


class QueryExecutor(object):
    def __init__(self, dialect, server):
        self._dialect = dialect
        self._server = server
        
    def execute(self, creation_script, query):
        if not query:
            return Result(query=query, error="Query is empty", table=None)
            
        connection = self._server.connect()
        try:
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
        finally:
            connection.close()
            
    def close(self):
        self._server.close()


class Sqlite3Dialect(object):
    DatabaseError = sqlite3.Error
    
    def start_server(self):
        return Sqlite3Server()
        
    def error_message(self, error):
        return error.message


class Sqlite3Server(object):
    def connect(self):
        return sqlite3.connect(":memory:")


_dialects = {
    "sqlite3": Sqlite3Dialect,
    "mysql": MySqlDialect,
}
