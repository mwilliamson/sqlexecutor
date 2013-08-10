__all__ = ["prepare", "executor"]


import os
import sqlite3
import contextlib

from .mysqlexecutor import MySqlDialect
from .results import ResultTable, Result


def prepare(name, working_dir):
    dialect = _get_dialect(name, working_dir)
    dialect.prepare()
    

def executor(name, working_dir):
    dialect = _get_dialect(name, working_dir)
    server = dialect.start_server()
    return QueryExecutor(dialect, server)


def _get_dialect(name, working_dir):
    if working_dir is not None:
        working_dir = os.path.join(working_dir, name)
    return _dialects[name](working_dir)


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
                error_message = connection.error_message(error)
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
    
    def __init__(self, working_dir):
        pass
    
    def start_server(self):
        return Sqlite3Server()
        
    def prepare(self):
        pass


class Sqlite3Server(object):
    def connect(self):
        return Sqlite3Connection(sqlite3.connect(":memory:"))
        
    def close(self):
        pass


class Sqlite3Connection(object):
    def __init__(self, connection):
        self.close = connection.close
        self.cursor = connection.cursor
    
    def error_message(self, error):
        return error.message


_dialects = {
    "sqlite3": Sqlite3Dialect,
    "mysql": MySqlDialect,
}
