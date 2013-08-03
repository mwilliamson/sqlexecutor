__all__ = ["executor"]


import sqlite3

from .mysqlexecutor import MySqlExecutor
from .results import ResultTable, Result


def executor(name):
    return _executors[name]()
    
    
class Sqlite3Executor(object):
    def execute(self, creation_script, query):
        if not query:
            return Result(query=query, error="Query is empty", table=None)
            
        connection = sqlite3.connect(":memory:")
        cursor = connection.cursor()
        cursor.executescript(creation_script)
        
        try:
            cursor.execute(query)
        except sqlite3.Error as error:
            return Result(query=query, error=error.message, table=None)
        
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


_executors = {
    "sqlite3": Sqlite3Executor,
    "mysql": MySqlExecutor,
}
