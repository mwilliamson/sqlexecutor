__all__ = ["executor"]


import sqlite3


def executor(name):
    return Sqlite3Executor()
    
    
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
        
        
class Result(object):
    def __init__(self, query, error, table):
        self.query = query
        self.error = error
        self.table = table


class ResultTable(object):
    def __init__(self, column_names, rows):
        self.column_names = column_names
        self.rows = rows
