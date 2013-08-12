__all__ = ["prepare", "executor"]


import sys
import os
import sqlite3
import subprocess
import threading
import time

import msgpack

from .mysqlexecutor import MySqlDialect
from .results import ResultTable, Result


def prepare(name, working_dir):
    dialect = _get_dialect(name, working_dir)
    dialect.prepare()
    

def executor(name, working_dir):
    dialect = _get_dialect(name, working_dir)
    server = dialect.start_server()
    return QueryExecutor(dialect, server)


def subprocess_executor(name, working_dir):
    return RestartingSubprocessQueryExecutor(name, working_dir)


def _get_dialect(name, working_dir):
    if working_dir is not None:
        working_dir = os.path.join(working_dir, name)
    return _dialects[name](working_dir)


class RestartingSubprocessQueryExecutor(object):
    def __init__(self, dialect_name, working_dir):
        self._dialect_name = dialect_name
        self._working_dir = working_dir
        self._executor = None
        
    def execute(self, creation_sql, query):
        self._start_executor()
        try:
            return self._executor.execute(creation_sql, query)
        except QueryTimeoutException:
            self._executor.close()
            self._executor = None
            return Result(query=query, error="The query took too long to finish", table=None)
    
    def close(self):
        if self._executor is not None:
            self._executor.close()
        
    def _start_executor(self):
        if self._executor is not None:
            return
        
        script_path = os.path.join(os.path.dirname(__file__), "process.py")
        
        process = subprocess.Popen(
            [
                sys.executable,
                script_path,
                self._dialect_name,
                os.path.abspath(self._working_dir),
            ],
            
            stdout=subprocess.PIPE,
            stderr=sys.stderr,
            stdin=subprocess.PIPE,
            
            # Create a new process group
            preexec_fn=os.setpgrp,
        )
        try:
            line = process.stdout.readline()
            if line != "Ready\n":
                raise Exception("Could not start executor" + line)
            
            self._executor = SubprocessQueryExecutor(process)
        except:
            process.terminate()
            raise


class SubprocessQueryExecutor(object):
    def __init__(self, process):
        self._process = process
        self._receiver = msgpack.Unpacker(self._process.stdout, read_size=1)
        
    def execute(self, creation_sql, query):
        self._send_command("execute", creation_sql, query)
        (error, column_names, rows) = self._receive()
        
        if column_names is None:
            table = None
        else:
            table = ResultTable(column_names, rows)
        
        return Result(
            query=query,
            error=error,
            table=table
        )
        
    def close(self):
        try:
            subprocess.check_call(["kill", "--", "-{0}".format(self._process.pid)])
            
            def really_kill_it():
                time.sleep(10)
                subprocess.call(["kill", "-KILL", "--", "-{0}".format(self._process.pid)])
                
            thread = threading.Thread(target=really_kill_it)
            thread.start()
                
            
        except IOError:
            # Probably already dead
            pass
        
    def _send_command(self, *args):
        msgpack.dump(args, self._process.stdin)
        self._process.stdin.flush()
        
    def _receive(self):
        result = [None]
        
        def run():
            try:
                result[0] = next(self._receiver)
            except StopIteration:
                result[0] = None
        
        thread = threading.Thread(target=run)
        thread.start()
        thread.join(2)
        if thread.isAlive():
            raise QueryTimeoutException()
        else:
            return result[0]


class QueryTimeoutException(Exception):
    pass
    

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
    
