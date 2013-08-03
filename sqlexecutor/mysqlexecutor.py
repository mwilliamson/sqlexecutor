import os
import contextlib
import subprocess

import MySQLdb
import spur

from .results import ResultTable, Result
from .tempdir import create_temporary_dir


_local = spur.LocalShell()

class MySqlExecutor(object):
    def execute(self, creation_script, query):
        with self._install_mysql() as mysql_server:
            if not query:
                return Result(query=query, error="Query is empty", table=None)
                
            connection = MySQLdb.connect(host="localhost", user="root", passwd="", db="test", unix_socket=mysql_server.socket_path)
            cursor = connection.cursor()
            for statement in creation_script:
                cursor.execute(statement)
            
            try:
                cursor.execute(query)
            except MySQLdb.MySQLError as error:
                return Result(query=query, error=error[1], table=None)
            
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
    
    @contextlib.contextmanager
    def _install_mysql(self):
        with create_temporary_dir() as temp_dir:
            url = "http://dev.mysql.com/get/Downloads/MySQL-5.6/mysql-5.6.13-linux-glibc2.5-x86_64.tar.gz/from/http://cdn.mysql.com/"
            path = self._download("mysql-5.6.13", url)
            _local.run(["tar", "xzf", path, "--directory", temp_dir])
            mysql_install_path = os.path.join(temp_dir, "mysql-5.6.13-linux-glibc2.5-x86_64")
            
            port = 55555
            socket_path = os.path.join(mysql_install_path, "mysql.sock")
            
            _local.run(
                [
                    "scripts/mysql_install_db",
                    "--no-defaults",
                    "--basedir=.",
                    "--datadir=data",
                    "--port={0}".format(port),
                    "--socket={0}".format(socket_path),
                ],
                cwd=mysql_install_path,
            )
            
            mysql_process = _local.spawn(
                [
                    "bin/mysqld",
                    "--no-defaults",
                    "--basedir=.",
                    "--datadir=data",
                    "--port={0}".format(port),
                    "--socket={0}".format(socket_path),
                ],
                cwd=mysql_install_path,
                store_pid=True,
                allow_error=True,
            )
            try:
                import time
                time.sleep(5)
                yield MySqlServer(socket_path=socket_path)
            finally:
                mysql_process.send_signal(15)
                mysql_process.wait_for_result()
            
    def _download(self, name, url):
        with create_temporary_dir() as mysql_install_dir:
            # TODO: concurrent access
            tarball_path = os.path.join(os.path.dirname(__file__), "downloads", name)
            if not os.path.exists(os.path.dirname(tarball_path)):
                os.makedirs(os.path.dirname(tarball_path))
            
            # TODO: check hash
            if not os.path.exists(tarball_path):
                subprocess.check_call(["curl", url, "--output", tarball_path, "--location", "--fail"])
                
            return tarball_path


class MySqlServer(object):
    def __init__(self, socket_path):
        self.socket_path = socket_path
