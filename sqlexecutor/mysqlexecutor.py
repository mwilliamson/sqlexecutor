import os
import contextlib
import subprocess
import time
import uuid

import MySQLdb
import spur

from .results import ResultTable, Result
from .tempdir import create_temporary_dir


_local = spur.LocalShell()

class MySqlDialect(object):
    DatabaseError = MySQLdb.MySQLError
    
    @contextlib.contextmanager
    def connect(self):
        with self._install_mysql() as mysql_server:
            connection = mysql_server.connect()
            try:
                yield connection
            finally:
                connection.close()

    def error_message(self, error):
        return error[1]
    
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
                server = MySqlServer(socket_path=socket_path, root_password="")
                connection = _retry(
                    lambda: server.connect_as_root(),
                    MySQLdb.MySQLError,
                    timeout=10, interval=0.2
                )
                try:
                    root_password = str(uuid.uuid4())
                    cursor = connection.cursor()
                    cursor.execute("SET PASSWORD = PASSWORD(%s)", (root_password,))
                finally:
                    connection.close()
                yield MySqlServer(socket_path=socket_path, root_password=root_password)
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
    def __init__(self, socket_path, root_password):
        self._socket_path = socket_path
        self._root_password = root_password

    def connect(self):
        return self.connect_as_root()

    def connect_as_root(self):
        return self._connect_as_user("root", self._root_password)

    def _connect_as_user(self, username, password):
        return MySQLdb.connect(
            host="localhost",
            user=username,
            passwd=self._root_password,
            db="test",
            unix_socket=self._socket_path
        )


def _retry(func, error_cls, timeout, interval):
    start_time = time.time()
    while True:
        try:
            return func()
        except error_cls as error:
            if time.time() - start_time > timeout:
                raise
            else:
                time.sleep(interval)
