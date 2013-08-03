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
    
    def error_message(self, error):
        return error[1]
    
    def start_server(self):
        mysql_install_path = self._download_mysql()
        temp_dir = create_temporary_dir()
        try:
            socket_path = os.path.join(temp_dir.path, "mysql.sock")
            port = 55555
            mysqld_args = self._mysqld_args(temp_dir.path, socket_path, port)
            
            _local.run(
                ["scripts/mysql_install_db"] + mysqld_args,
                cwd=mysql_install_path,
            )
            
            mysql_process = _local.spawn(
                ["bin/mysqld"] + mysqld_args,
                cwd=mysql_install_path,
                store_pid=True,
                allow_error=True,
            )
            
            server = MySqlServer(
                process=mysql_process,
                temp_dir=temp_dir,
                socket_path=socket_path,
                root_password="",
            )
        except:
            temp_dir.close()
            raise
        try:
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
                
            server._root_password = root_password
            
            return server
        except:
            server.close()
            raise
    
    def _mysqld_args(self, temp_path, socket_path, port):
        data_dir = os.path.join(temp_path, "data")
        pid_file = os.path.join(temp_path, "mysql.pid")
        return [
            "--no-defaults",
            "--basedir=.",
            "--datadir={0}".format(data_dir),
            "--port={0}".format(port),
            "--socket={0}".format(socket_path),
            "--pid-file={0}".format(pid_file),
        ]
        
    
    def _download_mysql(self):
        install_dir = os.path.join(self._downloads_dir(), "mysql-5.6.13")
        if not os.path.exists(install_dir):
            os.makedirs(install_dir)
            url = "http://dev.mysql.com/get/Downloads/MySQL-5.6/mysql-5.6.13-linux-glibc2.5-x86_64.tar.gz/from/http://cdn.mysql.com/"
            path = self._download("mysql-5.6.13.tar.gz", url)
            _local.run(["tar", "xzf", path, "--directory", install_dir, "--strip-components=1"])
            _local.run(["chmod", "-R", "-w", install_dir])
        return install_dir
    
    def _download(self, name, url):
        with create_temporary_dir() as mysql_install_dir:
            # TODO: concurrent access
            tarball_path = os.path.join(self._downloads_dir(), name)
            if not os.path.exists(os.path.dirname(tarball_path)):
                os.makedirs(os.path.dirname(tarball_path))
            
            # TODO: check hash
            if not os.path.exists(tarball_path):
                subprocess.check_call(["curl", url, "--output", tarball_path, "--location", "--fail"])
                
            return tarball_path
            
    def _downloads_dir(self):
        return os.path.join(os.path.dirname(__file__), "downloads")


class MySqlServer(object):
    def __init__(self, process, temp_dir, socket_path, root_password):
        self._process = process
        self._temp_dir = temp_dir
        self._socket_path = socket_path
        self._root_password = root_password

    def connect(self):
        database_name = str(uuid.uuid4()).replace("-", "")[:16]
        password = str(uuid.uuid4())
        connection = self.connect_as_root()
        try:
            cursor = connection.cursor()
            cursor.execute("CREATE DATABASE `{0}`".format(database_name))
            cursor.execute(
                "GRANT ALL PRIVILEGES ON `{0}`.* TO %s@'localhost' IDENTIFIED BY %s".format(database_name),
                (database_name, password,)
            )
            # TODO: tidy up user
            return self._connect_as_user(
                username=database_name,
                password=password,
                database=database_name,
            )
        finally:
            connection.close()

    def connect_as_root(self):
        return self._connect_as_user("root", self._root_password)

    def _connect_as_user(self, username, password, database=None):
        connect_kwargs = {
            "host": "localhost",
            "user": username,
            "passwd": password,
            "unix_socket": self._socket_path,
        }
        if database is not None:
            connect_kwargs["db"] = database
        return MySQLdb.connect(**connect_kwargs)
        
    def close(self):
        self._process.send_signal(15)
        self._process.wait_for_result()
        self._temp_dir.close()


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
