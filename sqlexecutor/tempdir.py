import contextlib
import tempfile
import shutil


@contextlib.contextmanager
def create_temporary_dir():
    temporary_dir = tempfile.mkdtemp()
    try:
        yield temporary_dir
    finally:
        shutil.rmtree(temporary_dir)
