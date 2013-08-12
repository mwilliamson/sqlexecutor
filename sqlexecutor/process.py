import sys

import msgpack

import sqlexecutor


def main():
    dialect_name, working_dir = sys.argv[1:]
    
    executor = sqlexecutor.executor(dialect_name, working_dir)
    try:
        for message in msgpack.Unpacker(sys.stdin, read_size=1):
            command = message[0]
            args = message[1:]
            
            if command == "execute":
                (creation_sql, query, ) = args
                result = executor.execute(creation_sql, query)
                if result.table is None:
                    column_names = None
                    rows = None
                else:
                    column_names = result.table.column_names
                    rows = result.table.rows
                
                
                msgpack.dump((result.error, column_names, rows), sys.stdout)
                sys.stdout.flush()
            else:
                return
            
    finally:
        executor.close()
    
    
if __name__ == "__main__":
    main()
