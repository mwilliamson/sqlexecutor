class Result(object):
    def __init__(self, query, error, table):
        self.query = query
        self.error = error
        self.table = table


class ResultTable(object):
    def __init__(self, column_names, rows):
        self.column_names = column_names
        self.rows = rows
