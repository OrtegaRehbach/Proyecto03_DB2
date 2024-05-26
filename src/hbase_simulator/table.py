from hbase_simulator.hfile import HFile

class Table:
    def __init__(self, name):
        self.name = name
        self.hfile = HFile(name)
        self.hfile.load()

    def put(self, row_key, column_family, column, value):
        self.hfile.put(row_key, column_family, column, value)
        self.hfile.save()

    def get(self, row_key):
        return self.hfile.get(row_key)

    def delete(self, row_key):
        self.hfile.delete(row_key)
        self.hfile.save()

    def describe(self):
        return f'Table: {self.name}'

class HBaseSimulator:
    def __init__(self):
        self.tables = {}

    def create_table(self, name):
        if name in self.tables:
            raise Exception("Table already exists")
        self.tables[name] = Table(name)

    def list_tables(self):
        return list(self.tables.keys())
