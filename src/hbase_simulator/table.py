from hbase_simulator.hfile import HFile
import os

class Table:
    def __init__(self, name):
        self.name = name
        self.hfile = HFile(name)
        self.hfile.load()
        self.enabled = True

    def put(self, row_key, column_family, column, value):
        if not self.enabled:
            raise Exception("Table is disabled")
        self.hfile.put(row_key, column_family, column, value)
        self.hfile.save()

    def get(self, row_key):
        if not self.enabled:
            raise Exception("Table is disabled")
        return self.hfile.get(row_key)

    def delete(self, row_key):
        if not self.enabled:
            raise Exception("Table is disabled")
        self.hfile.delete(row_key)
        self.hfile.save()

    def scan(self):
        if not self.enabled:
            raise Exception("Table is disabled")
        return self.hfile.scan()

    def delete_all(self):
        if not self.enabled:
            raise Exception("Table is disabled")
        self.hfile.delete_all()
        self.hfile.save()

    def count(self):
        if not self.enabled:
            raise Exception("Table is disabled")
        return self.hfile.count()

    def truncate(self):
        if not self.enabled:
            raise Exception("Table is disabled")
        self.hfile.truncate()

    def describe(self):
        return f'Table: {self.name}, Enabled: {self.enabled}'

    def disable(self):
        self.enabled = False

    def enable(self):
        self.enabled = True

    def is_enabled(self):
        return self.enabled

class HBaseSimulator:
    def __init__(self):
        self.tables = {}
        self.load_tables()

    def load_tables(self):
        if not os.path.exists('data'):
            os.makedirs('data')
        for file in os.listdir('data'):
            if file.endswith('.hfile'):
                table_name = file.replace('.hfile', '')
                self.tables[table_name] = Table(table_name)
                print(f"Tabla '{table_name}' cargada desde HFile.")

    def create_table(self, name):
        if name in self.tables:
            raise Exception("Table already exists")
        self.tables[name] = Table(name)

    def list_tables(self):
        return list(self.tables.keys())

    def drop_table(self, name):
        if name in self.tables:
            del self.tables[name]
            os.remove(os.path.join('data', f'{name}.hfile'))

    def drop_all_tables(self):
        for name in list(self.tables.keys()):
            self.drop_table(name)

    def alter_table(self, name, new_name):
        if name in self.tables:
            self.tables[new_name] = self.tables.pop(name)
            self.tables[new_name].name = new_name
            os.rename(os.path.join('data', f'{name}.hfile'), os.path.join('data', f'{new_name}.hfile'))
