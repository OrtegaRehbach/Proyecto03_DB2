from hbase_simulator.hfile import HFile
import os

class Table:
    def __init__(self, name, column_families=None):
        self.name = name
        self.column_families = column_families or []
        self.hfile = HFile(name, column_families)
        self.hfile.load()
        self.enabled = True

    def put(self, row_key, column_family, column, value):
        if not self.enabled:
            raise Exception("Table is disabled")
        if column_family not in self.column_families:
            raise Exception(f"Column family '{column_family}' does not exist")
        self.hfile.put(row_key, column_family, column, value)
        self.hfile.save()

    def get(self, row_key):
        if not self.enabled:
            raise Exception("Table is disabled")
        return self.hfile.get(row_key)

    def delete_all(self, row_key, column_family=None, column=None):
        if not self.enabled:
            raise Exception("Table is disabled")
        if column_family and column:
            self.hfile.delete_column(row_key, column_family, column)
        else:
            self.hfile.delete(row_key)
        self.hfile.save()
        
    def delete_cell(self, row_key, column_family, column, timestamp=None):
        if not self.enabled:
            raise Exception("Table is disabled")
        self.hfile.delete_cell(row_key, column_family, column, timestamp)

    def scan(self):
        if not self.enabled:
            raise Exception("Table is disabled")
        return self.hfile.scan()

    def count(self):
        if not self.enabled:
            raise Exception("Table is disabled")
        return self.hfile.count()

    def truncate(self):
        if self.enabled:
            raise Exception("Table must be disabled")
        self.hfile.truncate()

    def describe(self):
        return f'Table: {self.name}, Enabled: {self.enabled}, Column Families: {", ".join(self.column_families)}'

    def disable(self):
        self.enabled = False

    def enable(self):
        self.enabled = True

    def is_enabled(self):
        return self.enabled

    def add_column_family(self, column_family):
        if column_family in self.column_families:
            raise Exception(f"Column family '{column_family}' already exists")
        self.column_families.append(column_family)
        self.hfile.column_families = self.column_families
        self.hfile.save()

    def delete_column_family(self, column_family):
        if column_family not in self.column_families:
            raise Exception(f"Column family '{column_family}' does not exist")
        self.column_families.remove(column_family)
        for row_key, row_data in self.hfile.data.items():
            if column_family in row_data:
                del row_data[column_family]
        self.hfile.column_families = self.column_families
        self.hfile.save()

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
                temp_hfile = HFile(table_name)
                temp_hfile.load()
                self.tables[table_name] = Table(table_name, temp_hfile.column_families)
                print(f"Table '{table_name}' loaded from HFile.")

    def create_table(self, name, column_families):
        if name in self.tables:
            raise Exception("Table already exists")
        self.tables[name] = Table(name, column_families)

    def list_tables(self):
        return list(self.tables.keys())

    def drop_table(self, name):
        if name in self.tables:
            if self.tables[name].enabled:
                raise Exception("Table must be disabled before deletion")
            del self.tables[name]
            os.remove(os.path.join('data', f'{name}.hfile'))

    def drop_all_tables(self):
        for name in list(self.tables.keys()):
            self.tables[name].disable()
            self.drop_table(name)

    def alter_table_add_column_family(self, name, column_family):
        if name in self.tables:
            table = self.tables[name]
            table.add_column_family(column_family)
        else:
            raise Exception(f"Table '{name}' not found.")

    def alter_table_delete_column_family(self, name, column_family):
        if name in self.tables:
            table = self.tables[name]
            table.delete_column_family(column_family)
        else:
            raise Exception(f"Table '{name}' not found.")

