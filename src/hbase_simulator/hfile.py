import json
import os
import time
from collections import OrderedDict

class HFile:
    def __init__(self, table_name, column_families=None):
        self.table_name = table_name
        self.column_families = column_families or []
        self.data = OrderedDict()
        self.file_path = os.path.join('data', f'{self.table_name}.hfile')
        self.versions = 3

        # Crear la carpeta 'data' si no existe
        os.makedirs('data', exist_ok=True)

    def put(self, row_key, column_family, column, value):
        timestamp = self._current_timestamp()
        if row_key not in self.data:
            self.data[row_key] = {}
        if column_family not in self.data[row_key]:
            self.data[row_key][column_family] = {}
        if column not in self.data[row_key][column_family]:
            self.data[row_key][column_family][column] = []
        # If version limit is reached, remove oldest version before insertion
        if len(self.data[row_key][column_family][column]) == self.versions:
            self.data[row_key][column_family][column].pop(0)
        self.data[row_key][column_family][column].append({'value': value, 'timestamp': timestamp})
            
        # Re-sort row keys after update
        self.data = OrderedDict(sorted(self.data.items()))

    def get(self, row_key):
        return self.data.get(row_key, {})

    def delete(self, row_key):
        if row_key in self.data:
            del self.data[row_key]

    def delete_column(self, row_key, column_family, column):
        if row_key in self.data and column_family in self.data[row_key] and column in self.data[row_key][column_family]:
            del self.data[row_key][column_family][column]

    def scan(self):
        return self.data.items()

    def delete_all(self):
        self.data = {}

    def count(self):
        return len(self.data)

    def truncate(self):
        self.delete_all()
        self.save()

    def _current_timestamp(self):
        return int(time.time() * 1000)

    def save(self):
        with open(self.file_path, 'w') as f:
            # Sort row keys before saving to hfile
            data_to_save = {
                'column_families': self.column_families,
                'data': OrderedDict(sorted(self.data.items()))
            }
            json.dump(data_to_save, f)

    def load(self):
        if os.path.exists(self.file_path):
            with open(self.file_path, 'r') as f:
                loaded_data = json.load(f, object_pairs_hook=OrderedDict)
                self.column_families = loaded_data.get('column_families', [])
                self.data = loaded_data.get('data', OrderedDict())
