import json
import os
import time

class HFile:
    def __init__(self, table_name):
        self.table_name = table_name
        self.data = {}
        self.file_path = os.path.join('data', f'{self.table_name}.hfile')

        # Crear la carpeta 'data' si no existe
        os.makedirs('data', exist_ok=True)

    def put(self, row_key, column_family, column, value):
        timestamp = self._current_timestamp()
        if row_key not in self.data:
            self.data[row_key] = {}
        if column_family not in self.data[row_key]:
            self.data[row_key][column_family] = {}
        self.data[row_key][column_family][column] = {'value': value, 'timestamp': timestamp}

    def get(self, row_key):
        return self.data.get(row_key, {})

    def delete(self, row_key):
        if row_key in self.data:
            del self.data[row_key]

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
            json.dump(self.data, f)

    def load(self):
        if os.path.exists(self.file_path):
            with open(self.file_path, 'r') as f:
                self.data = json.load(f)
