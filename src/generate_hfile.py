import json
import os
import time
from collections import OrderedDict

class HFile:
    def __init__(self, table_name, column_families):
        self.table_name = table_name
        self.column_families = column_families
        self.data = OrderedDict()
        self.file_path = os.path.join('data', f'{self.table_name}.hfile')

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
        self.data[row_key][column_family][column].append({'value': value, 'timestamp': timestamp})
        # Re-sort row keys after update
        self.data = OrderedDict(sorted(self.data.items()))

    def save(self):
        with open(self.file_path, 'w') as f:
            sorted_data = OrderedDict(sorted(self.data.items()))
            json.dump({"column_families": self.column_families, "data": sorted_data}, f)

    def _current_timestamp(self):
        return int(time.time() * 1000)

def generate_hfile(table_name, num_records):
    column_families = ["personal_info", "account_data"]
    hfile = HFile(table_name, column_families)

    for i in range(1, num_records + 1):
        row_key = f"usuario_{i}"
        hfile.put(row_key, "personal_info", "name", f"Usuario {i}")
        hfile.put(row_key, "personal_info", "email", f"usuario_{i}@example.com")
        hfile.put(row_key, "account_data", "balance", f"{i * 10}")
        hfile.put(row_key, "account_data", "status", "active" if i % 2 == 0 else "inactive")

    hfile.save()
    print(f"HFile for table '{table_name}' with {num_records} records created successfully.")

if __name__ == "__main__":
    generate_hfile("users_large", 1000)
