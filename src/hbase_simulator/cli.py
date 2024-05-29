from hbase_simulator.table import HBaseSimulator
from hbase_simulator.utils import scan_to_dataframe
import time


def run_cli():
    simulator = HBaseSimulator()

    print("Welcome to HBase Shell simulator.")
    print("Enter 'exit' to exit.\n")

    while True:
        try:
            command = input("hbase> ").strip()
            if command.lower() == 'exit':
                break

            args = command.split()
            if not args:
                continue

            cmd = args[0]
            if cmd == "create":
                if len(args) < 3:
                    print("Usage: create <table_name> <column_family1> <column_family2> ...")
                else:
                    table_name = args[1]
                    column_families = args[2:]
                    simulator.create_table(table_name, column_families)
                    print(f"Table '{table_name}' created with column families: {', '.join(column_families)}.")
            
            elif cmd == "list":
                tables = simulator.list_tables()
                if tables:
                    print("Tables:")
                    for table in tables:
                        print(table)
                else:
                    print("No tables found.")

            elif cmd == "put":
                if len(args) != 6:
                    print("Usage: put <table_name> <row_key> <column_family> <column> <value>")
                else:
                    table_name, row_key, column_family, column, value = args[1:6]
                    if table_name in simulator.tables:
                        table = simulator.tables[table_name]
                        table.put(row_key, column_family, column, value)
                        print(f"Data inserted into '{table_name}': {row_key}, {column_family}:{column} = {value}")
                    else:
                        print(f"Table '{table_name}' not found.")

            elif cmd == "get":
                if len(args) != 3:
                    print("Usage: get <table_name> <row_key>")
                else:
                    table_name, row_key = args[1:3]
                    if table_name in simulator.tables:
                        table = simulator.tables[table_name]
                        row_data = table.get(row_key)
                        if row_data:
                            reformatted_data = [(row_key, row_data)]
                            df = scan_to_dataframe(reformatted_data, only_latest_version=True)
                            print(df)
                        else:
                            print(f"No data found at '{row_key}' in '{table_name}'.")
                    else:
                        print(f"Table '{table_name}' not found.")

            elif cmd == "delete":
                if len(args) == 5:
                    table_name, row_key, column_family, column = args[1:5]
                    if table_name in simulator.tables:  # Valid table
                        if simulator.tables[table_name].hfile.data.get(row_key):  # Valid row key
                            if simulator.tables[table_name].hfile.data[row_key].get(column_family):  # Valid column family
                                if simulator.tables[table_name].hfile.data[row_key][column_family].get(column):  # Valid column
                                    table = simulator.tables[table_name]
                                    table.delete_cell(row_key, column_family, column)
                                    print(f"Deleted cell at '{table_name}, {row_key}, {column_family}:{column}'.")
                                else:
                                    print(f"Column '{column}' not found.")
                            else:
                                print(f"Column family '{column_family}' not found.")
                        else:
                            print(f"Row key '{row_key}' not found.")
                    else:
                        print(f"Table '{table_name}' not found.")
                                                            
                elif len(args) == 6:
                    table_name, row_key, column_family, column, timestamp = args[1:6]
                    if table_name in simulator.tables:  # Valid table
                        if simulator.tables[table_name].hfile.data.get(row_key):  # Valid row key
                            if simulator.tables[table_name].hfile.data[row_key].get(column_family):  # Valid column family
                                if simulator.tables[table_name].hfile.data[row_key][column_family].get(column):  # Valid column
                                    cells = simulator.tables[table_name].hfile.data[row_key][column_family][column]
                                    valid_timestamp = any(str(cell.get("timestamp")) == timestamp for cell in cells)
                                    if valid_timestamp:
                                        table = simulator.tables[table_name]
                                        table.delete_cell(row_key, column_family, column, timestamp)
                                        print(f"Deleted cell at '{table_name}, {row_key}, {column_family}:{column} with timestamp: {timestamp}'.")
                                    else:
                                        print(f"Cell with timestamp '{timestamp}' not found.")
                                else:
                                    print(f"Column '{column}' not found.")
                            else:
                                print(f"Column family '{column_family}' not found.")
                        else:
                            print(f"Row key '{row_key}' not found.")
                    else:
                        print(f"Table '{table_name}' not found.")
                else:
                    print("Usage: delete <table_name> <row_key> <column_family> <column> [<timestamp>]")

            elif cmd == "scan":
                if len(args) != 2:
                    print("Usage: scan <table_name>")
                else:
                    table_name = args[1]
                    if table_name in simulator.tables:
                        table = simulator.tables[table_name]
                        data = table.scan()
                        if data:
                            df = scan_to_dataframe(data, only_latest_version=True)
                            print(df)
                        else:
                            print(f"No data found on table '{table_name}'.")
                    else:
                        print(f"Table '{table_name}' not found.")

            elif cmd == "delete_all":
                if len(args) != 3:
                    print("Usage: delete_all <table_name> <row_key>")
                else:
                    table_name, row_key = args[1:3]
                    if table_name in simulator.tables:
                        row_data = simulator.tables[table_name].get(row_key)
                        if row_data:
                            table = simulator.tables[table_name]
                            table.delete_all(row_key)
                            print(f"All data at row key '{row_key}' has been deleted.")
                        else:
                            print(f"No data found at '{row_key}' in '{table_name}'.")
                    else:
                        print(f"Table '{table_name}' not found.")

            elif cmd == "count":
                if len(args) != 2:
                    print("Usage: count <table_name>")
                else:
                    table_name = args[1]
                    if table_name in simulator.tables:
                        table = simulator.tables[table_name]
                        count = table.count()
                        print(f"Count of rows in '{table_name}': {count}")
                    else:
                        print(f"Table '{table_name}' not found.")

            elif cmd == "truncate":
                if len(args) != 2:
                    print("Usage: truncate <table_name>")
                else:
                    table_name = args[1]
                    if table_name in simulator.tables:
                        table = simulator.tables[table_name]
                        if table.enabled:
                            # Deshabilitar la tabla antes de truncarla
                            print("Truncating 'one' table: \n  - Disabling table... ")
                            table.disable()
                            print("  - Truncating table...")
                            table.truncate()
                            # Re-habilitar tabla
                            table.enable()
                        else:
                            print(f"Table is disabled.")
                    else:
                        print(f"Table '{table_name}' not found.")
                        
            elif cmd == "disable":
                if len(args) != 2:
                    print("Usage: disable <table_name>")
                else:
                    table_name = args[1]
                    if table_name in simulator.tables:
                        table = simulator.tables[table_name]
                        print(f"Disabling table '{table_name}'...")
                        table.disable()
                        print(f"Table '{table_name}' disabled.")
                    else:
                        print(f"Table '{table_name}' not found.")
            
            elif cmd == "enable":
                if len(args) != 2:
                    print("Usage: enable <table_name>")
                else:
                    table_name = args[1]
                    if table_name in simulator.tables:
                        table = simulator.tables[table_name]
                        print(f"Enabling table '{table_name}'...")
                        table.enable()
                        print(f"Table '{table_name}' enabled.")
                    else:
                        print(f"Table '{table_name}' not found.")

            elif cmd == "is_enabled":
                if len(args) != 2:
                    print("Usage: is_enabled <table_name>")
                else:
                    table_name = args[1]
                    if table_name in simulator.tables:
                        table = simulator.tables[table_name]
                        enabled = table.is_enabled()
                        print(f"Table '{table_name}' is {'enabled' if enabled else 'not enabled'}.")
                    else:
                        print(f"Table '{table_name}' not found.")

            elif cmd == "alter":
                if len(args) != 4:
                    print("Usage: alter <table_name>, NAME ⇒ '<new_column_family>', VERSIONS ⇒ <new_versions>")
                else:
                    table_name, col_family, new_versions = args[1:4]
                    if table_name in simulator.tables:
                        table = simulator.tables[table_name]
                        start_time = time.time()
                        print(f"hbase(main):003:0> alter '{table_name}', NAME ⇒ '{col_family}', VERSIONS ⇒ {new_versions}")
                        print("Updating all regions with the new schema...")
                        regions = table.get_regions()  # Obtenemos la lista de regiones de la tabla
                        for i, region in enumerate(regions):
                            print(f"{i}/{len(regions)} regions updated.")
                        print("Done.")
                        elapsed_time = time.time() - start_time
                        print(f"0 row(s) in {elapsed_time:.4f} seconds")
                    else:
                        print(f"Error: Table '{table_name}' not found.")


            elif cmd == "drop":
                if len(args) != 2:
                    print("Usage: drop <table_name>")
                else:
                    table_name = args[1]
                    if table_name in simulator.tables:
                        if not simulator.tables[table_name].enabled:
                            simulator.drop_table(table_name)
                            print(f"Table '{table_name}' dropped.")
                        else:
                            print("Table must be disabled before deletion.")
                    else:
                        print(f"Table '{table_name}' not found.")
            
            elif cmd == "drop_all":
                if len(args) != 1:
                    print("Usage: drop_all")
                else:
                    simulator.drop_all_tables()
                    print("All tables have been dropped.")
            
            elif cmd == "describe":
                if len(args) != 2:
                    print("Usage: describe <table_name>")
                else:
                    table_name = args[1]
                    if table_name in simulator.tables:
                        table = simulator.tables[table_name]
                        print(table.describe())
                    else:
                        print(f"Table '{table_name}' not found.")

            else:
                print("Unknown command. Available commands: create, list, put, get, delete, scan, delete_all, count, truncate, disable, enable, is_enabled, alter, drop, drop_all, describe, exit")

        except Exception as e:
            print(f"An error ocurred on executing command: '{command}'")
            print(f"Error: {e}")
            print(f"Traceback: {e.__traceback__}")
