from hbase_simulator.table import HBaseSimulator
from hbase_simulator.utils import scan_to_dataframe

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
                if len(args) == 3:
                    table_name, row_key = args[1:3]
                    if table_name in simulator.tables:
                        table = simulator.tables[table_name]
                        table.delete(row_key)
                        print(f"Row '{row_key}' deleted from '{table_name}'.")
                    else:
                        print(f"Table '{table_name}' not found.")
                elif len(args) == 4:
                    table_name, row_key, cf_col = args[1:4]
                    column_family, column = cf_col.split(":")
                    if table_name in simulator.tables:
                        table = simulator.tables[table_name]
                        table.delete(row_key, column_family, column)
                        print(f"Column '{column_family}:{column}' deleted from row '{row_key}' in '{table_name}'.")
                    else:
                        print(f"Table '{table_name}' not found.")
                else:
                    print("Usage: delete <table_name> <row_key> [<column_family:column>]")

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

            elif cmd == "deleteall":
                if len(args) != 2:
                    print("Usage: deleteall <table_name>")
                else:
                    table_name = args[1]
                    if table_name in simulator.tables:
                        table = simulator.tables[table_name]
                        table.delete_all()
                        print(f"All data on table '{table_name}' has been deleted.")
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
                        table.truncate()
                        print(f"Table '{table_name}' truncated.")
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
                if len(args) != 3:
                    print("Usage: alter <table_name> <new_table_name>")
                else:
                    table_name, new_table_name = args[1:3]
                    simulator.alter_table(table_name, new_table_name)
                    print(f"Table '{table_name}' renamed to '{new_table_name}'.")

            elif cmd == "drop":
                if len(args) != 2:
                    print("Usage: drop <table_name>")
                else:
                    table_name = args[1]
                    if table_name in simulator.tables:
                        simulator.drop_table(table_name)
                        print(f"Table '{table_name}' dropped.")
                    else:
                        print(f"Table '{table_name}' not found.")
            
            elif cmd == "dropall":
                if len(args) != 1:
                    print("Usage: dropall")
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
                print("Unknown command. Available commands: create, list, put, get, delete, scan, deleteall, count, truncate, disable, is_enabled, alter, drop, dropall, describe, exit")

        except Exception as e:
            print(f"An error ocurred on executing command: '{command}'")
            print(f"Error: {e.with_traceback()}")
