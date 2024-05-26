from hbase_simulator.table import HBaseSimulator

def run_cli():
    simulator = HBaseSimulator()

    print("Bienvenido al simulador de HBase. Ingrese 'exit' para salir.")

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
                if len(args) != 2:
                    print("Uso: create <table_name>")
                else:
                    table_name = args[1]
                    simulator.create_table(table_name)
                    print(f"Tabla '{table_name}' creada.")
            
            elif cmd == "list":
                tables = simulator.list_tables()
                if tables:
                    print("Tablas:", ", ".join(tables))
                else:
                    print("No hay tablas.")

            elif cmd == "put":
                if len(args) != 6:
                    print("Uso: put <table_name> <row_key> <column_family> <column> <value>")
                else:
                    table_name, row_key, column_family, column, value = args[1:6]
                    if table_name in simulator.tables:
                        table = simulator.tables[table_name]
                        table.put(row_key, column_family, column, value)
                        print(f"Datos insertados en '{table_name}': {row_key}, {column_family}:{column} = {value}")
                    else:
                        print(f"Tabla '{table_name}' no encontrada.")

            elif cmd == "get":
                if len(args) != 3:
                    print("Uso: get <table_name> <row_key>")
                else:
                    table_name, row_key = args[1:3]
                    if table_name in simulator.tables:
                        table = simulator.tables[table_name]
                        data = table.get(row_key)
                        if data:
                            print(f"Datos en '{row_key}' de '{table_name}': {data}")
                        else:
                            print(f"No se encontraron datos en '{row_key}' de '{table_name}'.")
                    else:
                        print(f"Tabla '{table_name}' no encontrada.")

            elif cmd == "delete":
                if len(args) != 3:
                    print("Uso: delete <table_name> <row_key>")
                else:
                    table_name, row_key = args[1:3]
                    if table_name in simulator.tables:
                        table = simulator.tables[table_name]
                        table.delete(row_key)
                        print(f"Fila '{row_key}' eliminada de '{table_name}'.")
                    else:
                        print(f"Tabla '{table_name}' no encontrada.")
            
            else:
                print("Comando no reconocido. Comandos disponibles: create, list, put, get, delete, exit")

        except Exception as e:
            print(f"Error: {e}")
