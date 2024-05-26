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

            elif cmd == "scan":
                if len(args) != 2:
                    print("Uso: scan <table_name>")
                else:
                    table_name = args[1]
                    if table_name in simulator.tables:
                        table = simulator.tables[table_name]
                        data = table.scan()
                        for row_key, row_data in data:
                            print(f"{row_key}: {row_data}")
                    else:
                        print(f"Tabla '{table_name}' no encontrada.")

            elif cmd == "deleteall":
                if len(args) != 2:
                    print("Uso: deleteall <table_name>")
                else:
                    table_name = args[1]
                    if table_name in simulator.tables:
                        table = simulator.tables[table_name]
                        table.delete_all()
                        print(f"Todos los datos en '{table_name}' han sido eliminados.")
                    else:
                        print(f"Tabla '{table_name}' no encontrada.")

            elif cmd == "count":
                if len(args) != 2:
                    print("Uso: count <table_name>")
                else:
                    table_name = args[1]
                    if table_name in simulator.tables:
                        table = simulator.tables[table_name]
                        count = table.count()
                        print(f"Cantidad de filas en '{table_name}': {count}")
                    else:
                        print(f"Tabla '{table_name}' no encontrada.")

            elif cmd == "truncate":
                if len(args) != 2:
                    print("Uso: truncate <table_name>")
                else:
                    table_name = args[1]
                    if table_name in simulator.tables:
                        table = simulator.tables[table_name]
                        table.truncate()
                        print(f"Tabla '{table_name}' truncada.")
                    else:
                        print(f"Tabla '{table_name}' no encontrada.")

            elif cmd == "disable":
                if len(args) != 2:
                    print("Uso: disable <table_name>")
                else:
                    table_name = args[1]
                    if table_name in simulator.tables:
                        table = simulator.tables[table_name]
                        table.disable()
                        print(f"Tabla '{table_name}' deshabilitada.")
                    else:
                        print(f"Tabla '{table_name}' no encontrada.")

            elif cmd == "is_enabled":
                if len(args) != 2:
                    print("Uso: is_enabled <table_name>")
                else:
                    table_name = args[1]
                    if table_name in simulator.tables:
                        table = simulator.tables[table_name]
                        enabled = table.is_enabled()
                        print(f"Tabla '{table_name}' {'está habilitada' if enabled else 'no está habilitada'}.")
                    else:
                        print(f"Tabla '{table_name}' no encontrada.")

            elif cmd == "alter":
                if len(args) != 3:
                    print("Uso: alter <table_name> <new_table_name>")
                else:
                    table_name, new_table_name = args[1:3]
                    simulator.alter_table(table_name, new_table_name)
                    print(f"Tabla '{table_name}' renombrada a '{new_table_name}'.")

            elif cmd == "dropall":
                if len(args) != 1:
                    print("Uso: dropall")
                else:
                    simulator.drop_all_tables()
                    print("Todas las tablas han sido eliminadas.")

            else:
                print("Comando no reconocido. Comandos disponibles: create, list, put, get, delete, scan, deleteall, count, truncate, disable, is_enabled, alter, dropall, exit")

        except Exception as e:
            print(f"Error: {e}")
