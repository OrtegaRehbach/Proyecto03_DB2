# Proyecto03_DB2 - HBase Shell Simulator

Bienvenido al Simulador de HBase Shell, una herramienta en Python que simula las funcionalidades básicas de HBase Shell. Este proyecto permite crear, manipular y consultar tablas HBase utilizando una interfaz de línea de comandos (CLI).

## Características

- Crear tablas con múltiples column families.
- Insertar datos en tablas.
- Consultar datos específicos por `row_key`.
- Escanear tablas completas.
- Eliminar datos específicos y filas completas.
- Contar filas en una tabla.
- Deshabilitar, habilitar y truncar tablas.
- Alterar tablas para agregar o eliminar column families.
- Listar y describir tablas.
- Soporte para múltiples versiones de celdas.

## Estructura del Proyecto

```
/root
├── data
│   ├── users.hfile
│   ├── products.hfile
│   └── students.hfile
├── src
│   ├── hbase_simulator
│   │   ├── init.py
│   │   ├── hfile.py
│   │   ├── table.py
│   │   └── utils.py
│   ├── cli.py
│   └── main.py
├── requirements.txt
└── README.md
```

## Instalación

Para instalar las dependencias necesarias, ejecuta:

```sh
pip install -r requirements.txt
```

## Uso

Para iniciar el simulador de HBase Shell, ejecuta el archivo `cli.py`:

```sh
python cli.py
```

### Comandos Disponibles

- **create**: Crear una nueva tabla.
  ```sh
  create <table_name> <column_family1> <column_family2> ...
  ```
- **list**: Listar todas las tablas.
  ```sh
  list
  ```
- **put**: Insertar datos en una tabla.
  ```sh
  put <table_name> <row_key> <column_family> <column> <value>
  ```
- **get**: Consultar datos específicos por `row_key`.
  ```sh
  get <table_name> <row_key>
  ```
- **scan**: Escanear todos los datos de una tabla.
  ```sh
  scan <table_name>
  ```
- **delete**: Eliminar datos específicos.
  ```sh
  delete <table_name> <row_key> <column_family> <column> [<timestamp>]
  ```
- **delete_all**: Eliminar todos los datos en una fila.
  ```sh
  delete_all <table_name> <row_key>
  ```
- **count**: Contar el número de filas en una tabla.
  ```sh
  count <table_name>
  ```
- **truncate**: Truncar una tabla (debe estar deshabilitada).
  ```sh
  truncate <table_name>
  ```
- **disable**: Deshabilitar una tabla.
  ```sh
  disable <table_name>
  ```
- **enable**: Habilitar una tabla.
  ```sh
  enable <table_name>
  ```
- **is_enabled**: Verificar si una tabla está habilitada.
  ```sh
  is_enabled <table_name>
  ```
- **alter**: Alterar una tabla para agregar o eliminar column families.
  ```sh
  alter <table_name> NAME=><column_family> [METHOD=>delete]
  ```
- **drop**: Eliminar una tabla (debe estar deshabilitada).
  ```sh
  drop <table_name>
  ```
- **drop_all**: Eliminar todas las tablas.
  ```sh
  drop_all
  ```
- **describe**: Describir una tabla.
  ```sh
  describe <table_name>
  ```

### Ejemplo de Uso

```sh
hbase> create students personal_info grades
Table 'students' created with column families: personal_info, grades.

hbase> put students row1 personal_info name "Alice Johnson"
Data inserted into 'students': row1, personal_info:name = Alice Johnson

hbase> get students row1
|   | Column_Family:Column |   Timestamp   |      Value     |
|---|----------------------|---------------|----------------|
| 0 | personal_info:name   | 1716933700000 | Alice Johnson  |
```

## Licencia

Este proyecto está licenciado bajo la [MIT License](LICENSE).

