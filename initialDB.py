import pymysql

# Detalles de conexión
host = 'localhost'
user = 'root'
password = '1234'
name = "libreria2"

# Crear la conexión
connection = pymysql.connect(host=host, user=user, password=password, db=name)

# Crear un cursor
cursor = connection.cursor()
'''
# Consulta para eliminar la tabla prestamo (si existe)
drop_table_query = "DROP TABLE IF EXISTS prestamo"
cursor.execute(drop_table_query)

# Consulta para eliminar la tabla libros (si existe)
drop_table_query = "DROP TABLE IF EXISTS libros"
cursor.execute(drop_table_query)

# Consulta para crear la tabla libros
create_table_query = """
CREATE TABLE libros (
    codigo INT PRIMARY KEY,
    nombre VARCHAR(100)
)
"""
cursor.execute(create_table_query)

# Consulta para crear la tabla prestamo
create_prestamo_table_query = """
CREATE TABLE prestamo (
    codigo INT PRIMARY KEY,
    usuario VARCHAR(100),
    tiempo DATE,
    sede VARCHAR(100),
    FOREIGN KEY (codigo) REFERENCES libros(codigo)
)
"""
cursor.execute(create_prestamo_table_query)

# Generar 300 inserciones en la tabla libros
for i in range(300):
    codigo = i + 1
    nombre = f"Libro {codigo}"
    insert_query = "INSERT INTO libros (codigo, nombre) VALUES (%s, %s)"
    values = (codigo, nombre)
    cursor.execute(insert_query, values)

# Generar 50 inserciones en la tabla prestamo (25 en cada sede)
for i in range(25):
    codigo = i + 1
    usuario = f"Usuario {codigo}"
    tiempo = "2023-05-29"
    sede = "Sede A"
    insert_query = "INSERT INTO prestamo (codigo, usuario, tiempo, sede) VALUES (%s, %s, %s, %s)"
    values = (codigo, usuario, tiempo, sede)
    cursor.execute(insert_query, values)

for i in range(25, 50):
    codigo = i + 1
    usuario = f"Usuario {codigo}"
    tiempo = "2023-05-29"
    sede = "Sede B"
    insert_query = "INSERT INTO prestamo (codigo, usuario, tiempo, sede) VALUES (%s, %s, %s, %s)"
    values = (codigo, usuario, tiempo, sede)
    cursor.execute(insert_query, values)

# Confirmar los cambios en la base de datos
connection.commit()

# Consulta para seleccionar todos los registros de la tabla libros
select_query = "SELECT * FROM libros"

# Ejecutar la consulta
cursor.execute(select_query)

# Obtener los resultados de la consulta
results = cursor.fetchall()

# Imprimir los registros de la tabla libros
print("Libros:")
for row in results:
    print(row)'''

# Consulta para seleccionar todos los registros de la tabla prestamo
select_query = "SELECT * FROM prestamo"

# Ejecutar la consulta
cursor.execute(select_query)

# Obtener los resultados de la consulta
results = cursor.fetchall()

# Imprimir los registros de la tabla prestamo
print("\nPrestamos:")
for row in results:
    print(row)

# Cerrar el cursor y la conexión
cursor.close()
connection.close()
