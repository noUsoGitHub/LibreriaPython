import argparse
import zmq
import pymysql

conexion = None
conexion2 = None

def replicate_data():
    global conexion
    db1_connection = conexion
    global conexion2
    db2_connection = conexion2
    
    # Obtener los datos de la tabla "libro" en la primera base de datos
    select_libros_query = "SELECT * FROM libro"
    cursor1 = db1_connection.cursor()
    cursor1.execute(select_libros_query)
    libros_data = cursor1.fetchall()

    # Insertar o actualizar los datos en la tabla "libro" de la segunda base de datos
    insert_libro_query = "INSERT INTO libro (codigo, nombre) VALUES (%s, %s)"
    update_libro_query = "UPDATE libro SET nombre = %s WHERE codigo = %s"
    cursor2 = db2_connection.cursor()

    for libro_row in libros_data:
        # Verificar si el libro ya existe en la segunda base de datos
        check_libro_query = "SELECT * FROM libro WHERE codigo = %s"
        cursor2.execute(check_libro_query, (libro_row[0],))
        existing_libro_row = cursor2.fetchone()

        if existing_libro_row:
            # Actualizar el nombre del libro existente en la segunda base de datos
            update_libro_values = (libro_row[1], libro_row[0])
            cursor2.execute(update_libro_query, update_libro_values)
        else:
            # Insertar un nuevo libro en la segunda base de datos
            insert_libro_values = (libro_row[0], libro_row[1])
            cursor2.execute(insert_libro_query, insert_libro_values)

    # Obtener los datos de la tabla "prestamo" en la primera base de datos
    select_prestamo_query = "SELECT * FROM prestamo"
    cursor1.execute(select_prestamo_query)
    prestamo_data = cursor1.fetchall()

    # Insertar o actualizar los datos en la tabla "prestamo" de la segunda base de datos
    insert_prestamo_query = "INSERT INTO prestamo (codigo, usuario, tiempo, sede) VALUES (%s, %s, %s, %s)"
    update_prestamo_query = "UPDATE prestamo SET usuario = %s, tiempo = %s, sede = %s WHERE codigo = %s"

    for prestamo_row in prestamo_data:
        # Verificar si el préstamo ya existe en la segunda base de datos
        check_prestamo_query = "SELECT * FROM prestamo WHERE codigo = %s"
        cursor2.execute(check_prestamo_query, (prestamo_row[0],))
        existing_prestamo_row = cursor2.fetchone()

        if existing_prestamo_row:
            # Actualizar el préstamo existente en la segunda base de datos
            update_prestamo_values = (prestamo_row[1], prestamo_row[2], prestamo_row[3], prestamo_row[0])
            cursor2.execute(update_prestamo_query, update_prestamo_values)
        else:
            # Insertar un nuevo préstamo en la segunda base de datos
            insert_prestamo_values = (prestamo_row[0], prestamo_row[1], prestamo_row[2], prestamo_row[3])
            cursor2.execute(insert_prestamo_query, insert_prestamo_values)

    # Confirmar los cambios en la segunda base de datos
    db2_connection.commit()

def conectar(usuario, clave, host, base_datos):
    # Conexión a la base de datos
    conexion = pymysql.connect(user=usuario, password=clave, host=host, database=base_datos)
    return conexion

def ejecutar_query(conexion, query):
    # Ejecutar consulta SQL
    cursor = conexion.cursor()
    cursor.execute(query)
    resultado = cursor.fetchall()
    cursor.close()
    return resultado

def crear_prestamo(uid, tiempo, sede, libro_codigo):
    # Crear el préstamo en la base de datos
    insert_query = "INSERT INTO prestamo (codigo, usuario, tiempo, sede) VALUES (%s, %s, %s, %s)"
    values = (libro_codigo, uid, tiempo, sede)
    global conexion
    cursor = conexion.cursor()
    cursor.execute(insert_query, values)
    conexion.commit()

def consultar_estado_prestamo(libro_codigo):
    # Consultar el estado de préstamo del libro en la base de datos
    select_query = "SELECT * FROM prestamo WHERE codigo = %s"
    global conexion
    cursor = conexion.cursor()
    cursor.execute(select_query, (libro_codigo,))
    resultado = cursor.fetchone()
    if resultado:
        return "Prestado"
    else:
        return "Disponible"
def extender_plazo_prestamo(libro_codigo, usuario):
    # Consultar el préstamo del libro para el usuario en la base de datos
    select_query = "SELECT * FROM prestamo WHERE codigo = %s AND usuario = %s"
    global conexion
    cursor = conexion.cursor()
    cursor.execute(select_query, (libro_codigo, usuario))
    resultado = cursor.fetchone()

    if resultado:
        # Actualizar el plazo del préstamo
        update_query = "UPDATE prestamo SET tiempo = DATE_ADD(tiempo, INTERVAL 7 DAY) WHERE codigo = %s AND usuario = %s"
        cursor.execute(update_query, (libro_codigo, usuario))
        conexion.commit()
        return "Extendido"
    else:
        return "Error"

def terminar_prestamo(libro_codigo, usuario):
    # Verificar si existe un préstamo del libro para el usuario en la base de datos
    select_query = "SELECT * FROM prestamo WHERE codigo = %s AND usuario = %s"
    global conexion
    cursor = conexion.cursor()
    cursor.execute(select_query, (libro_codigo, usuario))
    resultado = cursor.fetchone()

    if resultado:
        # Eliminar el préstamo de la base de datos
        delete_query = "DELETE FROM prestamo WHERE codigo = %s AND usuario = %s"
        cursor.execute(delete_query, (libro_codigo, usuario))
        conexion.commit()
        return "Terminado"
    else:
        return "Error"

def consultar_libro(libro_codigo):
    # Consultar el estado de préstamo del libro en la base de datos
    select_query = "SELECT * FROM libros WHERE codigo = %s"
    global conexion
    cursor = conexion.cursor()
    cursor.execute(select_query, (libro_codigo,))
    resultado = cursor.fetchone()
    if resultado:
        return "Existente"
    else:
        return "No existente"
    
def cerrar_conexion(conexion):
    # Cerrar la conexión a la base de datos
    conexion.close()
    conexion2.close()

def main():
    usuario="root"
    clave="1234"
    host="localhost"
    host2="localhost"
    base_datos="libreria"
    base_datos2="libreria2"

    # Configuración del contexto ZMQ
    contexto = zmq.Context()
    socket = contexto.socket(zmq.SUB)
    socket.connect("tcp://localhost:555")
    socket.subscribe("")  # Suscribirse a todos los mensajes

    # Conexión a la base de datos
    global conexion
    conexion = conectar(usuario, clave, host, base_datos)
    print("[Base local Conectada]")
    global conexion2
    conexion = conectar(usuario, clave, host, base_datos)
    conexion2 = conectar(usuario, clave, host2, base_datos2)
    print("[Base Objetivo Conectada]")
    print("[Esperando Comandos]")
    # Bucle principal para recibir comandos
    while True:

        '''El formato de los comandos es el siguiente:
                - Comando para crear un préstamo: "QP_uid_tiempo_sede_codigo_libro"
                - Donde:
                    - "Q" indica que es una consulta.
                    - "P" indica que se desea crear un préstamo.
                    - "uid" es el identificador de usuario.
                    - "tiempo" es la fecha del préstamo en formato AAAAMMDD.
                    - "sede" es la sede donde se realiza el préstamo.
                    - "codigo_libro" es el código del libro a prestar.
                - Comando para extender el plazo de un préstamo: "QE_codigo_libro_usuario"
                - Donde:
                    - "Q" indica que es una consulta.
                    - "E" indica que se desea extender el plazo de un préstamo.
                    - "codigo_libro" es el código del libro.
                    - "usuario" es el identificador del usuario.
                - Comando para terminar un préstamo: "QT_codigo_libro_usuario"
                - Donde:
                    - "Q" indica que es una consulta.
                    - "T" indica que se desea terminar un préstamo.
                    - "codigo_libro" es el código del libro.
                    - "usuario" es el identificador del usuario.
'''
        comando = socket.recv_string()
        # Procesar comando
        if comando[0] == "Q":
            if comando[1] == "P":
                # Obtener los datos del comando
                datos = comando[2:].split("_")
                uid = datos[0]
                tiempo = datos[1]
                sede = datos[2]
                libro_codigo = datos[3]

                # Llamar a la función para crear un préstamo en la base de datos
                crear_prestamo(uid, tiempo, sede, libro_codigo)
                contexto = zmq.Context()
                socketCliente = contexto.socket(zmq.REP)
                socketCliente.bind("tcp://*:666")
                # Enviar respuesta a través de ZMQ
                socketCliente.send_string("Préstamo creado")

            elif comando[1] == "E":
                # Obtener los datos del comando
                datos = comando[2:].split("_")
                libro_codigo = datos[0]
                usuario = datos[1]

                # Llamar a la función para extender el plazo del préstamo
                resultado = extender_plazo_prestamo(libro_codigo, usuario)

                # Enviar respuesta a través de ZMQ
                #socket.send_string(resultado)

            elif comando[1] == "T":
                # Obtener los datos del comando
                datos = comando[2:].split("_")
                libro_codigo = datos[0]
                usuario = datos[1]

                # Llamar a la función para terminar el préstamo
                resultado = terminar_prestamo(libro_codigo, usuario)

                # Enviar respuesta a través de ZMQ
                #socket.send_string(resultado)
            replicate_data()
        elif comando[0] == "C":
            # Cerrar la conexión a la base de datos
            cerrar_conexion(conexion)
            break



if __name__ == '__main__':
    main()
