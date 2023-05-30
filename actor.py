import zmq
import sys
import pymysql



def replicate_data():
    global conexion
    db1_connection = conexion
    global conexion2
    db2_connection = conexion2
    
    # Obtener los datos de la tabla "libro" en la primera base de datos
    select_libros_query = "SELECT * FROM libros"
    cursor1 = db1_connection.cursor()
    cursor1.execute(select_libros_query)
    libros_data = cursor1.fetchall()

    # Insertar o actualizar los datos en la tabla "libro" de la segunda base de datos
    insert_libro_query = "INSERT INTO libros (codigo, nombre) VALUES (%s, %s)"
    update_libro_query = "UPDATE libros SET nombre = %s WHERE codigo = %s"
    cursor2 = db2_connection.cursor()

    for libro_row in libros_data:
        # Verificar si el libro ya existe en la segunda base de datos
        check_libro_query = "SELECT * FROM libros WHERE codigo = %s"
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

def crear_prestamo(uid, tiempo, sede, libro_codigo):
     # Consultar el estado de préstamo del libro en la base de datos
    select_query = "SELECT * FROM prestamo WHERE codigo = %s"
    global conexion
    cursor = conexion.cursor()
    cursor.execute(select_query, (libro_codigo,))
    resultado = cursor.fetchone()
    if resultado:
        return "No es posible hacer ese prestamo"
    # Consultar el estado de préstamo del libro en la base de datos
    select_query = "SELECT * FROM libros WHERE codigo = %s"
    cursor = conexion.cursor()
    cursor.execute(select_query, (libro_codigo,))
    resultado = cursor.fetchone()
    if resultado:
        # Crear el préstamo en la base de datos
        insert_query = "INSERT INTO prestamo (codigo, usuario, tiempo, sede) VALUES (%s, %s, %s, %s)"
        values = (libro_codigo, uid, tiempo, sede)
        cursor = conexion.cursor()
        cursor.execute(insert_query, values)
        conexion.commit()
        return "Prestamo exitoso"
    else:
        return "Libro Inexistente"
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


def reciveMessage():
    mensaje = ""
    tmp = socket.recv_string()
    temp = tmp.split(" ")
    for m in temp:
        mensaje += m + " "
    return mensaje

def processMessage(mensaje):
    comando = mensaje.split(" ")[0]
    datos = mensaje.split(" ")[1].split("_")
    print(datos)
    
    if comando == "QP":
        # Obtener los datos del mensaje
        libro_codigo = datos[3]
        usuario = datos[0]
        sede = datos[2]
        fecha_formateada = datos[1]
        print("Solicitud Recibida: ",datos)
        result = crear_prestamo(usuario, fecha_formateada, sede, libro_codigo)
        global socketCliente
        socketCliente.send_string(result)
        print(result)
        
    
    elif comando == "QE":
        # Obtener los datos del mensaje
        libro_codigo = datos[0]
        usuario = datos[1]
        print("Solicitud Recibida: ",datos)
        # Construir el comando de extensión del plazo
        print(extender_plazo_prestamo(libro_codigo,usuario))
    
    elif comando == "QT":
        # Obtener los datos del mensaje
        libro_codigo = datos[0]
        usuario = datos[1]
        print("Solicitud Recibida: ",datos)
        # Construir el comando de terminación del préstamo
        print(terminar_prestamo(libro_codigo,usuario))
    replicate_data()  


args = sys.argv
port = args[1]                                     # Args para pasar el puerto y el tema a suscribir
context = zmq.Context()                             # Context de zmq
socket = context.socket(zmq.SUB)                    # Creamos un socket de tipo SUB
socket.setsockopt_string(zmq.SUBSCRIBE, args[2])    # Nos suscribimos al tema pasado por args
socket.connect('tcp://localhost:' + str(port))   # Conectamos el socket al puerto especificado
socketCliente = None
if (args[2]=="QP"):
    print("QP creado")
    socketCliente = context.socket(zmq.PUB)
    socketCliente.bind("tcp://*:4404")
usuario="root"
clave="1234"
host="localhost"
host2="localhost"
base_datos="libreria"
base_datos2="libreria2"
conexion = conectar(usuario, clave, host, base_datos)
conexion2 = conectar(usuario, clave, host2, base_datos2)

print("Servidor Inicializado en el puerto", port, "Suscrito al tema", args[2])
# Esperamos recibir un mensaje mientras no se finalice la comunicación con endCOMS
mensaje = ""
while "endCOMS" not in mensaje:
    mensaje = socket.recv_string() # Se procesa el mensaje
    print(f"Mensaje recibido del puerto[{args[1]}]: {mensaje}")  # Mostramos el mensaje recibido
    processMessage(mensaje)
