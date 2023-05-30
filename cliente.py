
import zmq


def listar_libros(socket):
    socket.send_string("LL")
    libros_disponibles = socket.recv_string()
    print("Lista de Libros Disponibles:")
    print(libros_disponibles)

def listar_prestamos(socket):
    socket.send_string("LP")
    prestamos_actuales = socket.recv_string()
    print("Lista de Préstamos Actuales:")
    print(prestamos_actuales)

def pedir_libro(socket):
    codigo_libro = input("Ingrese el código del libro que desea solicitar: ")
    usuario = input("Ingrese su nombre de usuario: ")
    tiempo = input("Ingrese la fecha de préstamo (YYYYMMDD): ")
    global sede
    comando = f"QP {usuario}_{tiempo}_{sede}_{codigo_libro}"
    socket.send_string(comando)
    respuesta = socket.recv_string()
    print(respuesta)

def extender_prestamo(socket):
    codigo_libro = input("Ingrese el código del libro cuyo préstamo desea extender: ")
    usuario = input("Ingrese su nombre de usuario: ")
    comando = f"QE {codigo_libro}_{usuario}"
        
    socket.send_string(comando)
    respuesta = socket.recv_string()
    print(respuesta)

def regresar_libro(socket):
    codigo_libro = input("Ingrese el código del libro que desea regresar: ")
    usuario = input("Ingrese su nombre de usuario: ")
    comando = f"QT {codigo_libro}_{usuario}"
    socket.send_string(comando)
    respuesta = socket.recv_string()
    print(respuesta)


# Configuración del contexto ZMQ
contexto = zmq.Context()
socket = contexto.socket(zmq.REQ)
socket.connect("tcp://localhost:5555")
sede = input("SEDE: ")
print("Bienvenido a la Librería Javeriana sede "+sede)

while True:
    print("1. Listar Libros")
    print("2. Listar Prestamos")
    print("3. Pedir Libro")
    print("4. Extender Prestamo")
    print("5. Regresar Libro")
    print("6. Salir")
    request = input("[Peticion]: ")

    if request == "1":
        listar_libros(socket)
    elif request == "2":
        listar_prestamos(socket)
    elif request == "3":
        pedir_libro(socket)
    elif request == "4":
        extender_prestamo(socket)
    elif request == "5":
        regresar_libro(socket)
    elif request == "6":
        socket.send_string("endCOMS")
        break
    else:
        print("Opción inválida. Por favor, ingrese un número válido del menú.")
