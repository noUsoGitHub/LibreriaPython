import zmq
import sys

def reciveMessage():
    mensaje=""
    tmp = socket.recv_string()
    temp = tmp.split(" ")
    for m in temp[1:]:
        mensaje+= m + " "
    return mensaje
def processMessage(mensaje):

    archivo = open('baseTemporal.txt', 'a')              # Abrir el archivo en modo escritura
    archivo.write(mensaje+"\n")                   # Escribir contenido en el archivo
    archivo.close()
    
args = sys.argv                                     #Args para pasar el puerto y el tema a suscribir
context = zmq.Context()                             #Context de zmq
socket = context.socket(zmq.SUB)                    # Creamos un socket de tipo SUB
socket.setsockopt_string(zmq.SUBSCRIBE, args[2])    # Nos suscribimos al tema pasado por args
socket.connect('tcp://localhost:'+str(args[1]))     # Conectamos el socket al puerto especificado


print("Servidor Inicializado en el puerto ",args[1], " Suscrito al tema",args[2])

# Esperamos recibir un mensaje mientras no se finalize la comunicacion con endCOMS
mensaje=""
while "endCOMS" not in mensaje:
    mensaje= reciveMessage() # Se procesa el mensaje
    print(f"Mensaje recibido del puerto[{args[1]}]: {mensaje}")     # Mostramos el mensaje recibido
    processMessage(mensaje)

