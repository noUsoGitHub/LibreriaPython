import zmq
import time
import sys
import Pyro4
args = sys.argv


def pasarPorArgumento():
    str=""
    for c in args[2:]:
        str+=c+" "
    return str


#Args para pasar el puerto


@Pyro4.expose
class GestorCarga:
    def __init__(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)                    # Creamos un socket de tipo PUB
        self.socket.bind('tcp://*:'+str(args[1]))                # Ligamos el socket al puerto pasado
        time.sleep(1)

    def escribir(self, mensaje):
        self.socket.send_string(mensaje)
        print(f"He recibido una solicitud [{mensaje}]")
        return f"Peticion Recibida"

daemon = Pyro4.Daemon.serveSimple(
    {
        GestorCarga: "example.gestor"
    },
    host="192.168.102.209",
    port=9090,
    ns=False
)

