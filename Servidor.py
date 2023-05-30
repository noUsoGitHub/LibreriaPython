import zmq
import time
import sys
import time
import zmq

args = sys.argv     #Args para pasar el puerto


class GestorCarga:
    def __init__(self):
        self.context = zmq.Context()
        self.socketActor = self.context.socket(zmq.PUB)                    # Creamos un socket de tipo PUB
        self.socketActor.bind('tcp://*:'+str(args[1]))                # Ligamos el socket al puerto pasado
        self.socketCliente = self.context.socket(zmq.REP)
        self.socketCliente.bind("tcp://*:5555")
        self.responseSync = self.context.socket(zmq.SUB)
        self.responseSync.setsockopt_string(zmq.SUBSCRIBE, "") 
        self.responseSync.connect("tcp://localhost:4404")
        time.sleep(1)

    def escribir(self, mensaje):
        print(mensaje[1])
        if  mensaje[1] != 'P':
            self.socketActor.send_string(mensaje)
            print(f"He recibido una solicitud [{mensaje}]")
            return f"Realizado"
        else:
            self.socketActor.send_string(mensaje)
            print(f"He recibido una solicitud [{mensaje}]")
            time.sleep(3)
            respuesta=""
            while (respuesta==""):
                respuesta = self.responseSync.recv_string()
            print("[RESPONSE] ",respuesta)
            return respuesta

        
gc = GestorCarga()
while True:
            #  Wait for next request from client
            message = gc.socketCliente.recv_string()

            #  Do some 'work'
            time.sleep(1)
            response = gc.escribir(message)
            time.sleep(1)
            #  Send reply back to client
            gc.socketCliente.send_string(response)