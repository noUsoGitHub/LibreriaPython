import Pyro4

 mi_objeto = Pyro4.Proxy("PYRO:example.hola@192.168.102.100:9090")

 print(mi_objeto.hi("Mundo"))


