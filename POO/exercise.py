import sys

class Vehiculo:

    def __init__(self, vel=10, comb="Gasolina"):
        self.velocidad = vel
        self.__combustible = comb

    def getVelocidad(self):
        return self.velocidad

    def getCombustible(self):
        return self.__combustible

class Acuatico(Vehiculo):

    def __init__(self, typ=""):
        self.__tipo = typ

    def getTipo(self):
        return self.__tipo

    def isTransporte(self):

        val = self.getTipo()

        if val == "Transporte":
            return True
        else:
            return False



class Terrestre(Vehiculo):

    def __init__(self, llantas=4):
        Vehiculo.__init__(self)
        self.llantas = llantas

    def getLlantas(self):
        return self.llantas

    def getVelocidad(self):
        return str(self.velocidad) + " km"



class Autobus(Terrestre):

    def __init__(self, asientos=20):
        Terrestre.__init__(self, llantas=6)
        self.asientos = asientos

    def getLlantas(self):
        return "tiene " + str(self.llantas) + " y " + str(self.asientos)


class Tanque(Terrestre):

    def __init__(self, peso=56):
        Vehiculo.__init__(self)
        self.peso = peso

    def getPeso(self):
        return "El tanque tiene un peso de " + str(self.peso) + " toneladas"



def acuticoMessage(b: Acuatico):
    if b.isTransporte():
        print("Es un barco seguro nadie lo va a tirar")
    else:
        print("Es un barco de guerra sal de ahi")





def main():

    veh = Vehiculo(comb="Gas", vel=120 )
    print(veh.getCombustible())
    print(veh.getVelocidad())

    barcoNormal = Acuatico("Transporte")
    barcoGuerra = Acuatico("Submarino")
    acuticoMessage(barcoNormal)
    acuticoMessage(barcoGuerra)

    t1 = Terrestre(4)
    print("Numero de llantas de t1 = {}".format(t1.getLlantas()))
    print(t1.getVelocidad())

    a = Autobus(100)
    print(a.getLlantas())

    tanque = Tanque(60)
    print(tanque.getPeso())


if __name__ == "__main__":

    sys.exit(main())
