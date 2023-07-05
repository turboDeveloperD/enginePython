import sys

## herencia simple
class Animal:
    do_some = "hablando"
    numero_de_patas = 4

    def hablar(self):
        print(self.do_some)

    def volar(self):
        print("puedo volar woodys")


class Gato(Animal):
    name = ""

    def __init__(self, nombre):
        self.name = nombre


    def hablar(self):
        print("miau")

    def show_number_patas(self):
        return self.numero_de_patas

    def vuela(self):
        self.volar()


def habla(x: Animal):
    print(x.hablar())

### Fin herencia simple

### Herencia Multiple y herencia nivel o por nivel

class Rutinas:

    @staticmethod
    def comer():
        print("Comer")

    @staticmethod
    def entrenar():
        print("entrenar")

    @staticmethod
    def domir():
        print("Dormir")

class Corredor(Rutinas):
    def __init__(self, _km):
        self.km_correr = _km

    def accion(self):
        print("corriendo")


class Nadador():
    def __init__(self, _km):
        self.km_nadar = _km

    def accion(self):
        print("nadando")


class Atleta(Nadador, Corredor):
    def __init__(self, name, km_c=10, km_n= 5):
        Nadador.__init__(self, km_n)
        Corredor.__init__(self, km_c)
        self.name = name

    def imprime_datos(self):
        print("Nombre: " + self.name)
        print("Km corridos" + str(self.km_correr))
        print("Km nadados" + str(self.km_nadar))
        self.accion()



### Fin Herencia Multiple

class Car:
    key = ""
    max_kmh = 0

    def __init__(self, _key="000", _max_kmh=180):
        self.key = _key
        self.max_kmh = _max_kmh

    @staticmethod
    def saluda():
        print("Hola")

def main():

    #Polimorfismo

    felix: Gato = Gato("felix")
    habla(felix)
    felix.volar()
    felix.vuela()



    #car1 = Car(_max_kmh = 123, _key="d3")
    #car2 = Car()



    #Car.key = "456" # modifica el atributo de una clase de manera genera para todos los objetos, no es recomendable
    #print("Max_kmh car1: \ncar2: {}".format( car2.max_kmh))
    #car2.saluda() # para llamar un metodo que


    altleta_1 = Atleta("Alfredo")
    altleta_1.imprime_datos()
    altleta_1.comer()
    altleta_1.domir()



if __name__ == "__main__":
    sys.exit(main())
