
from typing import Dict

from dataproc_sdk import DatioPysparkSession
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
from pyspark.sql import SparkSession

import  pyspark.sql.functions as f
from exampleenginepythonqiyhbwvw.business_logic.business_logic import Business_logic
from exampleenginepythonqiyhbwvw.io.init_values import InitValues
import exampleenginepythonqiyhbwvw.common.constants as c
import exampleenginepythonqiyhbwvw.common.input as i
import exampleenginepythonqiyhbwvw.common.output as o

class DataprocExperiment:
    """
    Just a wrapper class to ease the user code execution.
    """

    def __init__(self):
        """
        Constructor
        """
        self.__logger = get_user_logger(DataprocExperiment.__qualname__)

        # obtenemos la sesion de spark activa con esta linea
        self.__spark = SparkSession.builder.getOrCreate()

        self.__datio_pyspark_session = DatioPysparkSession().get_or_create()

    def run(self, **parameters: Dict) -> None:
        """
        Execute the code written by the user.

        Args:
            parameters: The config file parameters
        """
        self.__logger.info("Executing Experiment")
        logic = Business_logic()

        ## codigo nuevo
        # LECTURA

        init_Values = InitValues()
        clients_df, contracts_df, products_df, out_path, out_schema = init_Values.initialize_inputs(parameters)

        #clients_df = self.read_csv("clients_path", parameters)
        #clients_df.printSchema()
        #contracts_df = self.read_csv("contracts_path", parameters)
        #products_df = self.read_csv("products_path", parameters)


        print("clients tables")
        print("function count: {}".format(clients_df.count()))
        # El metodo count retorna el numero de elementos que tiene un dataframe
        print("function printSchema:"),
        # imprime la estructura del dataframe, es decir nos muestra el nombre, tipo y si admite null el campo del dataframe
        #print(clients_df.printSchema())
        #clients_df.show()

        # TRANSFORMACIONES
        dataCollect = clients_df.collect()
        ## Collect muestra la data en en formato Row
        # row(campo1=###, campo2="", ...), Row(campo1=###, campo2="", ...), ...
        #
        print(dataCollect)

        #head retorna un arreglo de un registro o los registros en formato Row del dataframe
        # dependiendo si recibe un n = ## o puede ir sin este, para este ultimo retorna el primer registros
        # como row
        print("Head")
        dfHead = clients_df.head(n=c.FOUR_VALUE)
        print(dfHead)

        print("Take")
        # take(n = integer): retorna un arreglo de Rows de un dataframe de 0 a n
        dfTake = clients_df.take(c.THREE_NUMBER)
        print(dfTake)

        #first()= retorna el primer Row de un DataFrame
        dfFirst = clients_df.first()
        print(dfFirst)
        # El metodo nos muestra la informacion (registros) del dataframe

        clients_df.show()
        print("filter business logic")

        filter = logic.filter_by_age_and_vip(clients_df)
        filter.show()

        contracts_df.show()
        products_df.show()

        print("Join")
        join_df = logic.join_tables(filter, contracts_df, products_df)
        join_df.show()

        print("Logic")

        filter_by_contracts =logic.filter_by_number_of_contracts(join_df)
        filter_by_contracts.show()

        print("Hash")

        hashed_df = logic.hash_columns(filter_by_contracts)

        # simulamos errores casteando, borrando columnas fec_alta, edad, desc_producto
        #final_df = hashed_df\
        #            .withColumn("hash", f.when(f.col("activo") == "false", f.lit("0")).otherwise(f.col("hash")))\
        #                    .filter(f.col("hash") == 0)

        final_df= hashed_df

        final_df.show()
        # ESCRITURA
        # validar schema
        self.__datio_pyspark_session.write().mode(c.OVERWRITE)\
            .option(c.MODE, c.DYNAMIC)\
            .partition_by([o.cod_producto.name,o.activo.name])\
            .datio_schema(out_schema)\
            .parquet(final_df, out_path)


        #.parquet(logic.select_all_columns(final_df), out_path)

        #final_df.show(20, False)

        # leer en rutas particionadas
        # muestra la particion por cod_producto =600 con el activo false
        # con el detalle que no me mostrara en el dataframe los campos cod_producto
        # y activo
        # self.__spark.read.parquet("resources/data/output/final_table/cod_producto=600/activo=false").show()

        # aqui si me mostrara todos los campos
        # antes del video 7
        # self.__spark.read.option("basePath","resources/data/output/final_table").parquet("resources/data/output/final_table/cod_producto=600/activo=false").show()


        #Video 6 escritura del archivo parquet"""
        # agregar info
        #hashed_df.write.mode("append").parquet(str(parameters["output"]))
        # antes del video 7
        # final_df.write.mode("overwrite").partitionBy("cod_producto","activo" ).option("partitionOverwriteMode","dynamic").parquet(str(parameters["output"]))


        """
        print("contracts_df tables")
        contracts_df.show()
        print("products_df tables")
        products_df.show()"""

        '''
        #mis cambios
        print("Parquets")
        print("Phones")


        #phones = self.read_Parquet("phone", parameters)
        print("customers")
        #customers = self.read_Parquet("customers", parameters)

        #init_ValuesMe = InitValuesMe()
        init_ValuesMe = InitValues()
        customers, phones, output_path, output_schema = init_ValuesMe.initialize_inputs_2(parameters)

        print("Phones: {}".format(phones.count()))
        print("customers {}".format(customers.count()))
        phones.show()
        customers.show()
        #phones.printSchema()
        #customers.printSchema()
        print("Rules 1:")
        rules1 = logic.filter_phones_by_date_and_mar(phones)
        # rules1.show() funciona
        print("count rules1 = {}".format(rules1.count()))
        print("Rules 2:")
        rules2 = logic.filter_customers(customers)
        # rules2.show() funciona
        print("count rules2 = {}".format(rules2.count()))

        print("Rule 3:")
        joinRules = logic.join_phone_customers(rules1, rules2)
        # joinRules.show() funciona
        countjoin = joinRules.filter(f.col("nfc").isNull())
        print("countjoin.count = {}".format(countjoin.count()))
        print("join count: {}".format(joinRules.count()))

        print("Rule 4:")
        rule4 = logic.customers_vips(joinRules)
        rule4.show() # funciona
        print("rule4 count: {}".format(rule4.count()))

        print("Rule 5")
        #rule5 = logic.discount_extra(joinRules) # ORIGINL
        rule5 = logic.discount_extra(rule4) # new


        rule5.show() #funciona
        print("count rule5: {}".format(rule5.count()))

        print("Rule 6")
        #rule6 = logic.price_final(joinRules) # original
        rule6 = logic.price_finalNew(rule5) # nueva funcion
        #rule6 = logic.price_final(rule5) # nuevo
        #rule6.printSchema()
        rule6.show() #funciona
        print("count rule6: {}".format( rule6.count() ) )
        filterrule6 = rule6.filter( f.col("nfc").isNull() )
        print("filterrule6.count = {}".format(filterrule6.count()))

        print("Rule 7")
        rule7 = logic.top50(rule6)# original # se modifico funcion price_final
        #rule7 = logic.top50(filterrule6)# original
        rule7.show() #funciona
        print("rule 7 = {}".format(rule7.count()))

        print("Rule 8")
        rule8 = logic.notNullDogs(rule7) # Original

        rule8.show() #funciona
        print("rule8 count {}".format(rule8.count()))
        strDate = self.getDateConfig("date", parameters)
        print(strDate)
        rule9 = logic.jwk_date(rule8, strDate)
        #rule9.show() #
        rule9.printSchema()

        #ejercicio 6
        rule6_1 = logic.birthday(rule9, strDate)
        rule6_1.show()
        #rule6_1.printSchema()

        print("logic select all")
        rule6_1.printSchema()
        rule7_4 = logic.select_all_columns2(rule6_1)
        rule7_4.printSchema()

        print("output_path")
        print(output_path)

        self.__datio_pyspark_session.write().mode("overwrite") \
            .option("partitionOverwriteMode", "dynamic") \
            .partition_by(["jwk_date"]) \
            .datio_schema(output_schema) \
            .parquet(rule7_4, output_path)

        # FIN MIO
                '''


        """rule6_1.write\
            .mode("overwrite")\
            .partitionBy("jwk_date")\
            .option("partitionOverwriteMode", "dynamic")\
            .parquet(str(parameters["output"]))"""

        #customers.show()



    def read_csv(self, table_id, parameters):
        return self.__spark.read.option("header", "true")\
            .option("delimiter",",")\
            .csv(str(parameters[table_id]))

    def read_Parquet(self, table_id, parameters):
        return self.__spark.read.option("header", "true")\
            .parquet(str(parameters[table_id]))

    def getDateConfig(self, table_id, parameters):
        return str(parameters[table_id])
