from dataproc_sdk import DatioPysparkSession, DatioSchema
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger


class InitValuesMe:

    def __init__(self):
        self.__logger = get_user_logger(InitValuesMe.__qualname__)
        self.__datio_pyspark_session = DatioPysparkSession().get_or_create()


    def initialize_inputs(self, parameters):
        self.__logger.info("Using given configuration")
        print("Functions initialize_inputs")
        phones = self.get_input_df(parameters, "phone_path", "phone_schema")
        customers = self.get_input_df(parameters, "customers_path", "customers_schema")
        #out_path, out_schema = self.get_config_by_name(parameters, "output_customersphones", "output_customersphones_schema")

        #return phones, customers, out_path, out_schema
        return phones, customers


    def get_config_by_name(self, parameters, key_path, key_schema):
        self.__logger.info("Get config for " + key_path + " " + key_schema)
        io_path = parameters[key_path]
        print("get_config_by_name io_path: {}".format(io_path))
        io_schema = DatioSchema.getBuilder().fromURI(parameters[key_schema]).build()
        print("get_config_by_name io_schema: {} ".format(io_schema))
        return io_path, io_schema


    def get_input_df(self, paramenters, key_path, key_schema):
        self.__logger.info("Reading from " + key_path + " schema " + key_schema)
        io_path, io_schema = self.get_config_by_name(paramenters, key_path, key_schema)
        print("get_input_df io_path {}".format(paramenters[io_path]))
        print("get_input_df io_schema {}".format(paramenters[io_schema]))

        return self.__datio_pyspark_session.read().datioSchema(io_schema)\
                .parquet(paramenters[io_path])



