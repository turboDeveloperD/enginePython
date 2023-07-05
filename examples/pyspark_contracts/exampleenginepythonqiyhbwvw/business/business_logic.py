from pyspark.sql.functions import broadcast, sha2, concat_ws
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger


class BusinessLogic:
    """
    This class contains several methods with needed logic for working in this PySpark example
    """

    def __init__(self):
        """
        Constructor
        """
        self.__logger = get_user_logger(BusinessLogic.__qualname__)

    def filter_example(self, df):
        """
        Filtering Clients Dataframe depending on age and if Client is or not VIP
        :param df: input df for filtering data
        :return: fileter df
        """
        self.__logger.info("Executing filter_example method...")
        return df.filter((df.edad >= 30) & (df.edad <= 50) & (df.vip == "true"))

    def join_example(self, df, df_join, df_broadcast):
        """
        Spark Join between Clients and Contracts Dataframes by "cod_client"/"cod_titular"
        Spark Broadcast join between previous Dataframe and Products Dataframe by "cod_producto"
        :param df: clients df
        :param df_join:  contracts df
        :param df_broadcast: products df
        :return:
        """
        self.__logger.info("Executing join_example method...")
        first_df = df_join.join(df, df_join.cod_titular == df.cod_client)
        return first_df.join(broadcast(df_broadcast), "cod_producto")

    def filter_sql_example(self, spark):
        """
        Spark SQL Query for filtering Clients with more than 3 contracts
        :param spark:
        :return:
        """
        self.__logger.info("Executing filter_sql_example method...")
        return spark.sql(
            "SELECT * FROM clients_contracts_df WHERE cod_client IN "
            "(SELECT cod_client FROM clients_contracts_df GROUP BY "
            "cod_client HAVING count(*) > 3)")

    def add_hash(self, df):
        """
        Add hash column
        :param df:
        :return: df with a hash new column
        """
        self.__logger.info("Executing add_hash method...")
        return df.withColumn("hash",
                             sha2(concat_ws("||", *df.columns), 256))

    def send_notification(self, df):
        name_list = df.select("nombre").distinct().collect()
        for name in name_list:
            self.__logger.info("Sending notification to: %s" % name)
        return name_list

    def add_binary_column(self, df: DataFrame, binary_column: str, column: str, value: int) -> DataFrame:
        """
        Adds a binary column with 0 or 1 depending on the value of column in parameter column
        Args:
            - df (DataFrame): initial spark dataframe.
            - binary_column (str): binary column with value 0 or 1.
            - column (str): the column to transform.
            - value (int): barrier value.
        Returns:
            - df (DataFrame): spark dataframe with binary column added.
        Examples:
            >>> from pyspark.sql import SparkSession
            >>> spark = SparkSession.builder.getOrCreate()
            >>> bl = BusinessLogic()
            >>> data = spark.createDataFrame([
            ...     ("Mike", "cell phone", 6000, "N", "BEC"),
            ...     ("Alice", "tablet", 1500, "S", "BEC"),
            ...     ("Jon", "tablet", 5500, "S", "BEC"),
            ...     ("Peter", "cell phone", 5000, "S", "BEC"),
            ...     ("Peter", "cell phone", 6000, "S", "BEC"),
            ...     ("Alice", "tablet", 2500, "S", "BEC"),
            ...     ("Mary", "cell phone", 3000, "S", "BEC"),
            ...     ("Peter", "cell phone", 3000, "S", "BEC"),
            ...     ("Alice", "tablet", 4500, "S", "BEC"),
            ...     ("Mary", "tablet", 6500, "S", "BEC")],
            ...     ["names", "category", "time", "col0", "col1"])
            >>> df = bl.add_binary_column(data, "num_time", "time", 1500)
            >>> df.filter("num_time = 1").count()
            9
        """
        df = df.withColumn(binary_column, F.when(F.col(column) <= value, 0).otherwise(1))
        return df
