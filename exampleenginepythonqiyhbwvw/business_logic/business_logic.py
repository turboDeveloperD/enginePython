from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
import  pyspark.sql.functions as f
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import *
import exampleenginepythonqiyhbwvw.common.constants as c
import exampleenginepythonqiyhbwvw.common.input as i
import exampleenginepythonqiyhbwvw.common.output as o


class Business_logic:
    """
    Just a wrapper class to ease the user code execution.
    """

    def __init__(self):
        """
        Constructor
        """
        self.__logger = get_user_logger(Business_logic.__qualname__)

        # aqui no necesitamos la variable de sparkSession
        #

    def filter_by_age_and_vip(self, df: DataFrame ) -> DataFrame:
        self.__logger.info("Applying filter by  edad and vip")
        return df.filter( (i.edad() >= c.THIRTY_NUMBER) &
                          (i.edad() <= c.FIFTY_NUMBER) &
                          (i.vip() == c.TRUE_VALUE) )


    def join_tables(self, clients_df: DataFrame, contracts_df: DataFrame, products_df: DataFrame ) -> DataFrame:
        self.__logger.info("Applying join process")
        return clients_df.join(contracts_df, i.cod_client() == i.cod_titular(), c.INNER_TYPE )\
            .join(products_df, [i.cod_producto.name], ) #f.col("cod_client") == f.col("cod_titular") es igual a [] esto para evitar ambiguedad


    def filter_by_number_of_contracts(self, df: DataFrame)-> DataFrame:
        self.__logger.info("Filtering by number of contracts")
        return df.select(*df.columns, f.count(i.cod_client() ).over(Window.partitionBy( i.cod_client() ) ).alias(c.COUNT_COLUMN) )\
            .filter( f.col(c.COUNT_COLUMN ) > c.THREE_NUMBER).drop(c.COUNT_COLUMN)


    def hash_columns(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Filtering by number of contracts")
        return df.select(*df.columns, f.sha2( f.concat_ws("", *df.columns), c.TWOFIVESIX_NUMBER ).alias(o.hash.name) )


    """Mis reglas de negocio"""

    def filter_phones_by_date_and_mar(self, phones: DataFrame)-> DataFrame:
        self.__logger.info("Filtering by date and marca")
        #temp_window = Window.partitionBy(f.col("brand")).orderBy(f.col("brand"))
        # filtro 1: por fecha
        listBrand = [c.BRAND_DELL, c.BRAND_COOLPAD, c.BRAND_CHEA, c.BRAND_BQ, c.BRAND_BLU]
        listCoutry = [c.COUNTRY_CH, c.COUNTRY_IT, c.COUNTRY_CZ, c.COUNTRY_DK]
        #f1 = phones.filter( (f.col("cutoff_date").between("2020-03-01", "2020-03-04") ) ) # original
        f1 = phones.filter( ( i.cutoff_date().between(c.DATE_1, c.DATE_2) ) )

        #f2 = f1.filter( f.col("brand").isin(listBrand) == False ) # original
        f2 = f1.filter( i.brand().isin(listBrand) == c.FALSE_VALUE )

        #f3 = f2.filter( f.col("country_code").isin(listCoutry) == False ) # original
        f3 = f2.filter( i.country_code().isin(listCoutry) == c.FALSE_VALUE )
        return f3

    def filter_customers(self, customers : DataFrame) -> DataFrame:
        self.__logger.info("Filtering by customers")

        # f1 = customers.filter( (f.col("gl_date").between(c.DATE_1, c.DATE_2) ) ) # original
        f1 = customers.filter( (i.gl_date().between(c.DATE_1, c.DATE_2) ) )
        #f2 = f1.filter( f.length(f.col("credit_card_number")) < 17 ) # original
        f2 = f1.filter( f.length(i.credit_card_number()) < c.SEVENTEEN_NUMBER )

        return f2

    def join_phone_customers(self, phone: DataFrame, customers: DataFrame) -> DataFrame:
        self.__logger.info("Join between phone and customers")

        #return phone.join(customers, f.col("customer_id") == f.col("delivery_id"), "inner")
        #return phone.join(customers, ["delivery_id", "customer_id"], "inner") # original
        return phone.join(customers, [i.delivery_id.name, i.customer_id.name], c.INNER_TYPE)



    def customers_vips(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Filting customers VIP")

        # ORIGINAL
        #vips = f.when( (f.col("prime") == "Yes") & (f.col("price_product") > 7500), f.lit("Yes") )\
        #    .otherwise(f.lit("No"))

        vips = f.when(( i.prime() == c.YES_WORK ) & ( i.price_product() > c.SEVEN_FIVE_THOUSAND ), f.lit(c.YES_WORK)) \
            .otherwise(f.lit(c.NOT_WORK))

        #return df.select(*df.columns, vips.alias("customer_vip")).filter( (f.col("prime") == "Yes") &
        #                                                                (f.col("price_product") > 7500) )
        return df.select(*df.columns, vips.alias(c.CUSTOMER_VIP ))\
                 .filter(( i.prime()  == c.YES_WORK) & ( i.price_product() > c.SEVEN_FIVE_THOUSAND))


    def discount_extra(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Filting discount extra")

        listBrand = ["XOLO", "Siemens", "Panasonic", "BlackBerry"]
        """discount = f.when( ( f.col("prime") == "Yes" ) & ( f.col("stock_number") < 35 ) &
                           ( f.col("brand").isin( listBrand ) == False ),
                           ( f.col("price_product") * .10 ) )\
                    .otherwise(f.lit(0.0))"""

        discount = f.when((f.col("prime") == "Yes") & (f.col("stock_number") < 35) &
                          (f.col("brand").isin(listBrand) == False),
                          (f.col("price_product") * .10)) \
                    .otherwise(f.lit(0.0))

        return df.select(*df.columns, discount.alias("discount_extra")).filter(
            f.col("discount_extra") > 0.0
        )


    def price_final(self, df: DataFrame) -> DataFrame:
        listBrand = ["XOLO", "Siemens", "Panasonic", "BlackBerry"]
        """ORIGINAL
        discount = f.when(
            (f.col("prime") == "Yes") & (f.col("stock_number") < 35) &
            (f.col("brand").isin(listBrand) == False),
            (f.col("price_product") * .10)) \
            .otherwise(f.lit(0.0))"""

        discount = f.when(
            ( i.prime() == c.YES_WORK ) & ( i.stock_number  < c.THIRTY_FIVE_NUMBER ) &
            ( i.brand().isin(listBrand) == c.FALSE_VALUE ),
            ( i.price_product()  * c.TEN_DOT )) \
            .otherwise(f.lit(c.ZERO_DOT))

        #rule5 = df.select(*df.columns, discount.alias("discount_extra"), ( f.col("price_product") + f.col("takes") - f.col("discount_amount") -f.col("discount_extra") ).alias("test") )
        rule5 = df.select(*df.columns, discount.cast(FloatType()).alias(c.DISCOUNT_EXTRA) ) # original
        #rule5 = df.select(*df.columns, discount.cast(FloatType()) )

        rule6 = rule5.withColumn(c.FINAL_PRICE, i.price_product() + i.taxes() - i.discount_amount() -
                                 o.discount_extra() )

        ruleValue = rule6.select(f.avg(c.FINAL_PRICE)).first()[c.ZERO_INT]

        #return price

        return rule6.withColumn(c.AVERAGE_FINAL_PRICE, f.lit(ruleValue))


    # nueva funcion para regla 6 price_final
    def price_finalNew(self, df: DataFrame) -> DataFrame:
        #rule5 = df.select(*df.columns, discount.cast(FloatType()) )

        rule6 = df.withColumn(c.FINAL_PRICE, i.price_product() + i.taxes() - i.discount_amount() -
                                 o.discount_extra())

        # ruleValue = rule6.select(f.avg("final_price")).first()[0] # ORIGINAL
        ruleValue = rule6.select(f.avg(c.FINAL_PRICE)).first()[c.ZERO_INT]
        #return price

        # return rule6.withColumn("average_final_price", f.lit(ruleValue)) # ORIGINAL

        return rule6.withColumn(c.AVERAGE_FINAL_PRICE, f.lit(ruleValue))


    def top50(self, df: DataFrame) -> DataFrame:
        # window = Window.partitionBy(f.col("brand")).orderBy(f.col("final_price").desc()) # original
        window = Window.partitionBy( i.brand()).orderBy( o.final_price().desc())


        return df.select(*df.columns,
                         f.dense_rank().over(window).cast(IntegerType()).alias(o.top_50.name)
                         ).filter( o.top_50() <= c.FIFTY_NUMBER )


    def notNullDogs(self, df: DataFrame) -> DataFrame:
        print("notNullDogs")
        #print(df.filter(f.col("nfc").isNull()).count())

        # df = df.na.fill("No", ["nfc"]) # original
        df = df.na.fill(c.NOT_WORK, [i.nfc.name])

        #return df.select(*df.columns).filter(f.col("nfc") == "No")
        return df.select(*df.columns).filter(i.nfc() == c.NOT_WORK)



    def jwk_date(self, df: DataFrame, field: str ) -> DataFrame:
        #return df.select(*df.columns, f.lit(field).alias("jwk_date")) # Original
        return df.select(*df.columns, f.lit(field).alias(o.jwk_date.name))


    def birthday(self, df: DataFrame, date : str) -> DataFrame:
        #return df.select(*df.columns, f.current_date().alias("current_date"), (f.round(f.months_between( f.current_date(), f.to_date(f.col("birth_date"), "MM-dd-yyyy") ) / f.lit(12) ).alias("age")))
        #return df.select(*df.columns, f.to_date(date, "MM-dd-yyyy").alias("date now"), (f.round(f.months_between( f.to_date(date, "MM-dd-yyyy"), f.to_date(f.col("birth_date"), "MM-dd-yyyy") ) / f.lit(12) ).alias("age")))
        #df = df.select(*df.columns, f.lit(date).alias("birthd1") ) # Original
        #df = df.select(*df.columns, (f.round(f.months_between( f.col("birthd1"), f.to_date(f.col("birth_date"), "MM-dd-yyyy") ) / f.lit(12) ).alias("age")) ) # Original
        #return df.drop(f.col("birthd1")) # Original

        df = df.select(*df.columns, f.lit(date).alias(o.birthd1.name))
        df = df.select(*df.columns, (
            f.round(f.months_between(o.birthd1(), f.to_date(o.birthd1(), c.FORMAT_DATE)) / f.lit(c.TWENTY)).alias(
                o.age.name)))
        return df.drop(o.birthd1())


    # video 7
    def select_all_columns(self, df: DataFrame) -> DataFrame:
        """df.select(
            f.col("cod_producto").cast(StringType()),
            f.col("cod_iuc").cast(StringType()),
            f.col("cod_titular").cast(StringType()),
            f.col("fec_alta").cast(StringType()),
            f.col("activo").cast(BooleanType()),
            f.col("cod_client").cast(StringType()),
            f.col("nombre").cast(StringType()),
            f.col("edad").cast(IntegerType()),
            f.col("provincia").cast(StringType()),
            f.col("cod_postal").cast(IntegerType()),
            f.col("vip").cast(BooleanType()),
            f.col("desc_producto").cast(StringType()),
            f.col("hash").cast(StringType())
                  )"""
        df.select(
            o.cod_producto().cast(StringType()),
            o.cod_iuc().cast(StringType()),
            o.cod_titular().cast(StringType()),
            o.fec_alta().cast(StringType()),
            o.activo().cast(BooleanType()),
            o.cod_client().cast(StringType()),
            o.nombre().cast(StringType()),
            o.edad().cast(IntegerType()),
            o.provincia().cast(StringType()),
            o.cod_postal().cast(IntegerType()),
            o.vip().cast(BooleanType()),
            o.desc_producto().cast(StringType()),
            o.hash().cast(StringType())
        )

    def select_all_columns2(self, df: DataFrame) -> DataFrame:
        """
        return df.select(
            f.col("city_name").cast(StringType()),
            f.col("street_name").cast(StringType()),
            f.col("credit_card_number").cast(StringType()),
            f.col("last_name").cast(StringType()),
            f.col("first_name").cast(StringType()),
            f.col("age").cast(IntegerType()),
            f.col("brand").cast(StringType()),
            f.col("model").cast(StringType()),
            f.col("nfc").cast(StringType()),
            f.col("country_code").cast(StringType()),
            f.col("prime").cast(StringType()),
            f.col("customer_vip").cast(StringType()), #
            f.col("taxes").cast(DecimalType(9, 2)),
            f.col("price_product").cast(DecimalType(9, 2)),
            f.col("discount_amount").cast(DecimalType(9, 2)),
            f.col("discount_extra").cast(DecimalType(9, 2)).alias("extra_discount"),
            f.col("final_price").cast(DecimalType(9, 2)),
            f.col("top_50").cast(IntegerType()).alias("brands_top"),
            f.col("jwk_date").cast(DateType())
            )"""

        return df.select(

            o.city_name().cast(StringType()),
            o.street_name().cast(StringType()),
            o.credit_card_number().cast(StringType()),
            o.last_name().cast(StringType()),
            o.first_name().cast(StringType()),
            o.age().cast(IntegerType()),
            o.brand().cast(StringType()),
            o.model().cast(StringType()),
            o.nfc().cast(StringType()),
            o.country_code().cast(StringType()),
            o.prime().cast(StringType()),
            o.customer_vip().cast(StringType()),  #
            o.taxes().cast(DecimalType(c.NINE_NUMBER, c.TWO_NUMBER)),
            o.price_product().cast(DecimalType(c.NINE_NUMBER, c.TWO_NUMBER)),
            o.discount_amount().cast(DecimalType(c.NINE_NUMBER, c.TWO_NUMBER)),
            o.discount_extra().cast(DecimalType(c.NINE_NUMBER, c.TWO_NUMBER)).alias(o.extra_discount.name),
            o.final_price().cast(DecimalType(c.NINE_NUMBER, c.TWO_NUMBER)),
            o.top_50().cast(IntegerType()).alias(o.brands_top.name),
            o.jwk_date().cast(DateType())
        )
