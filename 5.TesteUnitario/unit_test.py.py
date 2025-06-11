import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
from pyspark.sql.functions import col
from script import check_phone_format

schema =  StructType([
    StructField("cod_cliente", IntegerType(), True),
    StructField("num_telefone_cliente", StringType(), True)
])

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("pytest").getOrCreate()

def test_check_phone_format_return_values(spark):

    input_data = [
        (99, "(99)99999-9999"), # OK
        (1, "99999999999"), # Sem Hífen e sem parênteses
        (2, "(99)999999999"), # Sem hífen
        (3, "9999999-9999"), # Sem parênteses
        (4, "(99)9999-9999"), # QUantidade de dígitos menor
        (5, "(99)99999-99999"), # Quantidade de dígitos maior
        (6, "(99) 99999-9999"), # Espaço
        (7, "(99)99999 9999"), # Hífen por espaço
        (8, " 99 99999-9999"), # Parênteses por espaço
        (9, "(AA)BBBBB-CCCC"), # Letras formatadas
        (10, "(99)99999!9999"), # Hífen por caractere especial
        (11, "!99!99999-9999"), # Parentêses por caractere especial 
        (12, "abcde"), # Letras
        (13, None), # Nulo
        (14, " "), # Vazio
        (15, "") # Vazio
    ]

    df_input = spark.createDataFrame(input_data, schema)

    df_return = check_phone_format(df_input, "num_telefone_cliente")
    df_return = df_return.filter(col("num_telefone_cliente").isNotNull())
    cod_cliente_return = df_return.collect()[0]["cod_cliente"]

    assert df_return.count() == 1
    assert cod_cliente_return == 99

def test_check_phone_format_empty_dataframe(spark):

    df_input = spark.createDataFrame([], schema)
    df_return = check_phone_format(df_input, "num_telefone_cliente")

    assert df_return.count() == 0

def test_check_phone_format_wrong_column(spark):

    input_data = [
        (1, "(11)11111-1111"),
        (2, "(22)22222-2222")
    ]

    df_input = spark.createDataFrame(input_data, schema)

    with pytest.raises(Exception) as e:
        df_return = check_phone_format(df_input, "column")

