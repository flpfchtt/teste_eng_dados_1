import sys
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job 
from pyspark.sql import SparkSession, DataFrame
from awsglue.utils import GetResolvedOptions
from pyspark import StorageLevel
from datetime import datetime
import pytz
from pyspark.sql.functions import col, lit, upper, when, row_number, desc
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql.window import Window 
from script_data_quality import data_quality_analysis
import logging

logger = logging.getLogger()
log_format = "%(message)s"
date_format_log = "%Y-%m-%d %H:%M:%S"
log_stream = sys.stdout

if logger.handlers:
    for handler in logger.handlers:
        logger.removeHandler(handler)

logging.basicConfig(
    level=logging.INFO, format=log_format, stream=log_stream,
    datefmt=date_format_log
)

def get_spark_session():
    sc = SparkContext.getOrCreate()
    gc = GlueContext(sc)
    spark = gc.spark_session 
    return sc, gc, spark

def get_process_date() -> str:
    """
    Gera uma string da data atual no formato anomesdia para ser utilizada na criação da partição física e lógica.
    """
    now = datetime.utcnow()
    now_gmt = pytz.utc.localize(now).astimezone(pytz.timezone("America/Sao_Paulo"))
    return now_gmt.strftime("%Y%m%d")

def add_column_process_date(df: DataFrame, anomesdia: str) -> DataFrame:
    """
    Adiciona a coluna anomesdia ao DataFrame com a data de processamento atual.
    """
    return df.withColumn("anomesdia", lit(anomesdia))

def column_to_uppercase(df: DataFrame, column_name: str) -> DataFrame:
    """
    Converte o conteúdo de uma coluna específica do DataFrame para letras maiúsculas.
    """
    return df.withColumn(column_name, upper(col(column_name)))

def rename_column(df: DataFrame, column_name: str, new_column_name: str) -> DataFrame:
    """
    Renomeia uma coluna do DataFrame.
    """
    return df.withColumnRenamed(column_name, new_column_name)

def get_last_row_by_date(df: DataFrame, id_column_name: str, dt_column_name: str) -> DataFrame:
    """
    Obtém a última linha de cada grupo de id_column_name com base na data dt_column_name.
    """
    window_spec = Window.partitionBy(id_column_name).orderBy(desc(dt_column_name))
    df = df.withColumn("row_number", row_number().over(window_spec))
    return df.filter("row_number = 1").drop("row_number")

def check_phone_format(df: DataFrame, column_name: str) -> DataFrame:
    """
    Verfica se os valores na coluna de telefone estão no padrão (NN)NNNNN-NNNN.
    """
    return df.withColumn(column_name, when(col(column_name).rlike(r"^\(\d{2}\)\d{5}-\d{4}$"), col(column_name)).otherwise(None))

def df_to_table(df: DataFrame, database_name: str, table_name: str, anomesdia: str):
    """
    Escreve um DataFrame em uma tabela do Glue, criando partições físicas e lógicas com base na coluna anomesdia.
    """
    glue_client = boto3.client("glue")

    table_data_response = glue_client.get_table(
        DatabaseName = database_name,
        Name = table_name
    )

    df.write.format("parquet").mode("overwrite").option("compression", "snappy").partitionBy(["anomesdia"]).save(table_data_response["Table"]["StorageDescriptor"]["Location"])

    create_partition_response = glue_client.batch_create_partition(
        DatabaseName = database_name,
        TableName = table_name,
        PartitionInputList = [{
            "Values" : [
                anomesdia
            ],
            "StorageDescriptor" : {
                "Location" : f"{table_data_response['Table']['StorageDescriptor']['Location']}/anomesdia={anomesdia}",
                "InputFormat" : table_data_response["Table"]["StorageDescriptor"]["InputFormat"],
                "OutputFormat" : table_data_response["Table"]["StorageDescriptor"]["OutputFormat"],
                "SerdeInfo" : table_data_response["Table"]["StorageDescriptor"]["SerdeInfo"],
            }
        }]
    )

def main():

    ### Inicialização do Job ###

    logger.info("Inicializando o Job")

    args = GetResolvedOptions(
        sys.argv, [
            "JOB_NAME",
            "S3_INPUT_FILE_PATH",
            "BRONZE_DATABASE_NAME",
            "BRONZE_TABLE_NAME",
            "SILVER_DATABASE_NAME",
            "SILVER_TABLE_NAME"
        ]
    )

    sc, gc, spark = get_spark_session()
    job = Job(gc)
    job.init(args["JOB_NAME"], {})
    anomesdia = get_process_date()

    ### Leitura dos dados ###

    logger.info("Etapa: Leitura dados")

    file_schema = StructType([
        StructField("cod_cliente", IntegerType(), True),
        StructField("nm_cliente", StringType(), True),
        StructField("nm_pais_cliente", StringType(), True),
        StructField("nm_cidade_cliente", StringType(), True),
        StructField("nm_rua_cliente", StringType(), True),
        StructField("num_casa_cliente", StringType(), True),
        StructField("telefone_cliente", StringType(), True),
        StructField("dt_nascimento_cliente", DateType(), True),
        StructField("dt_atualizacao", DateType(), True),
        StructField("tp_pessoa", StringType(), True),
        StructField("vl_renda", FloatType(), True),
    ])

    df = spark.read.csv(args["S3_INPUT_FILE_PATH"], header=True,schema=file_schema)

    ### Processamento e escrita camada Bronze ###

    logger.info("Etapa: Camada Bronze")

    df = add_column_process_date(df, anomesdia)
    df = column_to_uppercase(df, "nm_cliente")
    df = rename_column(df, "telefone_cliente", "num_telefone_cliente")
    df.persist(StorageLevel.MEMORY_ONLY)
    df_to_table(df, args["BRONZE_DATABASE_NAME"], args["BRONZE_TABLE_NAME"], anomesdia)

    ### Processamento e escrita camada Silver ###

    logger.info("Etapa: Camada Silver")

    df = get_last_row_by_date(df, "cod_cliente", "dt_atualizacao")
    df = check_phone_format(df, "num_telefone_cliente")

    df.persist(StorageLevel.MEMORY_ONLY)
    df_to_table(df, args["SILVER_DATABASE_NAME"], args["SILVER_TABLE_NAME"], anomesdia)

    data_quality_analysis(df)

    logger.info("Finalizando o job")

    df.unpersist()
    job.commit()

if __name__ == "__main__":
    main()