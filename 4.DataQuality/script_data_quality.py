from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql import DataFrame
import logging
import sys

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

def data_quality_analysis(df: DataFrame) -> DataFrame:
    logger.info("Iniciando análise de Data Quality")
    data_quality_count = 0

    qtd_linhas = df.count()
    if qtd_linhas == 0:
        logger.error("Nenhum registro foi encontrado no Dataframe")
        data_quality_count += 1

    
    schema = {
        "cod_cliente" : IntegerType(),
        "nm_cliente" : StringType(),
        "nm_pais_cliente" : StringType(),
        "nm_cidade_cliente" : StringType(),
        "nm_rua_cliente" : StringType(),
        "num_casa_cliente" : StringType(),
        "num_telefone_cliente" : StringType(),
        "dt_nascimento_cliente" : DateType(),
        "dt_atualizacao" : DateType(),
        "tp_pessoa" : StringType(),
        "vl_renda" : FloatType(),
        "anomesdia" : StringType()
    }

    for column, type in schema.items():
        if column not in df.columns:
            logger.error(f"Coluna {column} não encontrada")
            data_quality_count += 1
        elif df.schema[column].dataType != type:
            logger.error(f"Coluna {column} com data type incorreto: Encontrado: {df.schema[column].dataType }, Esperado: {type}")


    duplicated_clients = (df.groupBy("cod_cliente").count().filter(col("count") > 1)).count()
    if duplicated_clients > 0:
        logger.error(f"Quantidade de cod_cliente duplicados: {duplicated_clients}")
        data_quality_count += 1

    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        if null_count > 0:
            logger.error(f"Coluna {column} possui {null_count} registros nulos")
            data_quality_count += 1

    
    if data_quality_count >0:
        logger.error(f"Data Quality falhou com {data_quality_count} inconsistências encontradas")