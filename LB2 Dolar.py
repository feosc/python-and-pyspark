# Databricks notebook source
#Bibliotecas

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import *

# COMMAND ----------

#Lendo API e Criando Dataframe com Pandas

# COMMAND ----------

api = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@dataInicial='01-01-2019'&@dataFinalCotacao='12-31-2025'&$top=9000&$format=text/csv&$select=cotacaoCompra,cotacaoVenda,dataHoraCotacao"

df = pd.read_csv(api)

#Dataframe do Pandas para Spark

df_dolar = spark.createDataFrame(df)

# COMMAND ----------

# Função que Armazena no Banco de Dados

# COMMAND ----------

def writeAzureSQLDB(df1, table_write):
        
    jdbcHostname = ''
    jdbcDatabase = ''
    jdbcPort = '1433'
    jdbcUsername = '' #jdbcUsername = dbutils.secrets.get("", "")
    jdbcPassword = ''               #jdbcPassword = dbutils.secrets.get(scope = "", key = "")
    
    jdbcUrl = f'jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase}'
        
    df1.select('cotacaoCompra',
              'cotacaoVenda',
              'dataHoraCotacao').write.format('jdbc')\
                                .mode('overwrite')\
                                .option('url', jdbcUrl)\
                                .option('dbtable', table_write)\
                                .option('user', jdbcUsername)\
                                .option('password', jdbcPassword)\
                                .save()
    
    
writeAzureSQLDB(df_dolar, 'dolar_felipe_coelho.DOLAR_STAGE_FELIPE_COELHO')
