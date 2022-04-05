# Databricks notebook source
#Localizando o StorageAccount que desejo ler, com a senha de acesso e formato do arquivo

storage_account_name = 'stgestudos'
storage_account_access_key = 'j+Ji7MO3YLhkwCarp8OCzbgy2ENitNs09cF8Sm2q2IU1UZGo82VKSmuVXidZeUEn4/ljKtItIB1ZW6JVd1YAuQ=='

file_location = f"wasbs://felipe-coelho@{storage_account_name}.blob.core.windows.net/"
file_type = "csv"

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

#Caminho referente ao arquivo que desejo consumir no BLOB

dolar_file = "dolar_bronze/dolar_raw"
path = file_location + dolar_file

#Criando data frame com informaçoes sobre meu arquivo

df = spark.read.format("csv").load(path , inferSchema = True, header = True)

#Mostrando meu Dataframe
display(df)

# COMMAND ----------

from pyspark.sql.functions import *

df_replace = df

df_replace = df_replace.withColumn("cotacaoCompra" , regexp_replace("cotacaoCompra", ",", "."))
df_replace = df_replace.withColumn("cotacaoVenda" , regexp_replace("cotacaoCompra", ",", "."))
df_replace = df_replace.withColumn("dataHoraCotacao", to_timestamp("dataHoraCotacao"))

display(df_replace)

# COMMAND ----------

df = df_replace

df = df.select(
      col("cotacaoCompra").cast("float"),
      col("cotacaoVenda").cast("float"),
      col("dataHoraCotacao")
)

# COMMAND ----------

file_output_name = "part-"
output_blob_folder = f"{file_location}/dolar_silver"
(df
 .coalesce(1)
 .write
 .mode("overwrite")
 .option("header", "true")
 .format("com.databricks.spark.csv")
 .save(output_blob_folder)
)

files = dbutils.fs.ls(output_blob_folder)
output_file = [x for x in files if x.name.startswith(file_output_name)]

#move o arquivo tranformado para a pasta "dolar_final" e renomeia o arquivo ".csv" para "dolar_gold"
dbutils.fs.mv(output_file[0].path, f"{file_location}/dolar_gold/gold_dolar_final")
