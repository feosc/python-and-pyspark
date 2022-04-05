# Databricks notebook source
##Biblioteca utilizada

from pyspark.sql.functions import *

# COMMAND ----------

#Conectando no Blob

container = 'wasbs://felipe-coelho@stgestudos.blob.core.windows.net/'

def connect_to_blob():
    #Sem KeyVault por motivos de configuração
    storage_account_access_key = 'LZtcOrTCuH88dGWTvRBFnq0g6DJnAFcvigqTlU2wI0b2qfpfl4kB2W0u/QxajuW1Z1lJ967qGWjN+ASthdLetw=='
    spark.conf.set("fs.azure.account.key.stgestudos.blob.core.windows.net", storage_account_access_key)
       
connect_to_blob()

# COMMAND ----------

#Caminho referente aos arquivos que desejo consumir no BLOB

space = 'labs4/bronze/df_space.json'
path_space = container + space
space = spark.read.format("json").load(path_space , inferSchema = True, header = True)

event = 'labs4/bronze/df_event.json'
path_event = container + event
event = spark.read.format("json").load(path_event , inferSchema = True, header = True)

# COMMAND ----------

##Gerando Dataframes referente a modelagem feita das tabelas STAGES

#Dataframe do Museu
df_space = space.select(
    col('id').cast('string'),
    col('name').cast('string'),
    col('shortDescription').cast('string'),
    col('En_Estado').cast('string'),
    col('geoEstado').cast('string'),
    col('endereco').cast('string'),
    col('terms.mus_area').cast('string'),
    col('location.latitude').cast('string'),
    col('location.longitude').cast('string'))

#Dataframe do Evento
df_event = event.select(
    col('occurrences.space.id').cast('string'),
    col('occurrences.rule.startsOn').cast('string'),
    col('occurrences.rule.description').cast('string'),
    col('shortDescription').cast('string'),
    col('occurrences.space.En_Estado').cast('string'),
    col('owner.name').cast('string'))

display(df_event)

# COMMAND ----------

##Retirando os Colchetes do Dataframe de Eventos

df_event = df_event.withColumn('id' ,          regexp_replace('id', "\\[", ""))
df_event = df_event.withColumn('id' ,          regexp_replace('id', "\\]", ""))
df_event = df_event.withColumn('startsOn' ,    regexp_replace('startsOn', "\\]", ""))
df_event = df_event.withColumn('startsOn' ,    regexp_replace('startsOn', "\\[", ""))
df_event = df_event.withColumn('description' , regexp_replace('description', "\\[", ""))
df_event = df_event.withColumn('description' , regexp_replace('description', "\\]", ""))
df_event = df_event.withColumn('En_Estado' ,   regexp_replace('En_Estado', "\\]", ""))
df_event = df_event.withColumn('En_Estado' ,   regexp_replace('En_Estado', "\\[", ""))

# COMMAND ----------

##Retirando os Colchetes do Dataframe Space

df_space = df_space.withColumn('mus_area' ,    regexp_replace('mus_area', "\\[", ""))
df_space = df_space.withColumn('mus_area' ,    regexp_replace('mus_area', "\\]", ""))


# COMMAND ----------

##Limpando o Array e Splitando algumas colunas

##Dataframe de Eventos
df_event = df_event.withColumn('id', split(df_event['id'], ',').getItem(0)) \
                   .withColumn('startsOn', split(df_event['startsOn'], ',').getItem(0)) \
                   .withColumn('En_Estado', split(df_event['En_Estado'], ',').getItem(0))

##Dataframa do Space com 3 novas colunas 'tipo_museu' provenientes do campo 'mus_area' que é um array com 0 ou mais valores
df_space = df_space.withColumn('tipo_museu_1', split(df_space['mus_area'], ',').getItem(0)) \
                   .withColumn('tipo_museu_2', split(df_space['mus_area'], ',').getItem(1)) \
                   .withColumn('tipo_museu_3', split(df_space['mus_area'], ',').getItem(2)) 

display(df_event)

# COMMAND ----------

#Função para verificar o compriimento da maior string de cada coluna e contar quantas rows existem na tabela

def charMax_count(df):

    charMax = df.select(
                        [length(col)\
                        for col in df.columns]
                       )

    charMax.groupby()\
           .max()\
           .display()

    df.count()
    
charMax_count(df_event)
charMax_count(df_space)

# COMMAND ----------

##Função para escrever Dataframes no Blob

def write_blob(df, name):
    
    file_output_name = "part-"
    
    output_blob_folder = f'{container}/labs4_databricks'
    (df.coalesce(1)
     .write
     .mode("overwrite")
     .option("header", "true")
     .format("com.databricks.spark.csv")
     .save(output_blob_folder)
    )

    files = dbutils.fs.ls(output_blob_folder)
    output_file = [x for x in files if x.name.startswith(file_output_name)]

    #Caminho da pasta que deseja salvar os dataframes
    dbutils.fs.mv(output_file[0].path, f"{container}/labs4/silver/{name}")

##Chamando a função passando o dataframe e o nome que desejo registrar no Blob

write_blob(df_event, 'df_event')
write_blob(df_space, 'df_space')
