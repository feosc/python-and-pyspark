# Databricks notebook source
#Coletando arquivo de consumo no Blob

# COMMAND ----------

storage_account_name = 'stgestudos'
storage_account_access_key = 'j+Ji7MO3YLhkwCarp8OCzbgy2ENitNs09cF8Sm2q2IU1UZGo82VKSmuVXidZeUEn4/ljKtItIB1ZW6JVd1YAuQ=='

file_location = f"wasbs://felipe-coelho@{storage_account_name}.blob.core.windows.net/"
file_type = "csv"

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

orders_file = "orders_bronze/orders.csv"
path = file_location + orders_file

# COMMAND ----------

#Criando Dataframe

# COMMAND ----------

df = spark.read.format("csv").load(path , inferSchema = True, header = True)

# COMMAND ----------

#Renomeando Colunas

# COMMAND ----------

df = df.withColumnRenamed("Item Type", "Item_Type") \
        .withColumnRenamed("Unit Price", "Unit_Price") \
        .withColumnRenamed("Unit Cost", "Unit_Cost") \
        .withColumnRenamed("Total Revenue", "Total_Revenue")\
        .withColumnRenamed("Total Cost", "Total_Cost") \
        .withColumnRenamed("Total Profit", "Total_Profit") \
        .withColumnRenamed("Sales Channel", "Sales_Channel") \
        .withColumnRenamed("Order Priority", "Order_Priority") \
        .withColumnRenamed("Order Date", "Order_Date") \
        .withColumnRenamed("Order ID", "Order_ID") \
        .withColumnRenamed("Ship Date", "Ship_Date") \
        .withColumnRenamed("Units Sold", "Units_Sold")

# COMMAND ----------

#Substituindo Virgulas por Pontos

# COMMAND ----------

from pyspark.sql.functions import *

df_replace = df

df_replace = df_replace.withColumn("Unit_Price" ,    regexp_replace("Unit_Price", ",", "."))
df_replace = df_replace.withColumn("Unit_Cost" ,     regexp_replace("Unit_Cost", ",", "."))
df_replace = df_replace.withColumn("Total_Revenue" , regexp_replace("Total_Revenue", ",", "."))
df_replace = df_replace.withColumn("Total_Cost" ,    regexp_replace("Total_Cost", ",", "."))
df_replace = df_replace.withColumn("Total_Profit" ,  regexp_replace("Total_Profit", ",", "."))

# COMMAND ----------

#Tipando e transformando Coluna de Datas

# COMMAND ----------

df = df_replace

df = df.withColumn("Order_Date", to_date("Order_Date", "dd/MM/yyyy"))
df = df.withColumn("Ship_Date", to_date("Ship_Date", "dd/MM/yyyy"))

# COMMAND ----------

#Convertendo Tipo de Colunas

# COMMAND ----------

df = df.select(
        col('Region').cast('string'),
        col('Country').cast('string'),
        col('Item_Type').cast('string'),
        col('Sales_Channel').cast('string'),
        col('Order_Priority').cast('string'),
        col('Order_Date'),
        col('Order_ID').cast('int'),
        col('Ship_Date'),
        col('Units_Sold').cast('int'),
        col('Unit_Price').cast('float'),
        col('Unit_Cost').cast('float'),
        col('Total_Revenue').cast('float'),
        col('Total_Cost').cast('float'),
        col('Total_Profit').cast('float')
)

# COMMAND ----------

#Mostrando DataFrame

# COMMAND ----------

display(df)

# COMMAND ----------

#Carregando Dataframe Transformado na tabela Stage SQL

# COMMAND ----------

def writeAzureSQLDB(df1, table_write):
        
    jdbcHostname = 'sql-estudo.database.windows.net'
    jdbcDatabase = 'db-estudos'
    jdbcPort = '1433'
    jdbcUsername = 'admin-azure@sql-estudo' #jdbcUsername = dbutils.secrets.get("", "")
    jdbcPassword = 'a&Ehs&HB'               #jdbcPassword = dbutils.secrets.get(scope = "", key = "")
    
    jdbcUrl = f'jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase}'
        
    df1.select('Region',
              'Country',
              'Item_Type',
              'Sales_Channel',
              'Order_Priority',
              'Order_Date',
              'Order_ID',
              'Ship_Date',
              'Units_Sold',
              'Unit_Price',
              'Unit_Cost',
              'Total_Revenue',
              'Total_Cost',
              'Total_Profit').write.format('jdbc')\
                            .mode('overwrite')\
                            .option('url', jdbcUrl)\
                            .option('dbtable', table_write)\
                            .option('user', jdbcUsername)\
                            .option('password', jdbcPassword)\
                            .save()
    
    
writeAzureSQLDB(df, 'STAGE_Felipe_Coelho.Orders')
