# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Creation de my_spark
my_spark = SparkSession.builder.appName("my_spark").getOrCreate()

# Print my_spark
print(my_spark)

# Lecture du fichier csv:
ventes_df = my_spark.read.csv("ventes.csv", header=True, inferSchema=True)

ventes_df.show()


# Nettoyage des données :
ventes_df.columns