# Copyright 2022 Google LLC
# 
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https: // www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

if len(sys.argv) == 1:
    print("Please provide a dataset name.")

dataset = sys.argv[1]
table = "bigquery-public-data:new_york_citibike.citibike_trips"

spark = SparkSession.builder \
    .appName("pyspark-example") \
    .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.26.0.jar") \
    .getOrCreate()

df = spark.read.format("bigquery").load(table)

top_ten = df.filter(F.col("start_station_id")
                    .isNotNull()) \
            .groupBy("start_station_id") \
            .count() \
            .orderBy("count", ascending=False) \
            .limit(10) \
            .cache()

top_ten.show()

# table = f"{dataset}.citibikes_top_ten_start_station_ids"
# Saving the data to BigQuery
# top_ten.write.format('bigquery') \
#   .option("writeMethod", "direct") \
#   .option("table", table) \
#   .save()

# print(f"Data written to BigQuery table: {table}.citibikes_top_ten_start_station_ids")

# --------------------------------------------------------
#Crie código com PySpark para calcular a duração média das viagens e exiba as 10 mais lentas. 

avg_duration = df.agg(F.avg("tripduration")).first()[0]
print(f"Duração média das viagens: {avg_duration}")
 
slowest_trips = df.orderBy("tripduration", ascending=False).limit(10)
slowest_trips = slowest_trips.withColumn("avgtripduration", F.lit(avg_duration))
slowest_trips.show()

# -----------------------------------------------------------
#Crie código com PySpark para calcular a duração média das viagens e exiba as 10 mais rápidas (remova as sem duração)

df_filtered = df.filter(df.tripduration > 0)
 
avg_duration = df_filtered.agg(F.avg("tripduration")).first()[0]
print(f"Duração média das viagens desconsiderando as viagens sem duração: {avg_duration}")
 
fastest_trips = df_filtered.orderBy("tripduration", ascending=True).limit(10)
fastest_trips = fastest_trips.withColumn("avgtripduration", F.lit(avg_duration))
fastest_trips.show()

# -----------------------------------------------------------
#Crie código com PySpark para calcular a duração média das viagens para cada par de estações. Em seguida,  ordene-as e exiba as 10 mais mais lentas.
station_pairs_avg_duration = df.groupBy("start_station_id", "end_station_id") \
                               .agg(F.avg("tripduration").alias("avgtripduration"))
 
slowest_station_pairs = station_pairs_avg_duration.orderBy("avgtripduration", ascending=False).limit(10)
slowest_station_pairs.show()

# -----------------------------------------------------------
# Crie código com PySpark para calcular o total de viagens por bicicleta e exibe as 10 bicicletas mais utilizadas
trips_per_bike = df.groupBy("bikeid").agg(F.count("*").alias("total_trips"))
 
most_used_bikes = trips_per_bike.orderBy(F.col("total_trips").desc()).limit(10)
most_used_bikes.show()

# -----------------------------------------------------------
# Crie código com PySpark para calcular a média de idade dos clientes

current_year = int(F.year(F.current_date()).collect()[0][0])
df_age = df.withColumn("age", current_year - F.col("birth_year"))

avg_age = df_age.agg(F.avg("age")).first()[0]
print(f"Média de idade dos clientes: {avg_age:.2f} anos")

# -----------------------------------------------------------
# Crie código com PySpark para calcular a distribuição entre gêneros

gender_counts = df.groupBy("gender").agg(F.count("*").alias("count"))
 
total_count = df.count()
gender_distribution = gender_counts.withColumn("percentage", F.round(F.col("count") / total_count * 100, 2))

gender_distribution.show()
