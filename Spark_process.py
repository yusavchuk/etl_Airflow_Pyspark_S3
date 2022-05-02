#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType,  \
IntegerType, BooleanType, ArrayType, FloatType
import pyspark.sql.functions as func


class Spark_process:
    """Documentations"""
    
    
    @staticmethod
    def transform(title, rating):
        
        spark = SparkSession.builder.appName('Transformation').getOrCreate()
        title_schema = StructType([\
                           StructField('tconst', StringType(), True),\
                           StructField('titleType', StringType(), True),\
                           StructField('primaryTitle', StringType(), True),\
                           StructField('originalTitle', StringType(), True),\
                           StructField('isAdult', BooleanType(), True),\
                           StructField('startYear', IntegerType(), True),\
                           StructField('endYear', IntegerType(), True),\
                           StructField('runtimeMinutes', IntegerType(), True),\
                           StructField('genres', StringType(), True)
                           ])
        df_title = spark.read.csv(title, schema=title_schema, 
                                  sep='\t', header=True)
        
        df_title = df_title.drop('originalTitle', 'endYear')
        df_title = df_title.withColumn('genres', func.split(df_title['genres'], ','))
        my_genres = ['Comedy', 'Horror', 'Drama', 'Romance']
        for genre in my_genres:
            df_title = df_title.withColumn(f'{genre}_OneHot',
                          func.when(func.array_contains(func.col('genres'), 
                                            genre) == True, 1).otherwise(0))
        
        rating_schema = StructType([\
                           StructField('tconst', StringType(), True),\
                           StructField('averageRating', FloatType(), True),\
                           StructField('numVotes', IntegerType(), True)\
                           ])
        df_rating = spark.read.csv(rating, sep='\t', header=True, 
                                   schema=rating_schema)
        
        df_main = df_title.join(func.broadcast(df_rating), ['tconst'], "inner")
        
        df_main.write.parquet('/home/yu_savchuk/airflow/dags/title_rating.parquet')


if __name__ == '__main__':
    title = '/home/yu_savchuk/airflow/dags/title.basics.tsv'
    rating = '/home/yu_savchuk/airflow/dags/title.ratings.tsv'
    
    Spark_process.transform(title, rating)
    
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        