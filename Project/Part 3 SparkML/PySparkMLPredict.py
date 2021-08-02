import io
import sys

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegressionModel, RandomForestRegressionModel
from pyspark.sql import SparkSession

import numpy as np

# Используем как путь откуда загрузить модель
MODEL_PATH = 'spark_ml_model'


def process(spark, input_file, output_file):
    test = spark.read.parquet(input_file)
    
    try:
        model = LinearRegressionModel.load(MODEL_PATH)
    except:
        model = RandomForestRegressionModel.load(MODEL_PATH)
        
    features = VectorAssembler(
        inputCols=['has_video', 'is_cpm', 'is_cpc', 'ad_cost', 'day_count', 'target_audience_count'],
        outputCol='features'
    )
    df_test = features.transform(test)
        
    prediction = model.transform(df_test)
    prediction[['ad_id', 'prediction']].coalesce(1).write.parquet(output_file) 
    
    print('Task completed successfully')   


def main(argv):
    input_path = argv[0]
    print("Input path to file: " + input_path)
    output_file = argv[1]
    print("Output path to file: " + output_file)
    spark = _spark_session()
    process(spark, input_path, output_file)


def _spark_session():
    return SparkSession.builder.appName('PySparkMLPredict').getOrCreate()


if __name__ == "__main__":
    arg = sys.argv[1:]
    if len(arg) != 2:
        sys.exit("Input and Target path are require.")
    else:
        main(arg)
