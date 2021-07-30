import io
import sys

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit

from pyspark.sql import SparkSession

import numpy as np

# Используйте как путь куда сохранить модель
MODEL_PATH = 'spark_ml_model'


def process(spark, train_data, test_data):
    train = spark.read.parquet(train_data)
    test = spark.read.parquet(test_data)
    
    features = VectorAssembler(
        inputCols=['has_video', 'is_cpm', 'is_cpc', 'ad_cost', 'day_count', 'target_audience_count'],
        outputCol='features'
    )
    
    lr = LinearRegression(solver='normal', 
                          loss='squaredError',
                          labelCol='ctr',
                          featuresCol='features')
    
    paramGrid = ParamGridBuilder()\
                    .addGrid(lr.standardization, [True, False]) \
                    .addGrid(lr.elasticNetParam, [0.2, 0.5, 0.8, 1.0])\
                    .build()
    
    tvs = TrainValidationSplit(estimator=lr,
                    estimatorParamMaps=paramGrid,
                    evaluator=RegressionEvaluator(labelCol='ctr', predictionCol='prediction', metricName='rmse'),
                    trainRatio=0.8)
    
    pipeline = Pipeline(stages=[features, tvs])
    p_model = pipeline.fit(train)
    prediction = p_model.transform(test)
    evaluator = RegressionEvaluator(labelCol='ctr', predictionCol='prediction', metricName='rmse')
    tvs_rmse = evaluator.evaluate(prediction)
    print('RMSE on test dataset is', np.round(tvs_rmse, 3))
    print('Task completed successfully')
    
    p_model.write().overwrite().save(MODEL_PATH)


def main(argv):
    train_data = argv[0]
    print("Input path to train data: " + train_data)
    test_data = argv[1]
    print("Input path to test data: " + test_data)
    spark = _spark_session()
    process(spark, train_data, test_data)


def _spark_session():
    return SparkSession.builder.appName('PySparkMLFitJob').getOrCreate()


if __name__ == "__main__":
    arg = sys.argv[1:]
    if len(arg) != 2:
        sys.exit("Train and test data are require.")
    else:
        main(arg)
