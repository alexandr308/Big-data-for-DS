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
    
    lr_paramGrid = ParamGridBuilder()\
                            .addGrid(lr.standardization, [True, False]) \
                            .addGrid(lr.elasticNetParam, [0.2, 0.5, 0.8, 1.0])\
                            .build()
    
    lr_tvs = TrainValidationSplit(estimator=lr,
                           estimatorParamMaps=lr_paramGrid,
                           evaluator=RegressionEvaluator(labelCol='ctr', predictionCol='prediction', metricName='rmse'),
                           trainRatio=0.8)
    
    lr_pipeline = Pipeline(stages=[features, lr_tvs])
    lr_model = lr_pipeline.fit(train)
    lr_prediction = lr_model.transform(test)
    lr_evaluator = RegressionEvaluator(labelCol='ctr', predictionCol='prediction', metricName='rmse')
    lr_tvs_rmse = lr_evaluator.evaluate(lr_prediction)
    print('LR RMSE', lr_tvs_rmse)
    
    rf = RandomForestRegressor(numTrees=10, 
                          maxDepth=10,
                          bootstrap=True,
                          seed=42,
                          labelCol='ctr',
                          featuresCol='features')
    
    rf_paramGrid = ParamGridBuilder()\
                        .addGrid(rf.maxDepth, [4, 7, 10]) \
                        .addGrid(rf.numTrees, [10, 20])\
                        .build()
    
    rf_tvs = TrainValidationSplit(estimator=rf,
                           estimatorParamMaps=rf_paramGrid,
                           evaluator=RegressionEvaluator(labelCol='ctr', predictionCol='prediction', metricName='rmse'),
                           trainRatio=0.8)
    
    rf_pipeline = Pipeline(stages=[features, rf_tvs])
    rf_model = rf_pipeline.fit(train)
    rf_prediction = rf_model.transform(test)
    rf_evaluator = RegressionEvaluator(labelCol='ctr', predictionCol='prediction', metricName='rmse')
    rf_tvs_rmse = rf_evaluator.evaluate(rf_prediction)
    print('RF RMSE', rf_tvs_rmse)
    
    if lr_tvs_rmse > rf_tvs_rmse:
        rf_model.stages[-1].bestModel.write().overwrite().save(MODEL_PATH)
        print('MaxDepth: {}'.format(rf_model.stages[-1].bestModel._java_obj.getMaxDepth()))
        print('NumTrees: {}'.format(rf_model.stages[-1].bestModel._java_obj.getNumTrees()))
    else:
        lr_model.stages[-1].bestModel.write().overwrite().save(MODEL_PATH)
        print('standardization: {}'.format(lr_model.stages[-1].bestModel._java_obj.getStandardization()))
        print('elasticNetParam: {}'.format(lr_model.stages[-1].bestModel._java_obj.getElasticNetParam()))
        
    print('Task completed successfully')


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
