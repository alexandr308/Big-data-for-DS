import io
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff
from pyspark.sql import functions as F

# определяем пропорции разбиения данных на трейн и тест
TRAIN_TEST_SPLIT = [0.75, 0.25]


def process(spark, input_file, target_path, train_test_split):
    # сначала загружаем данные
    df = spark.read.parquet(input_file)
    
    # создаем новые признаки
    ndf = df.withColumn('is_cpm', F.when(df.ad_cost_type == 'CPM', 1).otherwise(0))\
           .withColumn('is_cpc', F.when(df.ad_cost_type == 'CPC', 1).otherwise(0))\
           .withColumn('view', F.when(df.event == 'view', 1).otherwise(0))\
           .withColumn('click', F.when(df.event == 'click', 1).otherwise(0))
    
    ndf_grouped = ndf.groupBy('ad_id')\
                 .agg(
                        F.max(ndf.target_audience_count),
                        F.max(ndf.has_video),
                        F.max(ndf.is_cpm),
                        F.max(ndf.is_cpc),
                        F.sum(ndf.ad_cost),
                        F.countDistinct(ndf.date),
                        F.sum(ndf.view),
                        F.sum(ndf.click)
                      )\
            .withColumnRenamed('max(target_audience_count)', 'target_audience_count')\
            .withColumnRenamed('max(has_video)', 'has_video')\
            .withColumnRenamed('max(is_cpm)', 'is_cpm')\
            .withColumnRenamed('max(is_cpc)', 'is_cpc')\
            .withColumnRenamed('sum(ad_cost)', 'ad_cost')\
            .withColumnRenamed('count(date)', 'day_count')\
            .withColumnRenamed('sum(view)', 'views')\
            .withColumnRenamed('sum(click)', 'clicks')\
            .withColumn('CTR', F.coalesce(col('clicks') / col('views'), col('views')))\
            .drop('clicks', 'views')
                    
    
    # разбиваем на трейн и тест
    train, test = ndf_grouped.randomSplit(train_test_split, seed=42)
    
    train.coalesce(1).write.parquet(f'{target_path}/train')
    test.coalesce(1).write.parquet(f'{target_path}/test')


def main(argv):
    input_path = argv[0]
    print("Input path to file: " + input_path)
    target_path = argv[1]
    print("Target path: " + target_path)
    spark = _spark_session()
    process(spark, input_path, target_path, train_test_split=TRAIN_TEST_SPLIT)


def _spark_session():
    return SparkSession.builder.appName('PySparkJob').getOrCreate()


if __name__ == "__main__":
    arg = sys.argv[1:]
    if len(arg) != 2:
        sys.exit("Input and Target path are require.")
    else:
        main(arg)
