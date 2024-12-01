from pyspark.sql import SparkSession

def transform_data(input_path, output_path):
    spark = SparkSession.builder.appName("Simple ETL").getOrCreate()
    df = spark.read.option("header", "true").csv(input_path)
    filtered_df = df.filter(df["amount"].cast("int") > 100)
    filtered_df.write.option("header", "true").csv(output_path)
