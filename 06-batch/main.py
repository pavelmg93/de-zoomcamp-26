def main():
    print("Hello from 06-batch!")
    import pyspark
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('test') \
        .getOrCreate()

    print(f"Spark version: {spark.version}")

    df = spark.range(10)
    df.show()

    spark.stop()


if __name__ == "__main__":
    main()
