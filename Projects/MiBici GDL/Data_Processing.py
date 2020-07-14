
from pyspark.sql import SparkSession, Row
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number




def get_spark_session():
    return SparkSession.builder.appName("DATA_PROCESSING").getOrCreate()

def read_csv(spark, path):
    return spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path)

def save_df(df, path):
    return df.coalesce(1).write.option("header", "true").csv(path)


def main():

    spark = get_spark_session()
    january_details_df = read_csv(spark, "/Users/miguelojeda/Google Drive/Maestría/TOG/Desarrollo del proyecto/Datasets/Bici GDL/2019/Enero.csv")
    february_details_df = read_csv(spark, "/Users/miguelojeda/Google Drive/Maestría/TOG/Desarrollo del proyecto/Datasets/Bici GDL/2019/Febrero.csv")
    march_details_df = read_csv(spark, "/Users/miguelojeda/Google Drive/Maestría/TOG/Desarrollo del proyecto/Datasets/Bici GDL/2019/Marzo.csv")
    april_details_df = read_csv(spark, "/Users/miguelojeda/Google Drive/Maestría/TOG/Desarrollo del proyecto/Datasets/Bici GDL/2019/Abril.csv")
    may_details_df = read_csv(spark, "/Users/miguelojeda/Google Drive/Maestría/TOG/Desarrollo del proyecto/Datasets/Bici GDL/2019/Mayo.csv")
    june_details_df = read_csv(spark, "/Users/miguelojeda/Google Drive/Maestría/TOG/Desarrollo del proyecto/Datasets/Bici GDL/2019/Junio.csv")
    july_details_df = read_csv(spark, "/Users/miguelojeda/Google Drive/Maestría/TOG/Desarrollo del proyecto/Datasets/Bici GDL/2019/Julio.csv")
    august_details_df = read_csv(spark, "/Users/miguelojeda/Google Drive/Maestría/TOG/Desarrollo del proyecto/Datasets/Bici GDL/2019/Agosto.csv")
    september_details_df = read_csv(spark, "/Users/miguelojeda/Google Drive/Maestría/TOG/Desarrollo del proyecto/Datasets/Bici GDL/2019/Septiembre.csv")
    october_details_df = read_csv(spark, "/Users/miguelojeda/Google Drive/Maestría/TOG/Desarrollo del proyecto/Datasets/Bici GDL/2019/Octubre.csv")
    november_details_df = read_csv(spark, "/Users/miguelojeda/Google Drive/Maestría/TOG/Desarrollo del proyecto/Datasets/Bici GDL/2019/Noviembre.csv")
    december_details_df = read_csv(spark, "/Users/miguelojeda/Google Drive/Maestría/TOG/Desarrollo del proyecto/Datasets/Bici GDL/2019/Diciembre.csv")

    january_details_df.createOrReplaceTempView("JANUARY_DETAILS")
    february_details_df.createOrReplaceTempView("FEBRUARY_DETAILS")
    march_details_df.createOrReplaceTempView("MARCH_DETAILS")
    april_details_df.createOrReplaceTempView("APRIL_DETAILS")
    may_details_df.createOrReplaceTempView("MAY_DETAILS")
    june_details_df.createOrReplaceTempView("JUNE_DETAILS")
    july_details_df.createOrReplaceTempView("JULY_DETAILS")
    august_details_df.createOrReplaceTempView("AUGUST_DETAILS")
    september_details_df.createOrReplaceTempView("SEPTEMBER_DETAILS")
    october_details_df.createOrReplaceTempView("OCTOBER_DETAILS")
    november_details_df.createOrReplaceTempView("NOVEMBER_DETAILS")
    december_details_df.createOrReplaceTempView("DECEMBER_DETAILS")

    january_travels = spark.sql("SELECT 'JANUARY' AS MONTH, COUNT(DISTINCT(Usuario_Id)) AS TOTAL_USERS FROM JANUARY_DETAILS")
    february_travels = spark.sql("SELECT 'FEBRUARY' AS MONTH, COUNT(DISTINCT(Usuario_Id)) AS TOTAL_USERS FROM FEBRUARY_DETAILS")
    march_travels = spark.sql("SELECT 'MARCH' AS MONTH, COUNT(DISTINCT(Usuario_Id)) AS TOTAL_USERS FROM MARCH_DETAILS")
    april_travels = spark.sql("SELECT 'APRIL' AS MONTH, COUNT(DISTINCT(Usuario_Id)) AS TOTAL_USERS FROM APRIL_DETAILS")
    may_travels = spark.sql("SELECT 'MAY' AS MONTH, COUNT(DISTINCT(Usuario_Id)) AS TOTAL_USERS FROM MAY_DETAILS")
    june_travels = spark.sql("SELECT 'JUNE' AS MONTH, COUNT(DISTINCT(Usuario_Id)) AS TOTAL_USERS FROM JUNE_DETAILS")
    july_travels = spark.sql("SELECT 'JULY' AS MONTH, COUNT(DISTINCT(Usuario_Id)) AS TOTAL_USERS FROM JULY_DETAILS")
    august_travels = spark.sql("SELECT 'AUGUST' AS MONTH, COUNT(DISTINCT(Usuario_Id)) AS TOTAL_USERS FROM AUGUST_DETAILS")
    september_travels = spark.sql("SELECT 'SEPTEMBER' AS MONTH, COUNT(DISTINCT(Usuario_Id)) AS TOTAL_USERS FROM SEPTEMBER_DETAILS")
    october_travels = spark.sql("SELECT 'OCTOBER' AS MONTH, COUNT(DISTINCT(Usuario_Id)) AS TOTAL_USERS FROM OCTOBER_DETAILS")
    november_travels = spark.sql("SELECT 'NOVEMBER' AS MONTH, COUNT(DISTINCT(Usuario_Id)) AS TOTAL_USERS FROM NOVEMBER_DETAILS")
    december_travels = spark.sql("SELECT 'DECEMBER' AS MONTH, COUNT(DISTINCT(Usuario_Id)) AS TOTAL_USERS FROM DECEMBER_DETAILS")

    total_travels_details = january_travels.union(february_travels).union(march_travels).union(april_travels).union(may_travels).union(june_travels).union(july_travels).union(august_travels).union(september_travels).union(october_travels).union(november_travels).union(december_travels)

    sorted_df = total_travels_details.sort("TOTAL_USERS")
    final_df = sorted_df.withColumn("ID", row_number().over(Window.orderBy("TOTAL_USERS")))
    save_df(final_df, "/Users/miguelojeda/Google Drive/Maestría/TOG/Desarrollo del proyecto/Spark/data_to_be_analyzed")


if __name__ == '__main__':
    main()