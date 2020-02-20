package dfbasics

import org.apache.spark.sql.SparkSession

object CreatingDFWithCSVUsingSS {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("Creating DF using CSV in Spark 2.x way using spark session")
      .getOrCreate()

    val df = sparkSession.read.option("sep","\t")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/reviews.tsv")

    df.printSchema()

    df.show()
  }
}
