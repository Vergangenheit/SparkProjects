package dfbasics

import org.apache.spark.sql.SparkSession


object FileFormats {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DF field formats")
      .master("local")
      .getOrCreate()

    val jsonDF = spark.read.json("src/main/resources/bitcoin.json")
    jsonDF.printSchema()
    jsonDF.show()

  }
}
