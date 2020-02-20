package dfbasics

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object CreatingDFWithCSVusingSC {
  def main(args: Array[String]) : Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Createing CSV with SC")
    sparkConf.setMaster("local")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header","true")
      .option("inferSchema", "true")
      .load("src/main/resources/reviews.tsv")

    df.printSchema()

    df.show()


  }
}
