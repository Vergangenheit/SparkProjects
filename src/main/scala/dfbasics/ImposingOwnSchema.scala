package dfbasics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object ImposingOwnSchema {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("creating own schema for csv file")
      .master("local")
      .getOrCreate()

    val df = spark.read.option("header","true")
      .option("inferSchema","true")
      .csv("src/main/resources/Baby_Names__Beginning_2007.csv")

    df.printSchema()

    val ownSchema = StructType(
      StructField("Year", LongType, true) ::
      StructField("First Name", StringType, true) ::
      StructField("Country", StringType, true) ::
      StructField("Sex", StringType, true) ::
      StructField("Count", LongType, true) :: Nil)

    val namesWithOwnSchema = spark.read
      .option("header","true")
      .schema(ownSchema)
      .csv("src/main/resources/Baby_Names__Beginning_2007.csv")

    println("Custom Schema Imposed StructType")
    namesWithOwnSchema.printSchema()
  }
}
