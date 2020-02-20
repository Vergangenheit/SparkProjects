package dfbasics

import org.apache.spark.sql
import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DFWithSS {
  def main(args: Array[String]) : Unit = {
    val sparkSession = SparkSession.builder()
        .appName("Creating a DF using Spark Session")
        .master("local")
        .getOrCreate()

    val RDD = sparkSession.sparkContext.parallelize(Array('1','2','3','4','5'))

    val schema = StructType(
      StructField("Integers as String", StringType, true) :: Nil
    )

    //convert into a row type rdd
    val rowRDD = RDD.map(line => Row(line))

    val df = sparkSession.createDataFrame(rowRDD, schema)

    df.printSchema()
    df.show()

  }
}
