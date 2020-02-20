package dfbasics

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/*
in spark 1.x you create DF using SQLContext, which it's created using SparkContext
 */

object DFWithSC {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("First DF with SC")
    sparkConf.setMaster("local")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val RDD = sc.parallelize(Array(1,2,3,4,5))

    val schema = StructType(
          StructField("Numbers", IntegerType, false) :: Nil
    )

    val rowRDD = RDD.map(line => Row(line))

    val df = sqlContext.createDataFrame(rowRDD, schema)

    df.printSchema()

    df.show()
  }
}
