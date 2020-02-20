package dfbasics

import org.apache.spark.sql.SparkSession

object MultipleSparkSessions {
 def main(args: Array[String]): Unit = {

   val sparkSession1 = SparkSession.builder()
     .master("local")
     .appName("Creating multiple sessions in Spark 2.x")
     .getOrCreate()

   val sparkSession2 = SparkSession.builder()
     .master("local")
     .appName("Creating multiple sessions in Spark 2.x")
     .getOrCreate()

   val rdd1 = sparkSession1.sparkContext.parallelize(Array(1,2,3,4,5,6,7,8,9))
   val rdd2 = sparkSession2.sparkContext.parallelize(Array(10,11,12,13,14,15,16,17,18,19))

   rdd1.collect().foreach(println)
   rdd2.collect().foreach(println)


 }
}
