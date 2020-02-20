package dfbasics

import org.apache.spark.{SparkConf, SparkContext}

object MultipleSparkContexts {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local")
      .set("spark.driver.allowMultipleContexts", "true")
      .setAppName("Creating Multiple SC in spark 1.X")

    val sc = new SparkContext(sparkConf)
    val sc1 = new SparkContext(sparkConf)

    val rdd = sc.parallelize(Array(1,2,3,4,5,6,7,8,9))
    val rdd1 = sc1.parallelize(Array(10,11,12,13,14,15,16,17,18,19))

    rdd.collect()
    rdd1.collect()


  }
}
