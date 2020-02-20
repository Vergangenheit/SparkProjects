import org.apache.spark.sql.SparkSession

object CreatingSparkContextwithSparkSession {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("Creating SparkCOntext with SparkSession")
      .master("local")
      .getOrCreate()

    val array = Array(1,2,3,4,5)

    val arrayRDD = sparkSession.sparkContext.parallelize(array, 1)

    arrayRDD.foreach(println)

    val file = "src/main/resources/reviews.tsv"
    val fileRDD = sparkSession.sparkContext.textFile(file)

    println("Num of Records: " + fileRDD.count())
    fileRDD.take(10).foreach(println)
  }
}
