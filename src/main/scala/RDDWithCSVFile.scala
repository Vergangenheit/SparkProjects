import org.apache.spark.sql.SparkSession

object RDDWithCSVFile {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().
      appName("Creating RDD from CSV file")
      .master("local")
      .getOrCreate()

    val csvRDD = sparkSession.sparkContext.textFile("src/main/resources/USD_INR.csv", 3)

    val header = csvRDD.first()
    val csvRDDWithoutHeader = csvRDD.filter(line => line != header)

    //csvRDDWithoutHeader.take(10).foreach(println)

    val usd_ind_limited = csvRDDWithoutHeader.map(line => {
      val colArray = line.split(",")
      Array(colArray(0), colArray(4), colArray(5)).mkString(":")
    })

    usd_ind_limited.take(10).foreach(println)

    usd_ind_limited.saveAsTextFile("output_destination/customer_limited_columns")


  }
}
