package RDDBasics
//
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

//
object CreatingSparkContext {
 def main(args: Array[String]): Unit = {
   
   val sparkConf = new SparkConf()
   sparkConf.setAppName("First Spark Application")
   sparkConf.setMaster("local")
   
   val sc = new SparkContext(sparkConf)
   
   val array = Array(1,2,3,4,5,6,7,8,9)
   
   val arrayRDD = sc.parallelize(array)
   
   println("Num of elements in RDD: ", arrayRDD.count())
   arrayRDD.foreach(println)

    
  }
}

  


 
