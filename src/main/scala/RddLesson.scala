import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RddLesson {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rddlesson")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 3)
    rdd.saveAsTextFile("ouput")
    println(rdd.collect().mkString(","))


    val fileRdd: RDD[String] = sc.textFile("input")
  }
}
