import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount{
  def main(args: Array[String]): Unit = {
    //准备Spark环境
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordcount")

    //建立Spark连接
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    //实现业务逻辑

    val fileRdd: RDD[String] = sc.textFile(path = "input")

    val wordRDD: RDD[String] = fileRdd.flatMap(line => {
      line.split(" ")
    })

    val mapRDD: RDD[(String, Int)] = wordRDD.map(word => (word, 1))

    val wordToSumRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    val resArray: Array[(String, Int)] = wordToSumRDD.collect()

    println(resArray.mkString("，"))

    //释放连接
    sc.stop()
  }
}