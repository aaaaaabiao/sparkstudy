import org.apache.spark.{SparkConf, SparkContext}

object WordCount{
  def main(args: Array[String]): Unit = {
    //准备Spark环境
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    //建立Spark连接

    //实现业务逻辑
    val sc = new SparkContext(sparkConf);
    //释放连接
    sc.stop();
  }
}