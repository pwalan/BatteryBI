import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf();
    conf.setAppName("WordCount")
    conf.setMaster("local")

    val sc=new SparkContext(conf);
    val wordcount=sc.textFile("/home/alanp/Desktop/TestCase/data.txt")
      .flatMap(_.split(' '))
      .map((_,1))
      .reduceByKey(_+_)
      .collect()
      .foreach(println)
  }
}