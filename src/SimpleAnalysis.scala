import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}

object SimpleAnalysis {

  /**
    * 需要3个参数
    *
    * @param args
    * args(0)是数据所在地址；
    * args(1)是电池组ID
    * args(2)是开始日期，如2015/05/05；
    * args(3)是结束日期，如2016/05/05
    */
  def main(args: Array[String]): Unit = {

    var input = "/home/alanp/Downloads/10001737.csv"
    var ID = "10001737"
    var start = "2015/5/5"
    var end = "2015/5/5"
    if (args.length >= 4) {
      input = args(0)
      ID = args(1)
      start = args(2)
      end = args(3)
    }

    val conf = new SparkConf();
    conf.setAppName("SimpleAnalysis")
    conf.setMaster("local")

    val sc = new SparkContext(conf);
    val lines = sc.textFile(input)
    lines.cache()
    //lines.take(10).foreach(println)

    val data = lines.map(x => (x.split(",")(0), x))
    data.take(10).foreach(println)

    val sdf = new SimpleDateFormat("yyyy/mm/dd")
    val startDate = sdf.parse(start)
    val endDate = sdf.parse(end)

    //数据完整度
    val validData = data.filter { case (key, value) => {
      val valueArr = value.split(",")
      val dt = sdf.parse(valueArr(22) + "/" + valueArr(23) + "/" + valueArr(24))
      dt.after(startDate) && dt.before(endDate) || dt == startDate || dt == endDate
    }
    }
    var days = (endDate.getTime - startDate.getTime) / (1000 * 3600 * 24) + 1
    println("间隔天数：" + days.toString)
    val expectNum = days * (2 * 60 * 24)
    println("期望数据量：" + expectNum)
    val actualNum = validData.count()
    println("实际数据量：" + actualNum)
    val percent = actualNum.toDouble / expectNum.toDouble * 100.0
    println("数据完整度：" + f"$percent%1.2f" + "%")

  }
}