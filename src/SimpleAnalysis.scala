import java.text.SimpleDateFormat
import java.util.Date


import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object SimpleAnalysis {

  /**
    * 需要1个参数
    *
    * @param args
    * args(1)是数据所在地址
    */
  def main(args: Array[String]): Unit = {

    println("参数: " + args.length)
    args.foreach(println)

    //var input="/home/alanp/Downloads/10001737.csv"
    var input = "hdfs://inspire-dev-3:8020/user/ablecloud/10001737.csv"
    if (args.length >= 1) {
      input = args(0)
    }


    val conf = new SparkConf();
    conf.setAppName("SimpleAnalysis")
    conf.setMaster("local[4]")

    val sc = new SparkContext(conf);
    val lines = sc.textFile(input)
    //lines.take(10).foreach(println)

    val datas = lines.map(x => (x.split(",")(0), x))
    datas.cache()
    datas.take(10).foreach(println)

    var start = "2015/5/5"
    var end = "2015/5/7"
    val sdf = new SimpleDateFormat("yyyy/MM/dd")
    val startDate:Date = sdf.parse(start)
    val endDate:Date = sdf.parse(end)

    //数据完整度
    val validDatas = datas.filter { case (key, value) => {
      val valueArr = value.split(",")
      val dt = sdf.parse(valueArr(22) + "/" + valueArr(23) + "/" + valueArr(24))
      dt.after(startDate) && dt.before(endDate) || dt == startDate || dt == endDate
    }
    }
    //validDatas.cache()
    var days = (endDate.getTime - startDate.getTime) / (1000 * 3600 * 24) + 1
    println("间隔天数：" + days.toString)
    val expectNum = days * (2 * 60 * 24)
    println("期望数据量：" + expectNum)
    val actualNum = validDatas.countByKey()
    println("实际数据量：" + actualNum)
    val percents = actualNum.mapValues(_.toDouble / expectNum.toDouble * 100.0)
    print("数据完整度: ")
    percents.foreach(x => {
      val percent = x._2
      println(x._1 + " : " + f"$percent%1.2f" + "%")
    })

    //充放电统计
    val data = validDatas.reduceByKey(_ + ";" + _)
    //val data = datas.reduceByKey(_ + ";" + _)

    val result = data.mapValues(x => {
      val values = x.split(";")
      val chargeStartArr = ArrayBuffer[(String)]()
      val dischargeStartArr = ArrayBuffer[String]()
      val chargeEndArr = ArrayBuffer[(String)]()
      val dischargeEndArr = ArrayBuffer[String]()
      var countP = 0 //正电流出现的次数
      var countN = 0 //负电流出现的次数
      var pvalue = values(0) //正电流首次出现时的数据
      var nvalue = values(0) //负电流首次出现时的数据
      var ischarge = false
      var isP = true //之前是正电流吗
      var isfirst = true
      values.foreach(v => {
        val tmp = v.split(",")
        if (tmp(2).toString.contains("-")) { //电流为负
          if (countN < 4) {
            countN += 1
            countP -= 1
            if (isP) {
              nvalue = v
              isP = false
            }
          } else {
            if (isfirst) {
              chargeStartArr += nvalue
              isfirst = false
              ischarge = true
            }

            if (!isfirst && !ischarge) {
              chargeStartArr += nvalue
              dischargeEndArr += nvalue
            }
            ischarge = true

          }
        } else { //电流为正
          if (countP < 4) {
            countP += 1
            countN -= 1
            if (!isP) {
              pvalue = v
              isP = true
            }
          } else {
            if (isfirst) {
              dischargeStartArr += pvalue
              ischarge = false
              isfirst = false
            }

            if (!isfirst && ischarge) {
              dischargeStartArr += pvalue
              chargeEndArr += pvalue
            }
            ischarge = false
          }
        }
      })
      //判断最后一条数据时电池的状态
      if (ischarge) {
        chargeEndArr += values(values.length - 1)
      } else {
        dischargeEndArr += values(values.length - 1)
      }
      (chargeStartArr, chargeEndArr, dischargeStartArr, dischargeEndArr)
    })


    result.foreach(x => {
      println("ID:" + x._1)
      println("充电开始数组: ")
      x._2._1.foreach(println)
      println("充电结束数组: ")
      x._2._2.foreach(println)
      println("放电开始数组: ")
      x._2._3.foreach(println)
      println("放电结束数组: ")
      x._2._4.foreach(println)


      putToHbase(x._2._1, x._2._2,"charge")
      putToHbase(x._2._3, x._2._4,"discharge")

    })
  }

  def putToHbase(startArr: ArrayBuffer[(String)], endArr: ArrayBuffer[(String)],chargeType:String): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "inspire-dev-5")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    val tableName = "statistic"
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    val hBaseAdmin = new HBaseAdmin(conf);
    val table = new HTable(conf, tableName)
    val df = new SimpleDateFormat("yyyy/mm/dd HH:mm:ss")
    for (i <- 0 to startArr.length - 1) {
      val start = startArr(i).split(",")
      val end = endArr(i).split(",")
      val startDate = start(22) + "/" + start(23) + "/" + start(24) + " " + start(20).split(" ")(1)
      val endDate = end(22) + "/" + end(23) + "/" + end(24) + " " + end(20).split(" ")(1)
      val put = new Put((start(0) + " " + startDate).getBytes())
      put.add("data".getBytes(), "ID".getBytes(), Bytes.toBytes(start(0)))
      put.add("data".getBytes(), "TYPE".getBytes(), Bytes.toBytes(chargeType))
      put.add("data".getBytes(), "STARTTIME".getBytes(), Bytes.toBytes(startDate))
      put.add("data".getBytes(), "ENDTIME".getBytes(), Bytes.toBytes(endDate))
      put.add("data".getBytes(), "DURATION".getBytes(), Bytes.toBytes(((df.parse(startDate).getTime - df.parse(endDate).getTime) / (1000 * 60)).toString))
      put.add("data".getBytes(), "STARTSOC".getBytes(), Bytes.toBytes(start(3)))
      put.add("data".getBytes(), "ENDSOC".getBytes(), Bytes.toBytes(end(3)))
      put.add("data".getBytes(), "DIFFERENCESOC".getBytes(), Bytes.toBytes((start(3).toInt - end(3).toInt).toString))
      table.put(put)
    }

  }


}