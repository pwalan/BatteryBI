import java.text.SimpleDateFormat

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object StatisticStreaming {

  private val COUNT = 5 //判断充放需要的电正负电流次数
  private val TIMELIMITE = 30 //时间间隔阈值，放电过程中前后两条数据的时间差大于阈值要切分数据

  case class MyStatus() {
    var ischarge = false //是否是充电状态
    var isFirst = true //是否是第一次状态转换
    var isFirstData = true //是否是总的第一条数据
    var isP = true //之前是否是正电流
    var thisStartTime = "" //放电开始时间
    var thisStartSOC = "" //放电开始SOC
    var negCurrentCnt = 0 //负电流次数
    var posCurrentCnt = 0 //正电流次数
    var negCurrentStartTime = "" //充电开始时间
    var negCurrentStartSOC = "" //放电开始时间
    var lastTime = "" //上一条数据的时间
    var lastSOC = "" //上一条数据的SOC
    var maxTemperature = 0 //最高温度
    var minTemperature = 0 //最低温度
    var result = ArrayBuffer[String]() //充放电统计结果

    override def toString = {
      val size = result.size
      s"ischarge:$ischarge, isFirst:$isFirst, thisStartTime:$thisStartTime, " +
        s"thisStartSOC:$thisStartSOC, negCurrentCnt:$negCurrentCnt, negCurrentStartTime:$negCurrentStartTime, negCurrentStartSOC:$negCurrentStartSOC, " +
        s"resultSize:$size"
    }

  }

  def updateStatesFunc(curValues: Seq[ArrayBuffer[String]], state: Option[MyStatus]): Option[MyStatus] = {
    val prev = state.getOrElse(MyStatus())
    val newStatus = new MyStatus()
    println("本批数据大小：" + curValues.length)
    var countP = prev.posCurrentCnt
    var countN = prev.negCurrentCnt
    var ischarge = prev.ischarge
    var isP = prev.isP
    var isfirst = prev.isFirst
    var isFirstData = prev.isFirstData
    var lastTime = prev.lastTime
    var lastSOC = prev.lastSOC
    var pvalue = ArrayBuffer[String]() //正电流首次出现时的数据
    var nvalue = ArrayBuffer[String]() //负电流首次出现时的数据
    var maxTemperature = prev.maxTemperature
    var minTemperature = prev.minTemperature
    val df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")

    curValues.foreach(x => {
      //判断温度
      if (x(4).toInt > maxTemperature) {
        maxTemperature = x(4).toInt
      }
      if (x(5).toInt < minTemperature) {
        minTemperature = x(5).toInt
      }

      //判断电流
      if (x(2).toString.contains("-")) { //电流为负
        if (countN < COUNT) { //如果负电流次数小于COUNT，表明此时不一定在充电，需要接着判断
          countN += 1
          countP -= 1
          if (isP) { //如果之前是正电流，出现一个负电流可能就是充电开始或是刹车开始
            nvalue = x
            isP = false
          }
          if (isFirstData) {
            lastTime = x(1)
            lastSOC = x(3)
            nvalue = x
            isFirstData = false
            maxTemperature = x(4).toInt
            minTemperature = x(5).toInt
          }
        } else { //如果负电流次数大于COUNT，而且之前是放电状态，需要转为充电状态
          if (isfirst) { //对第一次状态的标记
            prev.negCurrentStartTime = nvalue(1)
            prev.negCurrentStartSOC = nvalue(3)
            isfirst = false
            ischarge = true
          }
          if (!isfirst && !ischarge) {
            prev.negCurrentStartTime = nvalue(1)
            prev.negCurrentStartSOC = nvalue(3)
            prev.result += (x(0) + " " + prev.thisStartTime + " " + prev.negCurrentStartTime + " " + prev.thisStartSOC + " " + prev.negCurrentStartSOC + " discharge"+" "+maxTemperature+" "+minTemperature)
            maxTemperature = nvalue(4).toInt
            minTemperature = nvalue(5).toInt
          }
          ischarge = true
        }
      } else { //电流为正
        if (countP < COUNT) { //如果正电流次数小于COUNT，表明此时不一定在放电，需要接着判断
          countP += 1
          countN -= 1
          if (!isP) {
            pvalue = x
            isP = true
          }
          if (isFirstData) {
            lastTime = x(1)
            lastSOC = x(3)
            pvalue = x
            isFirstData = false
            maxTemperature = x(4).toInt
            minTemperature = x(5).toInt
          }
        } else { //如果正电流次数大于COUNT，而且之前是充电状态，需要转为放电状态
          if (isfirst) { //对第一次状态的标记
            prev.thisStartTime = pvalue(1)
            prev.thisStartSOC = pvalue(3)
            ischarge = false
            isfirst = false
          }

          if (!isfirst && ischarge) {
            prev.thisStartTime = pvalue(1)
            prev.thisStartSOC = pvalue(3)
            prev.result += (x(0) + " " + prev.negCurrentStartTime + " " + prev.thisStartTime + " " + prev.negCurrentStartSOC + " " + prev.thisStartSOC + " charge"+" "+maxTemperature+" "+minTemperature)
            maxTemperature = pvalue(4).toInt
            minTemperature = pvalue(5).toInt
          }
          ischarge = false
        }

        //放电过程中如果前后两条数据的时间差大于TIMELIMIT，表明中途停车，需要将放电数据进行进一步切分
        if (!ischarge && (df.parse(x(1)).getTime - df.parse(lastTime).getTime) / (1000 * 60) >= 30) {
          prev.result += (x(0) + " " + prev.thisStartTime + " " + lastTime + " " + prev.thisStartSOC + " " + x(3) + " discharge"+" "+maxTemperature+" "+minTemperature)
          prev.thisStartTime = x(1)
          prev.thisStartSOC = x(3)
          maxTemperature = x(4).toInt
          minTemperature = x(5).toInt
        }

        lastTime = x(1)
        lastSOC = x(3)
      }
    })

    newStatus.isFirst = isfirst
    newStatus.ischarge = ischarge
    newStatus.isP = isP
    newStatus.result = prev.result
    newStatus.posCurrentCnt = countP
    newStatus.negCurrentCnt = countN
    newStatus.thisStartSOC = prev.thisStartSOC
    newStatus.thisStartTime = prev.thisStartTime
    newStatus.negCurrentStartSOC = prev.negCurrentStartSOC
    newStatus.negCurrentStartTime = prev.negCurrentStartTime
    newStatus.lastTime = lastTime
    newStatus.lastSOC = lastSOC
    newStatus.maxTemperature=maxTemperature
    newStatus.minTemperature=minTemperature

    //如果无数据，则将结果清空
    if (curValues.size == 0) {
      newStatus.result = ArrayBuffer[String]()
    } else {
      newStatus.lastTime = curValues(curValues.size - 1)(1)
      newStatus.lastSOC = curValues(curValues.size - 1)(3)
    }

    Some(newStatus)
  }

  def main(args: Array[String]): Unit = {
    var inputpath = "input"
    var checkpoint = "checkpoint"

    println("参数: " + args.length)
    args.foreach(println)

    if (args.length >= 2) {
      inputpath = args(0)
      checkpoint = args(1)

    }

    val conf = new SparkConf();
    conf.setAppName("StatisticStreaming")
    conf.setMaster("local")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint(checkpoint)

    val lines = ssc.textFileStream(inputpath)

    val dataDStream = lines.map(x => {
      //数据是以逗号分隔的csv文件，内容参照BatteryInfo类
      val v = x.split(",")
      val value = ArrayBuffer[(String)]()
      if (v.length >= 25) {
        var MM = ""
        var dd = ""
        if (v(23).toInt < 10) {
          MM = "0" + v(23)
        } else {
          MM = v(23)
        }
        if (v(24).toInt < 10) {
          dd = "0" + v(24)
        } else {
          dd = v(24)
        }
        val date = v(22) + "/" + MM + "/" + dd + " " + v(20).split(" ")(1)
        value += v(0) //ID
        value += date //日期
        value += v(2) //总电流
        value += v(3) //SOC
        value += v(12) //最高温度
        value += v(15) //最低温度
      }
      (v(0), value)
    })

    val resultDStream = dataDStream.updateStateByKey(updateStatesFunc)

    //将结果存入HBase
    resultDStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.foreachPartition(partition => {
          val conf = HBaseConfiguration.create()
          conf.set("hbase.zookeeper.quorum", "inspire-dev-5")
          conf.set("hbase.zookeeper.property.clientPort", "2181")
          val connection = ConnectionFactory.createConnection(conf)
          val tableName = TableName.valueOf("statistic")
          val table = connection.getTable(tableName)
          val df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
          partition.foreach(p => {
            p._2.result.foreach(x => {
              println(x)
              //数据样式：10001737 2015/05/06 7:25:13 2015/05/06 7:41:45 69 67 discharge 35 27
              val datas = x.split(" ")
              try {
                val startTime = datas(1) + " " + datas(2)
                val endTime = datas(3) + " " + datas(4)
                val duration = ((df.parse(endTime).getTime - df.parse(startTime).getTime) / (1000 * 60)).toString
                val put = new Put(Bytes.toBytes(String.valueOf(datas(0) + " " + startTime)))
                val startSOC = datas(5)
                val endSOC = datas(6)
                val difSOC = (endSOC.toDouble - startSOC.toDouble).toString
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("ID"), Bytes.toBytes(datas(0)))
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("TYPE"), Bytes.toBytes(datas(7)))
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("STARTTIME"), Bytes.toBytes(startTime))
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("ENDTIME"), Bytes.toBytes(endTime))
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("DURATION"), Bytes.toBytes(duration))
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("STARTSOC"), Bytes.toBytes(startSOC))
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("ENDSOC"), Bytes.toBytes(endSOC))
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("DIFFERENCESOC"), Bytes.toBytes(difSOC))
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("MAXTEMPERATURE"), Bytes.toBytes(datas(8)))
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("MINTEMPERATURE"), Bytes.toBytes(datas(9)))
                table.put(put)
              } catch {
                case e: Exception => e.printStackTrace()
              } finally {
                table.close()
              }
            })
          })
        })
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
