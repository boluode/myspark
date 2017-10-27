package com.hualala.myspark

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Hello world!
 *
 */
object App extends App {

  val conf = new SparkConf().setAppName("test").setMaster("local")

  var sc = new SparkContext(conf)

  var data = sc.textFile("E:/mydata/*.data")

  println("data.first()" + data.first())
  println("data.count():" + data.count())

  val simplifiedBillData = data.map(order => {

    val orderJson = JSON.parseObject(order.replace("\\\\n","").replace("\\n","").replace("\\r","").replace("\\t"," "))
    (orderJson)
  })

  /*simplifiedBillData.foreach(bill => {
    println(bill)
  })*/

  /*def hehe = (s1 : String) => {

    (s1.length<4129)
  }

  println()
  val large = data.filter(hehe)

  large.foreach(bill => {
    println(bill)
  })*/

  /*var kkk = data.filter(bill => {
    val orderJson = JSON.parseObject(bill.replace("\\\\n","").replace("\\n","").replace("\\r","").replace("\\t"," "))
    var master = JSON.parseObject(orderJson.get("master").toString)
    (master.get("checkoutTime").eq("20171008120106"))
  })

  kkk.foreach(k => {
    println(k)
  })*/
}
