package com.github.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max

/** find the maximum precipitation for a given date-Doubt in code */
object maxPrecep {
  
  def parseLines(line:String) = {
    val fields = line.split(",")
    val dat = fields(1)
    val precp = fields(2)
    val precpVal = fields(3).toInt
    (dat,precp,precpVal)
  }
  
  def main(args : Array[String])
  {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","maxPreception")
    val lines =  sc.textFile("../1800.csv")
    val rdd = lines.map(parseLines).filter(x => x._2 =="PRCP")
    
    //println("(date,PRECP,value)")
    //rdd.take(5).foreach(println)
    val dayPrecpVal = rdd.map( x => (x._1,x._3))
    
    //println("(date,value)")
    //dayPrecpVal.take(5).foreach(println)
    
    val maxPrecpPerDay = dayPrecpVal.reduceByKey((x,y) => max(x,y))
    println("date,max Precp value")
    maxPrecpPerDay.collect().foreach(println)
    val daywithmaxprecp = maxPrecpPerDay.max()(new Ordering[Tuple2[String, Int]]() {
                                                override def compare(x: (String, Int), y: (String, Int)): Int = 
                                                Ordering[Int].compare(x._2, y._2)
                                                })
   println("date with max prception=="+daywithmaxprecp)
  }
}