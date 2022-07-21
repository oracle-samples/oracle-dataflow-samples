package com.oracle.etc

import java.text.SimpleDateFormat
import scala.io.Source


object TPCDSReader extends App{

  var startDate = ""
  var endDate = ""
  var query = ""
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

   val lines =
    Source.fromFile("/Users/mohankumarln/Desktop/Delta Spark/Spark 3.2.1 Delta SF1000-run3")
      .getLines()
      .filter(s => s.startsWith("query")).toList

//  println(lines)
  // Users/mohankumarln/Desktop/TPC-DS-Spark321/Spark 3.2.1 SP1000-run3

  lines.foreach(line =>
      {
        //println(line)
        val res = line.split("\\t")
        res.take(1).mkString match {
          case "queryend" => endDate = res.last
                            val difference_In_Time = sdf.parse(endDate).getTime() - sdf.parse(startDate).getTime()
                            val difference_In_Seconds = (difference_In_Time / 1000) % 60
                            val difference_In_Minutes = (difference_In_Time / (1000 * 60) % 60)
                            //println(query + " = " + (difference_In_Minutes * 60 + difference_In_Seconds))
                            println(difference_In_Minutes * 60 + difference_In_Seconds)
                             //println(Math.abs(((sdf.parse(endDate).getTime() - sdf.parse(startDate).getTime()) / 1000) % 60))
                            //println(TimeUnit.MILLISECONDS.toMinutes(endDate.getTime() - startDate.getTime()) % 60)
          case "querystart" =>  startDate = res.last
          case _  => {
              query = res.take(1).mkString
          }
        }
      }
  )

/*  val startDate1 = "2022-04-28 04:10:31.462317"
  val endDate1 = "2022-04-28 04:10:55.462317"
  //val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

 // val q1 = formatter.parse(startDate)
 // val q2  = formatter.parse(endDate)
  //val q1 = LocalDate.parse(startDate, formatter)
  //val q2 = LocalDate.parse(endDate, formatter)
  //val t1 = new Timestamp(q1.getTime)

  //val timestamp = Timestamp.valueOf(startDate)

   val sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val d1 = sdf1.parse(startDate1)
  val d2 = sdf1.parse(endDate1)
  val difference_In_Time: Long = d2.getTime() - d1.getTime();
  println(difference_In_Time)
  val difference_In_Seconds = (difference_In_Time / 1000) % 60
  val difference_In_Minutes = (difference_In_Time / (1000 * 60) % 60)
  println(difference_In_Minutes + ":" + difference_In_Seconds) */
}
