package de.tub.dima.parquet

import de.tub.dima.MGD_GMQLSparkExecutor
import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.DataStructures.JoinParametersRD.{DistLess, RegionBuilder}
import it.polimi.genomics.core.GMQLSchemaFormat
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ArrArrLeftJoinTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test Parquet")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryoserializer.buffer", "64")
      .set("spark.driver.allowMultipleContexts","true")
      .set("spark.sql.tungsten.enabled", "true")
      .set("spark.executor.heartbeatInterval","300s")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir","/tmp/spark-events")

    val spark =  SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    val startTime = System.currentTimeMillis()
    val bin = 10000
    val gDist = DistLess(70000)

    val (ds1Path, ds2Path, loop) = if (args.length == 3) (args(0), args(1), args(2))
    else ("/home/dieutth/data/gmql/parquet/TAD_Aidens_id", "/home/dieutth/data/gmql/parquet/TAD_Aidens_id", "3")

//    val exp: RDD[((Int, Long),(Long, Long, Short, Array[Long], Array[Array[Double]]))]
    val exp = spark.read.parquet(ds1Path)
      .rdd
      //group by coordinate
      .groupBy { x =>
      val chrStr = x.getString(1).substring(3)
      val chr: Int = try {
        chrStr.toInt
      } catch {
        case e: Exception => 23
      }
      (chr, x.getLong(2), x.getLong(3), x.getShort(4))
    }
      //convert each group of item with the same coordinate to an array of coordinate, with list of ids
      .map(x => (x._1, (x._2.map(r => r.getLong(0)).toArray, x._2.map(_ => Array[Double]()).toArray)))



//    val ref: RDD[((Int, Long),(Long, Long, Short, Array[Long], Array[Array[Double]]))]
    val ref = spark.read.parquet(ds2Path)
      .rdd
      //group by coordinate
      .groupBy { x =>
      val chrStr = x.getString(1).substring(3)
      val chr: Int = try {
        chrStr.toInt
      } catch {
        case e: Exception => 23
      }
      (chr, x.getLong(2), x.getLong(3), x.getShort(4))
    }
      //convert each group of item with the same coordinate to an array of coordinate, with list of ids
      .map(x => (x._1, (x._2.map(r => r.getLong(0)).toArray, x._2.map(_ => Array[Double]()).toArray)))

    var reduced = ArrArrJoin(ref, exp, bin, RegionBuilder.LEFT, Some(gDist), None)
    for (i <- Range(1,loop.toInt))
      reduced = ArrArrJoin(ref, reduced, bin, RegionBuilder.LEFT, Some(gDist), None)
//    val tmp1 = ArrArrJoin(ref, exp, bin, RegionBuilder.LEFT, Some(gDist), None)
//    val tmp =  ArrArrJoin(exp, tmp1, bin, RegionBuilder.LEFT, Some(gDist), None)
//    val reduced =
//     val count = reduced
//                  .map{
//                    x =>
//                      x._2._1.size
////                      val k = x._1
////                      for (item <- (x._2._1 zip x._2._2))
////                        yield (k, item._1, item._2.mkString("[", ",", "]"))
//                  }
//    reduced.saveAsTextFile("/home/dieutth/testparquet/join/")
    println(reduced.count())
    println ("Execution time when running without custom-partitioner:" + (System.currentTimeMillis() - startTime )/1000)

  }

}


