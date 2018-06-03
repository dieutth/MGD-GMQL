package de.tub.dima.operators

import com.google.common.hash.Hashing
import de.tub.dima.MGD_GMQLSparkExecutor
import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.{DataTypes, GMQLSchemaFormat}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object RowArrMapTest {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Row-Arr Map Test")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryoserializer.buffer", "64")
      .set("spark.driver.allowMultipleContexts","true")
      .set("spark.sql.tungsten.enabled", "true")
      .set("spark.executor.heartbeatInterval","300s")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir","/tmp/spark-events")

    val spark =  SparkSession.builder().config(conf).getOrCreate()
    import  spark.implicits._
    val sc = spark.sparkContext

    val server = new GmqlServer(new MGD_GMQLSparkExecutor(sc=sc, outputFormat = GMQLSchemaFormat.TAB))

    def meta_parser(t: (Long, String)): Option[DataTypes.MetaType] = {
      val delimiter = "\t"
      try {
        val s = t._2 split delimiter
        Some((t._1, (s(0), s(1))))
      } catch {
        case ex: ArrayIndexOutOfBoundsException => None
      }
    }


    val startTime = System.currentTimeMillis()

    val bin = 10000
    val exp = spark.read.parquet("/home/dieutth/data/gmql/parquet/TAD_Aidens_id")
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
      .map(x => (x._1, x._2.map(
      r => r.getLong(0)
    ).toArray))
      //binning
      .flatMap{
      x =>
        val startbin = x._1._2/bin
        val stopbin = x._1._3/bin
        for (i <- startbin to stopbin)
        //produce tuple with same chr and bin number

          yield ((x._1._1, i), (x._1._2, x._1._3, x._1._4, x._2))
    }

    val ref: RDD[((Int,Long), (Long,Long,Short,Long))] = spark.read.parquet("/home/dieutth/data/gmql/parquet/TAD_Aidens_id")
      .rdd
      .flatMap{
      x =>
          val chrStr = x.getString(1).substring(3)
          val chr: Int = try {
            chrStr.toInt
          } catch {
            case e: Exception => 23
          }
          val start = x.getLong(2)
          val stop = x.getLong(3)
          val strand = x.getShort(4)
          val id = x.getLong(0)
          val startbin = start/bin
          val stopbin = stop/bin
          for (binNumber <- startbin to stopbin)
          //produce tuple with same chr and bin number
            yield ((chr, binNumber), (start, stop, strand, id))
    }


    val RefJoinedExp: RDD[((Int, Long, Long, Short),((Array[(Long, Int)])))] = ref.cogroup(exp)
      .flatMap{
        x =>
          val r: Iterable[(Long, Long, Short, Long)] = x._2._1
          val e: Iterable[(Long, Long, Short, Array[Long])] = x._2._2
          //          val key = x._1
          val res =
            r.flatMap{refRecord =>

              //key = start, stop, chr, strand
              val key = (x._1._1, refRecord._1, refRecord._2, refRecord._3)
              if (e.nonEmpty){
                e.map{
                  expRecord =>
//                    val xs = ListBuffer[Long]()
//                    val refId = refRecord._4
//                    for (expId <- expRecord._4)
//                      xs += Hashing.md5().newHasher().putLong(refId).putLong(expId).hash().asLong
//                    val ids = xs.toArray
                    val refId = refRecord._4
                    val ids = expRecord._4.map(Hashing.md5().newHasher().putLong(refId).putLong(_).hash().asLong)

                    if (
                    //region overlap
                      (refRecord._1 < expRecord._2 && expRecord._1 < refRecord._2)
                        //strand equal or at least one of the strand is non-determine
                        &&  (refRecord._3.equals('*') || expRecord._3.equals('*') || refRecord._3.equals(expRecord._3))
                        && (x._1._2 == refRecord._1/bin || x._1._2 == expRecord._1/bin)
                    ){

                      //                      (key,((refRecord._1, refRecord._2, refRecord._3, ids), 1))
                      val ids_counts = ids zip Array.fill(ids.length)(1)
                      (key, ids_counts)
                    }
                    else{
                      //                      (key,((refRecord._1, refRecord._2, refRecord._3, Array[Long]()), 0))
                      val ids_counts = ids zip Array.fill(ids.length)(0)
                      (key, ids_counts)

                    }
                }
              }
              else{
                //                Array((key,((refRecord._1, refRecord._2, refRecord._3, Array[Long]()), 0)))
                Array((key,( Array[(Long,Int)]())))
                //              Array()
              }
            }

          res
      }

    val reduced = RefJoinedExp
      .reduceByKey{
        (l, r) =>
          val res = l ++ r groupBy(_._1) mapValues (_.map(_._2).sum)
          res.toArray
      }
      .map{
        x=>
          (x._1.toString(), x._2.mkString("\t"))
      }
    reduced.saveAsTextFile("/home/dieutth/row_arr")
    println ("Execution time when running without custom-partitioner:" + (System.currentTimeMillis() - startTime )/1000)


  }

}
