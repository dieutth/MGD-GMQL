package de.tub.dima.parquet

import com.google.common.hash.Hashing
import de.tub.dima.MGD_GMQLSparkExecutor
import de.tub.dima.dimension.Coordinate
import de.tub.dima.loaders.Loaders
import de.tub.dima.loaders.writeMultiOutputFiles.RDDMultipleTextOutputFormat
import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object TestParquet {
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


    //original
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

    val ref = spark.read.parquet("/home/dieutth/data/gmql/parquet/TAD_Aidens_id")
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
      .map(x => (x._1, x._2.map(r => r.getLong(0)).toArray))
      //binning
      .flatMap{
      x =>
        val startbin = x._1._2/bin
        val stopbin = x._1._3/bin
        for (i <- startbin to stopbin)
        //produce tuple with same chr and bin number
          yield ((x._1._1, i), (x._1._2, x._1._3, x._1._4, x._2))
    }


    //    val RefJoinedExp: RDD[((Int,Long),(Long,Long,Char,Array[Long]))] = ref.cogroup(exp)
//                            .flatMapValues{
//                              x =>
//                                val r: Iterable[(Long, Long, Char, Array[Long])] = x._1
//                                val e: Iterable[(Long, Long, Char, Array[Long])] = x._2
//
//                                val res : Iterable[(Long, Long, Char, Array[Long])] =
//                                  r.flatMap{refRecord =>
//                                    if (e.nonEmpty){
//                                      e.map{
//                                        expRecord =>
//                                        if (
//                                          (refRecord._1 < expRecord._2 && expRecord._1 < refRecord._2)
//                                          &&  (refRecord._3.equals('*') || expRecord._3.equals('*') || refRecord._3.equals(expRecord._3))
//                                        ){
//                                          val ids: Array[Long] = {
//                                            val xs = List[Long]()
//                                            refRecord._4.flatMap {
//                                              refId => {
//                                                for (expId <- expRecord._4)
//                                                  Hashing.md5().newHasher().putLong(refId).putLong(expId).hash().asLong :: xs
//                                                expRecord._4.length :: xs
//                                                xs
//                                              }
//                                            }
//                                          }
//                                       (refRecord._1, refRecord._2, refRecord._3, ids)
//                                        }
//                                        else{
//                                         (refRecord._1, refRecord._2, refRecord._3, Array[Long](0))
//                                        }
//                                      }
//                                    }
//                                    else{
//                                     Array((refRecord._1, refRecord._2, refRecord._3, Array[Long](0)))
//                                    }
//                                }
//                            res
//                         }

//    val RefJoinedExp: RDD[((Int,Long),((Long,Long,Char,Array[Long]), Int))] = ref.cogroup(exp)
//    val RefJoinedExp: RDD[((Long, Long, Int, Char),((Long,Long,Char,Array[Long]), Int))] = ref.cogroup(exp)

//    def func(r: Iterable[(Long, Long, Char, Array[Long])], e: Iterable[(Long, Long, Char, Array[Long])], chr: Int): RDD[((Long, Long, Int, Char),((Array[Long]), Int))]={
//      val res =
//        r.flatMap{refRecord =>
//
//          //key = start, stop, chr, strand
//          val key = (refRecord._1, refRecord._2, chr, refRecord._3)
//          if (e.nonEmpty){
//            e.map{
//              expRecord =>
//                if (
//                  (refRecord._1 < expRecord._2 && expRecord._1 < refRecord._2)
//                    &&  (refRecord._3.equals('*') || expRecord._3.equals('*') || refRecord._3.equals(expRecord._3))
//                ){
//                  val ids: Array[Long] = {
//                    val xs = List[Long]()
//                    refRecord._4.flatMap {
//                      refId => {
//                        for (expId <- expRecord._4)
//                          Hashing.md5().newHasher().putLong(refId).putLong(expId).hash().asLong :: xs
//                        xs
//                      }
//                    }
//                  }
//                  //                      (key,((refRecord._1, refRecord._2, refRecord._3, ids), 1))
//                  (key,(ids, 1))
//                }
//                else{
//                  //                      (key,((refRecord._1, refRecord._2, refRecord._3, Array[Long]()), 0))
//                  (key,(Array[Long](), 0))
//
//                }
//            }
//          }
//          else{
//            //                Array((key,((refRecord._1, refRecord._2, refRecord._3, Array[Long]()), 0)))
//            Array((key,( Array[Long](), 0)))
//          }
//        }
//      res
//    }


//    val temp = ref.cogroup(exp)
    val RefJoinedExp: RDD[((Int, Long, Long, Short),((Array[(Long, Int)])))] = ref.cogroup(exp)
      .flatMap{
        x =>
          val r: Iterable[(Long, Long, Short, Array[Long])] = x._2._1
          val e: Iterable[(Long, Long, Short, Array[Long])] = x._2._2
        //          val key = x._1
        val res =
          r.flatMap{refRecord =>

            //key = start, stop, chr, strand
            val key = (x._1._1, refRecord._1, refRecord._2, refRecord._3)
            if (e.nonEmpty){
              e.map{
                expRecord =>
//                  val ids: Array[Long] = {
//                    val xs = List[Long]()
//                    refRecord._4.flatMap {
//
//                      refId => {
//                        for (expId <- expRecord._4)
//                          Hashing.md5().newHasher().putLong(refId).putLong(expId).hash().asLong :: xs
//                      }
//                        xs
//                    }
//
//                  }
                  val xs = ListBuffer[Long]()
                  for (refId <- refRecord._4)
                    for (expId <- expRecord._4)
                      xs += Hashing.md5().newHasher().putLong(refId).putLong(expId).hash().asLong
                  val ids = xs.toArray

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
//      .
                      .map{
                        x=>
                          x._2.map(_._2).sum
                      }
      .sum()
    println(reduced)
//    reduced.saveAsTextFile("/home/dieutth/testparquet3")

//    reduced.saveAsHadoopFile("/home/dieutth/testparquet/new",classOf[String],classOf[String],classOf[RDDMultipleTextOutputFormat])

    println ("Execution time when running without custom-partitioner:" + (System.currentTimeMillis() - startTime )/1000)


  }
}


