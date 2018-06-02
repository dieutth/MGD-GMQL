package de.tub.dima.parquet

import com.google.common.hash.Hashing
import de.tub.dima.loaders.{BedParser, CustomParser, Loaders}
import de.tub.dima.parquet.legacy.MapArrArrNC_leftOuterJoin
import it.polimi.genomics.core.DataStructures.JoinParametersRD.{DistLess, RegionBuilder}
import it.polimi.genomics.core.GDouble
import org.apache.spark.sql.functions.{input_file_name, lit}
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
      .set("spark.default.parallelism", "2")

    val spark =  SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    val bin = 10000
    val gDist = DistLess(70000)

    val (ds1Path, ds2Path, loop) = if (args.length == 3) (args(0), args(1), args(2))
//    else ("/home/dieutth/data/gmql/parquet/TAD_Aidens_id", "/home/dieutth/data/gmql/parquet/TAD_Aidens_id", "3")
//    else ("/home/dieutth/data/gmql/uncompressed/TADs_Aiden/", "/home/dieutth/data/gmql/uncompressed/TADs_Aiden/", "3")
    else ("/home/dieutth/data/gmql/uncompressed/TADs_Aiden_working_medium/", "/home/dieutth/data/gmql/uncompressed/TADs_Aiden_working_medium/", "3")
//    else ("/home/dieutth/data/gmql/uncompressed/tmp/ref/", "/home/dieutth/data/gmql/uncompressed/tmp/exp/", "3")


    val startTime = System.currentTimeMillis()
    val ref = Loaders.forPath(sc, ds1Path).LoadRegionsCombineFiles(new CustomParser().setSchema(ds1Path).region_parser)
      .groupBy{
        x =>
          val y = x._1.chrom.substring(3)
          val chr: Int = try {
            y.toInt
          } catch {
            case e: Exception => if (y == "X") 23 else 24
          }
          (chr, x._1.start, x._1.stop, x._1.strand.toShort)
      }
      .map(x=>
        (x._1, x._2.map(y => (y._1.id, y._2.flatMap(_ match {case GDouble(t)=> Some(t)
        case _=> None}))).toArray.unzip)

      )

    val exp = Loaders.forPath(sc, ds2Path).LoadRegionsCombineFiles(new CustomParser().setSchema(ds2Path).region_parser)
      .groupBy{
        x =>
          val y = x._1.chrom.substring(3)
          val chr: Int = try {
            y.toInt
          } catch {
            case e: Exception => if (y == "X") 23 else 24
          }
          (chr, x._1.start, x._1.stop, x._1.strand.toShort)
      }
      .map(x=>
        (x._1, x._2.map(y => (y._1.id, y._2.flatMap(_ match {case GDouble(t)=> Some(t)
        case _=> None}))).toArray.unzip)

      )

    testArrMap_3(2)
//    testArrMap(2)
//    testArrMap_2(2)
//    testArrJoin(2)
//    testArrJoinNoCartesian(2)
    def testArrMap(loop: Int) = {
      var reduced = MapArrArrSingleMatrixNoCartesian(sc, ref, exp, bin)
      for (i <- Range(1, loop))
        reduced = MapArrArrSingleMatrixNoCartesian(sc, ref, reduced, bin)
      val r = reduced
        .flatMap {
          x =>
            val k = ("chr"+x._1._1, x._1._2, x._1._3, x._1._4.toChar)
            for (item <- (x._2._1 zip x._2._2))
              yield (k, item._1, item._2.mkString(","))
        }
        .saveAsTextFile("/home/dieutth/testparquet/mapNoCartesian/")
      println("Execution time for Map:" + (System.currentTimeMillis() - startTime) / 1000)

    }

    def testArrMap_2(loop: Int) = {
      var reduced = MapArrArrNC_leftOuterJoin(sc, ref, exp, bin)
      for (i <- Range(1, loop))
        reduced = MapArrArrNC_leftOuterJoin(sc, ref, reduced, bin)
      val r = reduced
        .flatMap {
          x =>
            val k = ("chr"+x._1._1, x._1._2, x._1._3, x._1._4.toChar)
            for (item <- (x._2._1 zip x._2._2))
              yield (k, item._1, item._2.mkString(","))
        }
        .saveAsTextFile("/home/dieutth/testparquet/mapNoCartesian_2/")
      println("Map Arr-Arr NoCartesian leftOuterJoin:" + (System.currentTimeMillis() - startTime) / 1000)

    }

    def testArrMap_3(loop: Int) = {
      var reduced = Map_AA_NC_binInt(sc, ref, exp, bin)
      for (i <- Range(1, loop))
        reduced = Map_AA_NC_binInt(sc, ref, reduced, bin)
      val r = reduced
        .flatMap {
          x =>
            val k = ("chr"+x._1._1, x._1._2, x._1._3, x._1._4.toChar)
            for (item <- (x._2._1 zip x._2._2))
              yield (k, item._1, item._2.mkString(","))
        }
        .saveAsTextFile("/home/dieutth/testparquet/mapNoCartesian_2/")
      println("Map Arr-Arr NoCartesian binNumber type Int:" + (System.currentTimeMillis() - startTime) / 1000)

    }

    def testArrJoin(loop: Int) = {
      var reduced = ArrArrJoin(ref, exp, bin, RegionBuilder.LEFT, Some(gDist), None)
      for (i <- Range(1, loop))
//        reduced = ArrArrJoin(ref, reduced, bin, RegionBuilder.LEFT, Some(gDist), None)
        reduced = ArrArrJoin(reduced, ref, bin, RegionBuilder.LEFT, Some(gDist), None)
      reduced
//        .flatMap {
//          x =>
//            val k = x._1
//            for (item <- (x._2._1 zip x._2._2))
//              yield (k, item._1, item._2.mkString(","))
//        }
        .saveAsTextFile("/home/dieutth/testparquet/joinNormal/")
      println("Execution time for normal join :" + (System.currentTimeMillis() - startTime) / 1000)
    }

    def testArrJoinNoCartesian(loop: Int) = {
          var reduced = ArrArrJoin_NoCartesian(ref, exp, bin, RegionBuilder.LEFT, Some(gDist), None)
      for (i <- Range(1, loop))
//            reduced = ArrArrJoin_NoCartesian(ref, reduced, bin, RegionBuilder.LEFT, Some(gDist), None)
            reduced = ArrArrJoin_NoCartesian(reduced, ref, bin, RegionBuilder.LEFT, Some(gDist), None)
      reduced
        .saveAsTextFile("/home/dieutth/testparquet/joinNoCartesian/")
      println("Execution time when running on Join No cartesian:" + (System.currentTimeMillis() - startTime) / 1000)
    }
  }

//  private def readDS(spark: SparkSession, path: String): Map[String, Long] ={
//    val allFiles = spark.read.text(path)
//                  .select(input_file_name)
//                    .flatMap{
//                      x =>
//                        val filename = x.getString(0)
//                        if (filename.endsWith("schema") || filename.endsWith("meta"))
//                          None
//                        else{
//                          val fn = filename.substring(0, filename.lastIndexOf("."))
//                          val hashKey = Hashing.md5().newHasher().putString(fn.replaceAll("/",""),java.nio.charset.StandardCharsets.UTF_8).hash().asLong()
//                         Some(filename, hashKey)
//                        }
//                    }.collect().toMap
//    allFiles
//
//  }

}


