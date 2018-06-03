package de.tub.dima

import com.google.common.hash.Hashing
import de.tub.dima.loaders.{CustomParser, Loaders}
import de.tub.dima.operators.join.{ArrArrJoin, ArrArrJoin_NoCartesian}
import de.tub.dima.operators.legacy.{MapArrArrNC_leftOuterJoin, Map_AA_NC_binInt}
import de.tub.dima.operators.map.Map_ArrArr_NoCartesian
import it.polimi.genomics.core.DataStructures.JoinParametersRD.{DistLess, RegionBuilder}
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core.{GDouble, GRecordKey, GValue}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach, FunSuite}


/**
  * @author dieutth, 03/06/2018
  * @see documentation at [[http://www.bioinformatics.deib.polimi.it/geco/?try]] for more information
  *      about Map operator semantic.
  * 
  * Tests for the correctness of GMQL Map operation in different scenarios.
  * The default aggregation function is Count.
  *         
  */
class TestMapCorrectness extends FunSuite with BeforeAndAfter{

  val configInfo: Map[String, String] = Map(
      ("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
      ("spark.kryoserializer.buffer", "64"),
      ("spark.driver.allowMultipleContexts","true"),
      ("spark.sql.tungsten.enabled", "true"),
      ("spark.executor.heartbeatInterval","300s"),
      ("spark.eventLog.enabled", "true"),
      ("spark.eventLog.dir","/tmp/spark-events"),
      ("spark.default.parallelism", "2")
  )
  var spark: SparkSession = _
  var sc: SparkContext = _
  val hasher = Hashing.md5().newHasher()
  val bin = 10000
  
  before{
    val conf = new SparkConf().setAppName("Test Map operations correctness")
      .setMaster("local[*]")
    for ((key, value) <- configInfo)
      conf.set(key, value)
    spark =  SparkSession.builder().config(conf).getOrCreate()
    sc = spark.sparkContext

  }
  
  after{
    if (sc != null)
        sc.stop
  }

  /**
    * ref: S0: (chr1	1	7	*	0.2	0.3)(chr1	1	3	*	0.2	0.7)
    *       S1: (chr1	1	3	*	0.7	0.2)(chr1	2	4	*	0.2	0.2)
    * exp: S0: (chr1	2	5	*	0.2	0.3)
    *       S1: (chr1	1	3	*	0.7	0.2)(chr1	2	4	*	0.2	0.2)
    */
  test("Arr-Arr: No region-duplication in each sample in both dataset"){
    val refFilePath = "./resources/no_region_duplicate_in_both/ref/"
    val expFilePath = "./resources/no_region_duplicate_in_both/exp/"
    val ref = loadDataset(refFilePath).transformToSingleMatrix()
    val exp = loadDataset(expFilePath).transformToSingleMatrix()

//    val actualResult: Array[Rep] = Map_ArrArr_NoCartesian(sc, ref, exp, bin).transformToRow().collect()
//                                        .map()


    val List(s00, s01, s10, s11) = generateId(refFilePath, expFilePath)
    val expectedResult = Array(
      (GRecordKey(s00, "chr1", 1, 7, '*'), Array[GValue](GDouble(0.2), GDouble(0.3)))
    )

  }



  private def loadDataset(path: String): RDD[GRECORD] = {
    Loaders.forPath(sc, path).LoadRegionsCombineFiles(new CustomParser().setSchema(path).region_parser)
  }

  private def generateId(refPath: String, expPath: String): List[Long]={
    val refIds = for (f1 <- Array("S_00000.gdm", "S_00001"))
                    yield hasher.putString((refPath+f1).replaceAll("/",""),java.nio.charset.StandardCharsets.UTF_8).hash().asLong()

    val expIds = for (f1 <- Array("S_00000.gdm", "S_00001"))
                      yield hasher.putString((expPath+f1).replaceAll("/",""),java.nio.charset.StandardCharsets.UTF_8).hash().asLong()

    val ids = for(refId <- refIds; expId <- expIds)
      yield hasher.putLong(refId).putLong(expId).hash().asLong
    ids.toList

  }

  implicit class TransformFromRow(ds: RDD[GRECORD]){
    def transformToSingleMatrix(): RDD[((Int, Long, Long, Short), (Array[Long],Array[Array[Double]]))] ={
      ds.groupBy{
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
    }

    def transformToMultiMatrix() = {
      ds.groupBy{
        x =>
          val y = x._1.chrom.substring(3)
          val chr: Int = try {
            y.toInt
          } catch {
            case e: Exception => if (y == "X") 23 else 24
          }
          (chr, x._1.start, x._1.stop, x._1.strand.toShort)
      }
        .map { x =>
          val (ids, features) = x._2.map(y => (y._1.id, y._2.flatMap(_ match
          { case GDouble(t) => Some(t)
            case _ => None
          }))).toArray.unzip

          (x._1, (Array(ids), Array(features)))

        }
    }
  }

  implicit class TransformFromSingleMatrix(ds: RDD[((Int, Long, Long, Short), (Array[Long],Array[Array[Double]]))]) {
    def transformToRow(): RDD[GRECORD]= {
      ds.flatMap {
        x =>
          val k = x._1
          for (item <- (x._2._1 zip x._2._2))
            yield (GRecordKey(item._1, "chr" + x._1, x._1._2, x._1._3, x._1._4.toChar), item._2.map(GDouble(_).asInstanceOf[GValue]))
      }

    }
  }
  implicit class TransformFromMultiMatrixToRow(ds: RDD[((Int, Long, Long, Short), (Array[Array[Long]],Array[Array[Array[Double]]]))]) {
    def transformToRow (): RDD[GRECORD]= {
      ds.flatMap {
        x =>
          val key = x._1
          val ids = x._2._1.slice(1, x._2._1.size).foldLeft(x._2._1.head)((l, r) => for (il <- l; ir <- r) yield Hashing.md5().newHasher().putLong(il).putLong(ir).hash().asLong)
          val features = x._2._2.slice(1, x._2._2.size).foldLeft(x._2._2.head)((l, r) => for (il <- l; ir <- r) yield il ++ ir)
          for (i <- 0 until ids.length)
            yield (GRecordKey(ids(i), "chr" + x._1, x._1._2, x._1._3, x._1._4.toChar), features(i).map(GDouble(_).asInstanceOf[GValue]))
      }
    }
  }
}

case class Rep()





