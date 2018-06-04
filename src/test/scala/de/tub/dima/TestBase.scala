package de.tub.dima

import com.google.common.hash.Hashing
import de.tub.dima.loaders.{CustomParser, Loaders}
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core.{GDouble, GRecordKey, GValue}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
  * @author dieutth, 04/06/2018
  *
  */
class TestBase extends FunSuite with BeforeAndAfter{

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
  val bin = 3

  val List(s00, s01, s02, s10, s11, s12): List[Long]={
    val refIds = for (f1 <- Array("S_00000.gdm", "S_00001.gdm"))
      yield Hashing.md5().newHasher().putString(f1,java.nio.charset.StandardCharsets.UTF_8).hash().asLong()

    val expIds = for (f1 <- Array("S_00000.gdm", "S_00001.gdm", "S_00002.gdm"))
      yield Hashing.md5().newHasher().putString(f1,java.nio.charset.StandardCharsets.UTF_8).hash().asLong()

    val ids = for(refId <- refIds; expId <- expIds)
      yield Hashing.md5().newHasher().putLong(refId).putLong(expId).hash().asLong
    ids.toList

  }
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


  protected def loadDataset(path: String): RDD[GRECORD] = {
    Loaders.forPath(sc, path).LoadRegionsCombineFiles(new CustomParser().setSchema(path).region_parser)
  }

  implicit class TransformFromRow(ds: RDD[GRECORD]){
    def transformToSingleMatrix(): RDD[((Int, Long, Long, Short), (Array[Long],Array[Array[Double]]))] ={
      ds.groupBy{
        x =>
          val y = x._1.chrom.substring(3)
          val chr: Int = try {
            y.toInt
          } catch {
            case _: Exception => if (y == "X") 23 else 24
          }
          (chr, x._1.start, x._1.stop, x._1.strand.toShort)
      }
        .map(x=>
          (x._1, x._2.map(y => (y._1.id, y._2.flatMap(_ match {case GDouble(t)=> Some(t)
          case _=> None}))).toArray.unzip)

        )
    }

    def transformToMultiMatrix(): RDD[((Int, Long, Long, Short), (Array[Array[Long]],Array[Array[Array[Double]]]))] = {
      ds.groupBy{
        x =>
          val y = x._1.chrom.substring(3)
          val chr: Int = try {
            y.toInt
          } catch {
            case _: Exception => if (y == "X") 23 else 24
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
    def transformToRow(): RDD[GRECORD]= ds.flatMap {
      x =>
        (x._2._1 zip x._2._2).map(item => (GRecordKey(item._1, "chr" + x._1._1, x._1._2, x._1._3, x._1._4.toChar), item._2.map(GDouble(_).asInstanceOf[GValue])))
    }
  }
  implicit class TransformFromMultiMatrixToRow(ds: RDD[((Int, Long, Long, Short), (Array[Array[Long]],Array[Array[Array[Double]]]))]) {
    def transformToRow (): RDD[GRECORD]= {
      ds.flatMap {
        x =>
          val ids = x._2._1.slice(1, x._2._1.length).foldLeft(x._2._1.head)((l, r) => for (il <- l; ir <- r) yield Hashing.md5().newHasher().putLong(il).putLong(ir).hash().asLong)
          val features = x._2._2.slice(1, x._2._2.length).foldLeft(x._2._2.head)((l, r) => for (il <- l; ir <- r) yield il ++ ir)
          for (i <- ids.indices)
            yield (GRecordKey(ids(i), "chr" + x._1._1, x._1._2, x._1._3, x._1._4.toChar), features(i).map(GDouble(_).asInstanceOf[GValue]))
      }
    }
  }
}

case class Rep(rec: GRECORD){
  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case that: Rep => rec._1 == that.rec._1 &&
        rec._2.sameElements(that.rec._2)
      case _ => false
    }
  }
  override def hashCode(): Int = rec._1.chrom.hashCode

  override def toString: String = rec._1.toString() + "\t" + rec._2.mkString("\t")

}
