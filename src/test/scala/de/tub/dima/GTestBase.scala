package de.tub.dima

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core.{GDouble, GRecordKey, GValue}
import it.polimi.genomics.spark.implementation.loaders.{CustomParser, Loaders}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

class GTestBase extends FunSuite with BeforeAndAfter{

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

  val refFilePath_NoRegionDup= "/home/dieutth/git/MGD-GMQL/src/test/resources/withGValue/no_region_duplicate_in_both/ref/"
  val expFilePath_NoRegionDup =  "/home/dieutth/git/MGD-GMQL/src/test/resources/withGValue/no_region_duplicate_in_both/exp/"

  val refFilePath_RegionDupInREF = "/home/dieutth/git/MGD-GMQL/src/test/resources/withGValue/region_dup_in_ref_only/ref/"
  val expFilePath_RegionDupInREF = "/home/dieutth/git/MGD-GMQL/src/test/resources/withGValue/region_dup_in_ref_only/exp/"

  val refFilePath_RegionDupInEXP = "/home/dieutth/git/MGD-GMQL/src/test/resources/withGValue/region_dup_in_exp_only/ref/"
  val expFilePath_RegionDupInEXP = "/home/dieutth/git/MGD-GMQL/src/test/resources/withGValue/region_dup_in_exp_only/exp/"

  val refFilePath_RegionDupInBOTH = "/home/dieutth/git/MGD-GMQL/src/test/resources/withGValue/region_dup_in_both/ref/"
  val expFilePath_RegionDupInBOTH = "/home/dieutth/git/MGD-GMQL/src/test/resources/withGValue/region_dup_in_both/exp/"

  val refFilePath_RecordDupInREF = "/home/dieutth/git/MGD-GMQL/src/test/resources/withGValue/record_dup_in_ref_only/ref/"
  val expFilePath_RecordDupInREF = "/home/dieutth/git/MGD-GMQL/src/test/resources/withGValue/record_dup_in_ref_only/exp/"


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
    *
    * ============================================= HELPER METHODS AND CLASSES =============================================*
    *
    */

  /**
    * Load data from files to RDD, using loaders from current System GMQL-Spark
    * @param path absolute path to folder contains sample files of the dataset
    * @return row-based dataset (an RDD of GRECORD)
    */
  protected def loadDataset(path: String): RDD[GRECORD] = {
    Loaders.forPath(sc, path).LoadRegionsCombineFiles(new CustomParser().setSchema(path).region_parser)
  }


  /**
    * Implicit class to transform a row-based dataset to array-based dataset
    * @param ds input dataset, in form of an RDD[GRECORD]
    */
  implicit class TransformFromRow(ds: RDD[GRECORD]){
    def transformToSingleMatrix(): RDD[((String, Long, Long, Short), (Array[Long],Array[Array[GValue]]))] ={
      ds.groupBy{
        x =>

          (x._1.chrom, x._1.start, x._1.stop, x._1.strand.toShort)
      }
        .map(x=>
          (x._1, x._2.map(y => (y._1.id, y._2)).toArray.unzip)

        )
    }

    def transformToMultiMatrix(): RDD[((String, Long, Long, Short), (Array[Array[Long]],Array[Array[Array[GValue]]]))] = {
      ds.groupBy{
        x =>

          (x._1.chrom, x._1.start, x._1.stop, x._1.strand.toShort)
      }
        .map { x =>
          val (ids, features) = x._2.map(y => (y._1.id, y._2)).toArray.unzip

          (x._1, (Array(ids), Array(features)))

        }
    }
  }


  /**
    * Implicit class to transform a dataset in array-singleMatrix-based to row-based
    * @param ds input array-singleMatrix-based dataset
    */
  implicit class TransformFromSingleMatrix(ds: RDD[((String, Long, Long, Short), (Array[Long],Array[Array[GValue]]))]) {
    def transformToRow(): RDD[GRECORD]= ds.flatMap {
      x =>
        (x._2._1 zip x._2._2).map(item => (GRecordKey(item._1, x._1._1, x._1._2, x._1._3, x._1._4.toChar), item._2))
    }

    def writeToFile(output: String): Unit = {
      ds.flatMap {
        x =>
          (x._2._1 zip x._2._2).map(item => (GRecordKey(item._1, x._1._1, x._1._2, x._1._3, x._1._4.toChar), item._2.mkString(", ")))
      }
        .saveAsTextFile(output)
    }
  }


  /**
    * Implicit class to transform a dataset in array-multiMatrix-based to row-based
    * @param ds input array-multiMatrix-based dataset
    */
  implicit class TransformFromMultiMatrixToRow(ds: RDD[((String, Long, Long, Short), (Array[Array[Long]],Array[Array[Array[GValue]]]))]) {
    def transformToRow (): RDD[GRECORD]= {
      ds.flatMap {
        x =>
          val ids = x._2._1.slice(1, x._2._1.length).foldLeft(x._2._1.head)((l, r) => for (il <- l; ir <- r) yield Hashing.md5().newHasher().putLong(il).putLong(ir).hash().asLong)

          val features = x._2._2.slice(1, x._2._2.length).foldLeft(x._2._2.head)((l, r) => for (il <- l; ir <- r) yield il ++ ir)
          for (i <- ids.indices)
            yield (GRecordKey(ids(i), x._1._1, x._1._2, x._1._3, x._1._4.toChar), features(i))
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