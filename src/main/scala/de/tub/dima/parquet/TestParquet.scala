package de.tub.dima.parquet

import de.tub.dima.MGD_GMQLSparkExecutor
import de.tub.dima.dimension.Coordinate
import de.tub.dima.loaders.Loaders
import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

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
    val ds1 = spark.read.parquet("/home/dieutth/data/gmql/parquet/TAD_Aidens_id")
                .rdd
                .map(x => (GRecordKey(x.getLong(0), x.getString(1), x.getLong(2), x.getLong(3), x.getShort(4).toChar), Array[GValue]()))

    val metaDs = Loaders forPath(sc, "/home/dieutth/data/gmql/uncompressed/TADs_Aiden/*.meta") LoadMetaCombineFiles(meta_parser)
    val ref1 = server READ("") USING(metaDs, ds1, List[(String, PARSING_TYPE)]())
    val ref2 = server READ("") USING(metaDs, ds1, List[(String, PARSING_TYPE)]())

    val map = ref1 MAP(None, List(), ref2)
    server setOutputPath("/home/dieutth/data/gmql/result/") MATERIALIZE(map)
    server.run()

    println ("Execution time when running without custom-partitioner:",(System.currentTimeMillis() - startTime )/1000)


    //Array based
    //RDD[(Coordinate, Array[])]]
//    val ds1Arr //: RDD[(Coordinate,Array[Array])]
//    = spark.read.parquet("/home/dieutth/data/gmql/parquet/TAD_Aidens_id")
//                    .rdd

  }
}
