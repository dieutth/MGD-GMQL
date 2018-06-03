package de.tub.dima

import loaders.{BedParser, CustomParser, Loaders}
import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.DataStructures.JoinParametersRD.{DistLess, JoinQuadruple, RegionBuilder}
import it.polimi.genomics.core._
import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Partitioner, updated Gmap71, twiceDS").setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryoserializer.buffer", "64")
      .set("spark.driver.allowMultipleContexts","true")
      .set("spark.sql.tungsten.enabled", "true")
      .set("spark.executor.heartbeatInterval","300s")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir","/tmp/spark-events")
      .set("spark.default.parallelism", "2")
    val sc:SparkContext =new SparkContext(conf)

    val server = new GmqlServer(new MGD_GMQLSparkExecutor(sc=sc,outputFormat = GMQLSchemaFormat.TAB))

//    val dsFilePath1 = "/home/dieutth/data/gmql/uncompressed/TADs_Aiden_working_medium/"///home/dieutth/data/gmql/uncompressed/TADs_Aiden/" // /home/dieutth/share/data/Example_Dataset_1/files"
//    val dsFilePath2 = "/home/dieutth/data/gmql/uncompressed/TADs_Aiden_working_medium/"///home/dieutth/data/gmql/uncompressed/TADs_Aiden/" // /home/dieutth/share/data/Example_Dataset_2/files"

    val (dsFilePath1, dsFilePath2, loop) = if (args.length == 3) (args(0), args(1), args(2))
    //    else ("/home/dieutth/data/gmql/parquet/TAD_Aidens_id", "/home/dieutth/data/gmql/parquet/TAD_Aidens_id", "3")
    //    else ("/home/dieutth/data/gmql/uncompressed/TADs_Aiden/", "/home/dieutth/data/gmql/uncompressed/TADs_Aiden/", "3")
//        else ("/home/dieutth/data/gmql/uncompressed/TADs_Aiden_working_medium/", "/home/dieutth/data/gmql/uncompressed/TADs_Aiden_working_medium/", "3")
    else ("/home/dieutth/data/gmql/uncompressed/tmp/ref/", "/home/dieutth/data/gmql/uncompressed/tmp/exp/", "3")
//    else ("/home/dieutth/data/gmql/uncompressed/tmp_dupIn1Sample/", "/home/dieutth/data/gmql/uncompressed/tmp_dupIn1Sample/", "3")

    testMap(0)
    def testMap(loop: Int): Unit = {
      val timestamp = System.currentTimeMillis()
      val ds1 = server READ(dsFilePath1 ) USING (new CustomParser().setSchema(dsFilePath1))
      val ds2 = server READ(dsFilePath2 ) USING (new CustomParser().setSchema(dsFilePath2))

      var map = ds1.MAP(None,List(),ds2)
      for (i <- Range(0, loop))
        map = ds1.MAP(None,List(),map)

      server setOutputPath("/home/dieutth/result/current_system_map/") MATERIALIZE(map)
      server.run()
      println ("Execution time of current system map: " + (System.currentTimeMillis() - timestamp )/1000 + " loop = " + loop)
    }


    def testJoin: Unit = {
      val timestamp = System.currentTimeMillis()
      val ds1 = server READ(dsFilePath1 ) USING (new CustomParser().setSchema(dsFilePath1))
      val ds2 = server READ(dsFilePath2 ) USING (new CustomParser().setSchema(dsFilePath2))

      var join = ds1.JOIN(None, List(new JoinQuadruple(Some(DistLess(70000)))), RegionBuilder.LEFT, ds2)

      //        for (i <- Range(1, 10))
     // join = ds2.JOIN(None, List(new JoinQuadruple(Some(DistLess(70000)))), RegionBuilder.LEFT, join)

      server setOutputPath("/home/dieutth/result/current_system_join/") MATERIALIZE(join)
      server.run()
      println ("Execution time of current system join: ",(System.currentTimeMillis() - timestamp )/1000)

    }

  }
}
