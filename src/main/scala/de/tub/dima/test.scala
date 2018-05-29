package de.tub.dima

import loaders.CustomParser
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
    val sc:SparkContext =new SparkContext(conf)

    val server = new GmqlServer(new MGD_GMQLSparkExecutor(sc=sc,outputFormat = GMQLSchemaFormat.TAB))

    val dsFilePath1 = "/home/dieutth/data/gmql/uncompressed/TADs_Aiden/" // /home/dieutth/share/data/Example_Dataset_1/files"
    val dsFilePath2 = "/home/dieutth/data/gmql/uncompressed/TADs_Aiden/" // /home/dieutth/share/data/Example_Dataset_2/files"

    val timestamp = System.currentTimeMillis()
    val ds1 = server READ(dsFilePath1 ) USING (new CustomParser().setSchema(dsFilePath1))
    val ds2 = server READ(dsFilePath2 ) USING (new CustomParser().setSchema(dsFilePath2))

/*-------------------------Testing for Map operator-----------------------------------------------------*/
//    val map = ds1.MAP(None,List(),ds2)
//    server setOutputPath("/home/dieutth/result/") MATERIALIZE(map)
//    server.run()
//    println ("Execution time of current system join: ",(System.currentTimeMillis() - timestamp )/1000)


    /*-------------------------Testing for Join operator-----------------------------------------------------*/
        var join = ds1.JOIN(None, List(new JoinQuadruple(Some(DistLess(70000)))), RegionBuilder.LEFT, ds2)

//        for (i <- Range(1, 10))
          join = join.JOIN(None, List(new JoinQuadruple(Some(DistLess(70000)))), RegionBuilder.LEFT, ds2)

        server setOutputPath("/home/dieutth/result/tmpp4/") MATERIALIZE(join)
        server.run()
        println ("Execution time of current system join: ",(System.currentTimeMillis() - timestamp )/1000)



  }

}
