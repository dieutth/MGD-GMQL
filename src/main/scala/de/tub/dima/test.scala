package de.tub.dima

import de.tub.dima.loaders.CustomParser
import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core._
import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Partitioner, updated Gmap71, twiceDS").setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryoserializer.buffer", "64")
//      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.driver.allowMultipleContexts","true")
      .set("spark.sql.tungsten.enabled", "true")
      .set("spark.executor.heartbeatInterval","300s")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir","/tmp/spark-events")
    val sc:SparkContext =new SparkContext(conf)

    val server = new GmqlServer(new MGD_GMQLSparkExecutor(sc=sc,outputFormat = GMQLSchemaFormat.TAB))

//    val metaDS = sc.parallelize((1 to 10).map(x=> (1l,("test","Abdo"))))
//    println("ref size: ",(1 until 1000000000 by 1000).size)
//    val exp = (1 until 1000000000 by 1000)
//    println("exp:size ",exp.size)
//    val regionDS1 = sc.parallelize((1 until 1000000000 by 1000).map{x=>(new GRecordKey(1,"Chr"+(x%2),x,x+200,'*'),Array[GValue](GDouble(1)) )}).partitionBy(new CustomPartitioner(2))
//    val regionDS2 = sc.parallelize(exp.map{x=>(new GRecordKey(1,"Chr"+(x%2),x,x+200,'*'),Array[GValue](GDouble(1)) )}).partitionBy(new CustomPartitioner(2))
//
//    val timestamp = System.currentTimeMillis()
//    val ds1 = server.READ("").USING(metaDS,regionDS1,List[(String, PARSING_TYPE)](("score",ParsingType.DOUBLE)))
//    val ds2 = server.READ("").USING(metaDS,regionDS2,List[(String, PARSING_TYPE)](("score",ParsingType.DOUBLE)))
//
//
//    val cover = ds1.MAP(None,List(),ds2)
//
//    val output = server.setOutputPath("").TAKE(cover,100)
//    server.run()
//    println ("EXEC Time is: ",(System.currentTimeMillis() - timestamp )/1000)

    val dsFilePath1 = "/home/dieutth/data/gmql/uncompressed/TADs_Aiden/" // /home/dieutth/share/data/Example_Dataset_1/files"
    val dsFilePath2 = "/home/dieutth/data/gmql/uncompressed/TADs_Aiden/" // /home/dieutth/share/data/Example_Dataset_2/files"

    val timestamp = System.currentTimeMillis()
    val ds1 = server READ(dsFilePath1 ) USING (new CustomParser().setSchema(dsFilePath1))
    val ds2 = server READ(dsFilePath2 ) USING (new CustomParser().setSchema(dsFilePath2))


//    server COLLECT(ds1)
//    val cover = ds1.MAP(None,List(),ds2)
//    val result = server TAKE (cover, 10)
//    server setOutputPath("/home/dieutth/data/gmql/result/") MATERIALIZE(cover)
    server.run()

    println ("Execution time when running without custom-partitioner:",(System.currentTimeMillis() - timestamp )/1000)

//    result.asInstanceOf[GMQL_DATASET]._1.foreach(record =>
//      println(record._1 + "\t" + record._2.map(x => x.toString).mkString("\t")))

  }

}
