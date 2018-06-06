package de.tub.dima.BENCHMARKING

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.GMQLSchemaFormat
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.{SparkConf, SparkContext}

object LocalBenchmarking {

  def main(args: Array[String]): Unit = {
      val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryoserializer.buffer", "64")
      .set("spark.driver.allowMultipleContexts","true")
      .set("spark.sql.tungsten.enabled", "true")
      .set("spark.executor.heartbeatInterval","300s")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir","/tmp/spark-events")
      .set("spark.default.parallelism", "2")
    

    val (refPath, expPath, loop) =
    //     ("/home/dieutth/data/gmql/parquet/TAD_Aidens_id", "/home/dieutth/data/gmql/parquet/TAD_Aidens_id", "3")
     ("/home/dieutth/data/gmql/uncompressed/TADs_Aiden/", "/home/dieutth/data/gmql/uncompressed/TADs_Aiden/", "3")
    //     ("/home/dieutth/data/gmql/uncompressed/TADs_Aiden_withF/", "/home/dieutth/data/gmql/uncompressed/TADs_Aiden_withF/", "3")
    //     ("/home/dieutth/data/gmql/uncompressed/TADs_Aiden_working_medium/", "/home/dieutth/data/gmql/uncompressed/TADs_Aiden_working_medium/", "3")
    //     ("/home/dieutth/data/gmql/uncompressed/tmp/ref/", "/home/dieutth/data/gmql/uncompressed/tmp/exp/", "3")
    //     ("/home/dieutth/data/gmql/uncompressed/tmp_dupIn1Sample/", "/home/dieutth/data/gmql/uncompressed/tmp_dupIn1Sample/", "3")
    //    ("/home/dieutth/git/MGD-GMQL/src/test/resources/no_region_duplicate_in_both/ref/",
    //          "/home/dieutth/git/MGD-GMQL/src/test/resources/no_region_duplicate_in_both/exp/" ,"3")


//    val testCurrentMap = Benchmark(conf, refPath, expPath, "/home/dieutth/data/gmql/result/current_system_map").benchmarkCurrentSystem_Map(0)
//    val testCurrentJoin = Benchmark(conf, refPath, expPath, "/home/dieutth/data/gmql/result/current_system_join").benchmarkCurrentSystem_Join(0)
//    val testArrArrJoin = Benchmark(conf, refPath, expPath, "/home/dieutth/data/gmql/result/joinNormal").benchmarkArrArrJoin(0)
//    val testArrArrJoinNoCartesian = Benchmark(conf, refPath, expPath, "/home/dieutth/data/gmql/result/joinNoCartesian").benchmarkArrArrJoinNoCartesian(0)
//    val testArrArrJoinMultimatrix = Benchmark(conf, refPath, expPath, "/home/dieutth/data/gmql/result/joinMultimatrix").benchmarkArrArrJoinMultimatrix(0)
//    val testArrArrMapNoCartesian = Benchmark(conf, refPath, expPath, "/home/dieutth/data/gmql/result/mapNoCartesian").benchmarkArrArrMapNoCartesian(0)
//    val testArrArrMapMultimatrix = Benchmark(conf, refPath, expPath, "/home/dieutth/data/gmql/result/mapMultimatrix").benchmarkArrArrMapMultimatrix(0)
    
  }

}
