package de.tub.dima.G_BENCHMARKING

import org.apache.spark.SparkConf

/**
  * @author dieutth, 06/06/2018
  *
  *  Main method to benchmark operators performance (by measuring execution time) when executing in IDE on small datasets.
  *  For a fair comparison, when perform a benchmark for a particular experiment, commenting all other benchmarks code,
  *  and run only the experiment of interest.
  */

object G_LocalBenchmarking {

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
    

    //Various datasets with different characteristics that could be used for benchmarking.
    val (refPath, expPath, loop) =
    //     ("/home/dieutth/data/gmql/parquet/TAD_Aidens_id", "/home/dieutth/data/gmql/parquet/TAD_Aidens_id", "3")
//     ("/home/dieutth/data/gmql/uncompressed/TADs_Aiden/", "/home/dieutth/data/gmql/uncompressed/TADs_Aiden/", "3")
    ("/home/dieutth/data/gmql/uncompressed/HG19_BED_ANNOTATION/", "/home/dieutth/data/gmql/uncompressed/HG19_BED_ANNOTATION/", "3")
    //     ("/home/dieutth/data/gmql/uncompressed/TADs_Aiden_withF/", "/home/dieutth/data/gmql/uncompressed/TADs_Aiden_withF/", "3")
    //     ("/home/dieutth/data/gmql/uncompressed/TADs_Aiden_working_medium/", "/home/dieutth/data/gmql/uncompressed/TADs_Aiden_working_medium/", "3")
    //     ("/home/dieutth/data/gmql/uncompressed/tmp/ref/", "/home/dieutth/data/gmql/uncompressed/tmp/exp/", "3")
    //     ("/home/dieutth/data/gmql/uncompressed/tmp_dupIn1Sample/", "/home/dieutth/data/gmql/uncompressed/tmp_dupIn1Sample/", "3")



//    val testCurrentMap = G_Benchmark(conf, refPath, expPath, "/home/dieutth/data/gmql/result/current_system_map").benchmarkCurrentSystem_Map(0)
//    val testCurrentJoin = G_Benchmark(conf, refPath, expPath, "/home/dieutth/data/gmql/result/current_system_join").benchmarkCurrentSystem_Join(0)
//    val testArrArrJoin = G_Benchmark(conf, refPath, expPath, "/home/dieutth/data/gmql/result/joinNormal").benchmarkArrArrJoin(0)
//    val testArrArrJoinNoCartesian = G_Benchmark(conf, refPath, expPath, "/home/dieutth/data/gmql/result/joinNoCartesian").benchmarkArrArrJoinNoCartesian(0)
//    val testArrArrJoinMultimatrix = G_Benchmark(conf, refPath, expPath, "/home/dieutth/data/gmql/result/joinMultimatrix").benchmarkArrArrJoinMultimatrix(0)
    val testArrArrMapNoCartesian = G_Benchmark(conf, refPath, expPath, "/home/dieutth/data/gmql/result/mapNoCartesian").benchmarkArrArrMapNoCartesian(0)
//    val testArrArrMapMultimatrix = G_Benchmark(conf, refPath, expPath, "/home/dieutth/data/gmql/result/mapMultimatrix").benchmarkArrArrMapMultimatrix(0)
//
  }

}
