package de.tub.dima.BENCHMARKING

import org.apache.spark.SparkConf

/**
  * @author dieutth, 06/06/2018
  *         
  * Main method for testing on cluster.
  *
  * Different from LocalBenchmarking, ClusterBenchmarking has:
  *
  *   - value of conf: SparkConf is minimized with some default values.
  *     Other configuration should be made when submitting the job (fat jar) to spark
  *     In particular, value of setMaster and spark.default.parallelism need to be provided
  *
  *   - value of method (to benchmark), refPath, expPath, outputPath, loop are obtained via list of arguments of main
  *
  */

object ClusterBenchmarking {

  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer", "64")
      .set("spark.driver.allowMultipleContexts","true")
      .set("spark.sql.tungsten.enabled", "true")
      .set("spark.executor.heartbeatInterval","5000s")
      .set("spark.network.timeout", "10000s")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir","/tmp/spark-events")

    if (args.length != 5){
      println(
        """Provide 5 arguments to execute!
          |args(0): method to benchmark. Possible values = (current_map, current_join, arrarr_join, arrarr_join_nc, arrarr_join_mm, arrarr_map_nc, arrarr_map_mm)
          |args(1): refPath. Absolute path to ref dataset
          |args(2): expPath. Absolute path to exp dataset
          |args(3): outputPath. Absolute path to write output dataset to.
          |args(4): loop. An integer represents number of loop
          |An example of correct arguments list:
          |current_map /home/dieutth/gmqldata/tmp/ref/ /home/dieutth/gmqldata/tmp/exp/ /home/dieutth/result/current_map/ 1
        """.stripMargin)

    }else{
      val List(method, refPath, expPath, outputPath, loop) = args.toList
      
      method.toLowerCase() match{
        case "current_map" => 
              Benchmark(conf, refPath, expPath, outputPath).benchmarkCurrentSystem_Map(loop.toInt)
        case "current_join" => 
              Benchmark(conf, refPath, expPath, outputPath).benchmarkCurrentSystem_Join(loop.toInt)
        case "arrarr_join" => 
              Benchmark(conf, refPath, expPath, outputPath).benchmarkArrArrJoin(loop.toInt)
        case "arrarr_join_nc"=>
              Benchmark(conf, refPath, expPath, outputPath).benchmarkArrArrJoinNoCartesian(loop.toInt)
        case "arrarr_join_mm" => 
              Benchmark(conf, refPath, expPath, outputPath).benchmarkArrArrJoinMultimatrix(loop.toInt)
        case "arrarr_map_nc" =>
              Benchmark(conf, refPath, expPath, outputPath).benchmarkArrArrMapNoCartesian(loop.toInt)
        case "arrarr_map_mm" =>
              Benchmark(conf, refPath, expPath, outputPath).benchmarkArrArrMapMultimatrix(loop.toInt)
      }
      
    }
  }

}
