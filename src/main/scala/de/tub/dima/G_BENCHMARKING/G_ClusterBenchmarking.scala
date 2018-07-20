package de.tub.dima.G_BENCHMARKING

import org.apache.spark.SparkConf

/**
  * @author dieutth, 06/06/2018
  *         
  * Main method for testing on cluster.
  *
  * Different from G_LocalBenchmarking, G_ClusterBenchmarking has:
  *
  *   - value of conf: SparkConf is minimized with some default values.
  *     Other configuration should be made when submitting the job (fat jar) to spark
  *     In particular, value of setMaster and spark.default.parallelism need to be provided
  *
  *   - value of method (to benchmark), refPath, expPath, "", loop are obtained via list of arguments of main
  *
  */

object G_ClusterBenchmarking {

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


    if (args.isEmpty) {
      println(
        """Provide arguments to execute!""".stripMargin)
    }
    else{
      
      val query = args(0)
                  
      query.toLowerCase() match{
        case "current_map" =>
              val List(refPath, expPath) = args.toList.tail
              G_Benchmark(conf, refPath, expPath, "").benchmarkCurrentSystem_Map(1)
          
        case "current_join" =>
              val List(refPath, expPath) = args.toList.tail
              G_Benchmark(conf, refPath, expPath, "").benchmarkCurrentSystem_Join(1)
          
        case "arrarr_join" =>
              val List(refPath, expPath) = args.toList.tail
              G_Benchmark(conf, refPath, expPath, "").benchmarkArrArrJoin(1)
          
        case "arrarr_join_nc"=>
              val List(refPath, expPath) = args.toList.tail
              G_Benchmark(conf, refPath, expPath, "").benchmarkArrArrJoinNoCartesian(1)
          
        case "arrarr_join_mm" =>
              val List(refPath, expPath) = args.toList.tail
              G_Benchmark(conf, refPath, expPath, "").benchmarkArrArrJoinMultimatrix(1)
          
        case "arrarr_map_nc" =>
              val List(refPath, expPath) = args.toList.tail
              G_Benchmark(conf, refPath, expPath, "").benchmarkArrArrMapNoCartesian(1)
          
        case "arrarr_map_mm" =>
              val List(refPath, expPath) = args.toList.tail
              G_Benchmark(conf, refPath, expPath, "").benchmarkArrArrMapMultimatrix(1)

          
          //*****************************************************************************************
          //*************************************COMPLEX QUERIES*************************************
          //*****************************************************************************************

        case "current-complex-map" =>
          val List(refPath, expPath, loop) = args.toList.tail
          G_Benchmark(conf, refPath, expPath, "").benchmarkCurrentSystem_Map(loop.toInt)

        case "current-complex-join" =>
          val List(refPath, expPath, loop) = args.toList.tail
          G_Benchmark(conf, refPath, expPath, "").benchmarkCurrentSystem_Join(loop.toInt)

       
        case "single-complex-join"=>
          val List(refPath, expPath, loop) = args.toList.tail
          G_Benchmark(conf, refPath, expPath, "").benchmarkArrArrJoinNoCartesian(loop.toInt)

        case "multi-complex-join" =>
          val List(refPath, expPath, loop) = args.toList.tail
          G_Benchmark(conf, refPath, expPath, "").benchmarkArrArrJoinMultimatrix(loop.toInt)

        case "single-complex-map" =>
          val List(refPath, expPath, loop) = args.toList.tail
          G_Benchmark(conf, refPath, expPath, "").benchmarkArrArrMapNoCartesian(loop.toInt)

        case "multi-complex-map" =>
          val List(refPath, expPath, loop) = args.toList.tail
          G_Benchmark(conf, refPath, expPath, "").benchmarkArrArrMapMultimatrix(loop.toInt)


        case "complex-1-current" =>
          val List(refPath, expPath, third) = args.toList.tail
          G_Benchmark(conf, refPath, expPath, "").benchmarkCurrentSystemMapMap(third)

        case "complex-2-current" =>
          val List(ds1, ds2, ds3) = args.toList.tail
          G_Benchmark(conf, "").benchmarkCurrentSystem_ComplexQuery_2(ds1, ds2, ds3)

        case "complex-3-current" =>
          val List(ds1, ds2, ds3, ds4) = args.toList.tail
          G_Benchmark(conf, "").benchmarkCurrentSystem_ComplexQuery_3(ds1, ds2, ds3, ds4)
        case "complex-4-current" =>
          val List(ds1, ds2, ds3, ds4, ds5, ds6) = args.toList.tail
          G_Benchmark(conf, "").benchmarkCurrentSystem_ComplexQueries_4(ds1, ds2, ds3, ds4, ds5, ds6)

        case "complex-1-single" =>
          val List(refPath, expPath, third) = args.toList.tail
          G_Benchmark(conf, refPath, expPath, "").benchmarkMapMap_NC(third)

        case "complex-2-single" =>
          val List(ds1, ds2, ds3) = args.toList.tail
          G_Benchmark(conf, "").benchmark_Single_Complex_2(ds1, ds2, ds3)

        case "complex-3-single" =>
          val List(ds1, ds2, ds3, ds4) = args.toList.tail
          G_Benchmark(conf, "").benchmark_Single_Complex_3(ds1, ds2, ds3, ds4)

        case "complex-4-single" =>
          val List(ds1, ds2, ds3, ds4, ds5, ds6) = args.toList.tail
          G_Benchmark(conf, "").benchmark_Single_Complex_4(ds1, ds2, ds3, ds4, ds5, ds6)

        case "complex-1-multi" =>
          val List(refPath, expPath, third) = args.toList.tail
          G_Benchmark(conf, refPath, expPath, "").benchmarkMapMap_MM(third)

        case "complex-2-multi" =>
          val List(ds1, ds2, ds3) = args.toList.tail
          G_Benchmark(conf, "").benchmark_Multi_Complex_2(ds1, ds2, ds3)

        case "complex-3-multi" =>
          val List(ds1, ds2, ds3, ds4) = args.toList.tail
          G_Benchmark(conf, "").benchmark_Multi_Complex_3(ds1, ds2, ds3, ds4)

        case "complex-4-multi" =>
          val List(ds1, ds2, ds3, ds4, ds5, ds6) = args.toList.tail
          G_Benchmark(conf, "").benchmark_Multi_Complex_4(ds1, ds2, ds3, ds4, ds5, ds6)
      }

    }

  }

}
