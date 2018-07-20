package de.tub.dima.G_BENCHMARKING

import com.google.common.hash.Hashing
import de.tub.dima.opGvalue.join.{ArrArrJoin, ArrArrJoin_Multimatrix, ArrArrJoin_NoCartesian}
import de.tub.dima.opGvalue.map.{Map_ArrArr_Multimatrix, Map_ArrArr_NoCartesian}
import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.DataStructures.JoinParametersRD.{DistLess, JoinQuadruple, RegionBuilder}
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core._
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.RegionsOperators.SelectRegions.StoreMEMRD
import it.polimi.genomics.spark.implementation.loaders.{CustomParser, Loaders}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author dieutth, 06/06/2018
  */

object G_Benchmark{
  def apply(conf: SparkConf, refFilePath: String, expFilePath: String,
              outputPath: String) = new G_Benchmark(conf, refFilePath, expFilePath, outputPath)

  def apply(conf: SparkConf, outputPath: String): G_Benchmark = new G_Benchmark(conf, "", "", outputPath)
}


class G_Benchmark(conf: SparkConf, refFilePath: String, expFilePath: String, outputPath: String){
  var sc: SparkContext = _
  var server: GmqlServer = _
  val bin = 10000
  val distLess = Some(DistLess(0))
  val distGreater = None
  val bS = BinSize(bin, bin, bin)


  /**
    * Measure execution time of a MAP operation using current system GMQL-Spark
    * @param loop
    */
  def benchmarkCurrentSystem_Map(loop: Int): Unit = {
    conf.setAppName("Current System Map")
    sc = new SparkContext(conf)
    val executor = new GMQLSparkExecutor(sc=sc, binSize = bS, outputFormat = GMQLSchemaFormat.TAB)
    server = new GmqlServer(executor)

    val startTime = System.currentTimeMillis()
    val ds1 = server READ(refFilePath ) USING (new CustomParser().setSchema(refFilePath))
    val ds2 = server READ(expFilePath ) USING (new CustomParser().setSchema(expFilePath))

    var map = ds1.MAP(None,List(),ds2)
    for (i <- Range(1, loop))
      map = map.MAP(None,List(),ds1)

    val rdd = StoreMEMRD(executor, "", map.regionDag, map.metaDag, map.schema, sc)
    val count = rdd.count()
    println("Count = " + count)
//    server setOutputPath(outputPath) MATERIALIZE(map)
//    server.run()
    println ("Execution time of current system map: " + (System.currentTimeMillis() - startTime)/1000 + " loop = " + loop)

  }

  /**
    * Measure execution time of a JOIN operation using current system GMQL-Spark
    * @param loop
    */
  def benchmarkCurrentSystem_Join(loop: Int): Unit = {
    conf.setAppName("Current System Join")
    sc = new SparkContext(conf)
    val executor = new GMQLSparkExecutor(sc=sc, binSize = bS, outputFormat = GMQLSchemaFormat.TAB)
    server = new GmqlServer(executor)
    val startTime = System.currentTimeMillis()
    val ds1 = server READ(refFilePath ) USING (new CustomParser().setSchema(refFilePath))
    val ds2 = server READ(expFilePath ) USING (new CustomParser().setSchema(expFilePath))

    var join = ds1.JOIN(None, List(new JoinQuadruple(distLess)), RegionBuilder.LEFT, ds2)

    for (i <- Range(1, loop))
      join = ds2.JOIN(None, List(new JoinQuadruple(distLess)), RegionBuilder.LEFT, join)

    val rdd = StoreMEMRD(executor, "", join.regionDag, join.metaDag, join.schema, sc)
    val count = rdd.count()
    println("Count = " + count)

//    server setOutputPath(outputPath) MATERIALIZE(join)
//    server.run()
    println ("Execution time of current system join: ",(System.currentTimeMillis() - startTime)/1000)

  }

  /**
    * Measure execution time for JOIN using Arr-Arr representation
    * @param loop
    */
  def benchmarkArrArrJoin(loop: Int): Unit = {
    conf.setAppName("ArrArr JOIN")
    sc = new SparkContext(conf)
    val startTime = System.currentTimeMillis()
    val ref = loadDataset(refFilePath).transformToSingleMatrix()
    val exp = loadDataset(expFilePath).transformToSingleMatrix()

    var join = ArrArrJoin(ref, exp, bin, RegionBuilder.LEFT, distLess, distGreater)
    for (i <- Range(1, loop))
      join = ArrArrJoin(join, ref, bin, RegionBuilder.LEFT, distLess, distGreater)

//    join.transformToRow().saveAsTextFile(outputPath)
    val count = join.transformToRow().count()
    println("Count = " + count)
    println("Execution time for normal join: " + (System.currentTimeMillis() - startTime) / 1000)

  }

  /**
    * Measure execution time for JOIN using Arr-Arr representation, with optimization by not directly performing
    * Cartesian product on intermediate result
    * @param loop
    */
  def benchmarkArrArrJoinNoCartesian(loop: Int): Unit = {
    conf.setAppName("ArrArr JOIN NoCartesian")
    sc = new SparkContext(conf)
    val startTime = System.currentTimeMillis()
    val ref = loadDataset(refFilePath).transformToSingleMatrix()
    val exp = loadDataset(expFilePath).transformToSingleMatrix()

    var join = ArrArrJoin_NoCartesian(ref, exp, bin, RegionBuilder.LEFT, distLess, distGreater)
    for (i <- Range(1, loop))
      join = ArrArrJoin_NoCartesian(join, exp, bin, RegionBuilder.LEFT, distLess, distGreater)

    val count = join.transformToRow().count()
    println("Count = " + count)
//    join.transformToRow().saveAsTextFile(outputPath)
    println("Execution time for join NoCartesian: " + (System.currentTimeMillis() - startTime) / 1000)

  }
  

  /**
    * Measure execution time for JOIN using Arr-Arr representation. 
    * Ref dataset is multiMatrix-based 
    * Exp dataset is singleMatrix-based
    * @param loop
    */
  def benchmarkArrArrJoinMultimatrix(loop: Int): Unit = {
    conf.setAppName("ArrArr JOIN Multimatrix")
    sc = new SparkContext(conf)
    val startTime = System.currentTimeMillis()
    val ref = loadDataset(refFilePath).transformToMultiMatrix()
    val exp = loadDataset(expFilePath).transformToSingleMatrix()

    var join = ArrArrJoin_Multimatrix(ref, exp, bin, RegionBuilder.LEFT, distLess, distGreater)
    for (i <- Range(1, loop))
      join = ArrArrJoin_Multimatrix(join, exp, bin, RegionBuilder.LEFT, distLess, distGreater)

    val count = join.transformToRow().count()
    println("Count = " + count)
//    join.transformToRow().saveAsTextFile(outputPath)
    println("Execution time for join Multimatrix: " + (System.currentTimeMillis() - startTime) / 1000)

  }


  /**
    * Measure execution time for MAP using Arr-Arr representation, with optimization by not directly performing
    * Cartesian product on intermediate result
    * @param loop
    */
  def benchmarkArrArrMapNoCartesian(loop: Int): Unit = {
    conf.setAppName("ArrArr MAP NoCartesian")
    sc = new SparkContext(conf)
    val startTime = System.currentTimeMillis()
    val ref = loadDataset(refFilePath).transformToSingleMatrix()
    val exp = loadDataset(expFilePath).transformToSingleMatrix()

    var map = Map_ArrArr_NoCartesian(sc, ref, exp, bin)
    for (i <- Range(1, loop))
      map = Map_ArrArr_NoCartesian(sc, map, exp, bin)

    val count = map.transformToRow().count()
    println("Count = " + count)
//    map.transformToRow().saveAsTextFile(outputPath)
    println("Execution time for Map NoCartesian: " + (System.currentTimeMillis() - startTime) / 1000)

  }


  /**
    * Measure execution time for MAP using Arr-Arr representation.
    * Ref dataset is multiMatrix-based 
    * Exp dataset is singleMatrix-based
    * @param loop
    */
  def benchmarkArrArrMapMultimatrix(loop: Int): Unit = {
    conf.setAppName("ArrArr MAP Multimatrix")
    sc = new SparkContext(conf)
    val startTime = System.currentTimeMillis()
    val ref = loadDataset(refFilePath).transformToMultiMatrix()
    val exp = loadDataset(expFilePath).transformToSingleMatrix()

    var map = Map_ArrArr_Multimatrix(sc, ref, exp, bin)
    for (i <- Range(1, loop))
      map = Map_ArrArr_Multimatrix(sc, map, exp, bin)

    val count = map.transformToRow().count()
    println("Count = " + count)
//    map.transformToRow().saveAsTextFile(outputPath)
    println("Execution time for Map MultiMatrix: " + (System.currentTimeMillis() - startTime) / 1000)
  }


  def benchmarkCurrentSystemMapMap(third: String): Unit = {
    conf.setAppName("Current System A MAP (B MAP C) ")
    sc = new SparkContext(conf)
    val executor = new GMQLSparkExecutor(sc=sc, binSize = bS, outputFormat = GMQLSchemaFormat.TAB)
    server = new GmqlServer(executor)

    val ds1 = server READ(refFilePath ) USING (new CustomParser().setSchema(refFilePath))
    val ds2 = server READ(expFilePath ) USING (new CustomParser().setSchema(expFilePath))
    val ds3 = server READ(third ) USING (new CustomParser().setSchema(third))

    val map = ds1 MAP (None, List(), ds2 MAP(None, List(), ds3))
    val rdd = StoreMEMRD(executor, "", map.regionDag, map.metaDag, map.schema, sc)
    val count = rdd.count()
    println("Count = " + count)
//    server setOutputPath(outputPath) MATERIALIZE(map)
//    server.run()
  }


  def benchmarkMapMap_MM(third: String): Unit = {
    conf.setAppName("ArrArr A MAP (B MAP C)  MM-NC")
    sc = new SparkContext(conf)

    val ref = loadDataset(refFilePath).transformToMultiMatrix()
    val exp = loadDataset(expFilePath).transformToSingleMatrix()
    val ds3 = loadDataset(third).transformToSingleMatrix()

    var map1 = Map_ArrArr_NoCartesian(sc, exp, ds3, bin)
    val map = Map_ArrArr_Multimatrix(sc, ref, map1, bin)
    val count = map.transformToRow().count()
    println("Count = " + count)
//    map.transformToRow().saveAsTextFile(outputPath)


  }

  def benchmarkMapMap_NC(third: String): Unit = {
    conf.setAppName("ArrArr A MAP (B MAP C) NC-NC")
    sc = new SparkContext(conf)
    val ref = loadDataset(refFilePath).transformToSingleMatrix()
    val exp = loadDataset(expFilePath).transformToSingleMatrix()
    val ds3 = loadDataset(third).transformToSingleMatrix()

    val tmp = Map_ArrArr_NoCartesian(sc, exp, ds3, bin)
    var map = Map_ArrArr_NoCartesian(sc, ref, tmp, bin)
    val count = map.transformToRow().count()
    println("Count = " + count)
//    map.transformToRow().saveAsTextFile(outputPath)

  }

  /**
    * Complex query 1 =
    * SELECT ((DS1 MAP DS2) LEFT_JOIN DS3)
    * @param datasets
    */
  def benchmarkCurrentSystem_ComplexQuery_2(datasets: String*): Unit = {
    conf.setAppName("Current System - Complex 2")
    sc = new SparkContext(conf)
    val executor = new GMQLSparkExecutor(sc=sc, binSize = bS, outputFormat = GMQLSchemaFormat.TAB)
    server = new GmqlServer(executor)

    val ds1 = server READ(datasets(0) ) USING new CustomParser().setSchema(datasets(0))
    val ds2 = server READ(datasets(1) ) USING (new CustomParser().setSchema(datasets(1)))
    val ds3 = server READ(datasets(2) ) USING (new CustomParser().setSchema(datasets(2)))

    val ds12 = ds1.MAP(None, List(), ds2)
    val result = ds12.JOIN(None, List(new JoinQuadruple(distLess)), RegionBuilder.LEFT, ds3)
    val rdd = StoreMEMRD(executor, "", result.regionDag, result.metaDag, result.schema, sc)
    val count = rdd.count()
    println("Count = " + count)
//    server setOutputPath(outputPath) MATERIALIZE(result)
//    server.run()

  }

  /**
    * (DS1 MAP DS2) JOIN DS3 MAP DS4
    * @param datasets
    */
  def benchmarkCurrentSystem_ComplexQuery_3(datasets: String*): Unit = {
    conf.setAppName("Current System - Complex 3")
    sc = new SparkContext(conf)
    val executor = new GMQLSparkExecutor(sc=sc, binSize = bS, outputFormat = GMQLSchemaFormat.TAB)
    server = new GmqlServer(executor)

    val ds1 = server READ(datasets(0) ) USING new CustomParser().setSchema(datasets(0))
    val ds2 = server READ(datasets(1) ) USING (new CustomParser().setSchema(datasets(1)))
    val ds3 = server READ(datasets(2) ) USING (new CustomParser().setSchema(datasets(2)))
    val ds4 = server READ(datasets(3) ) USING (new CustomParser().setSchema(datasets(3)))

    val ds12 = ds1.MAP(None, List(), ds2)
    val ds123 = ds12.JOIN(None, List(new JoinQuadruple(distLess)), RegionBuilder.LEFT, ds3)
    val result = ds123.MAP(None, List(), ds4)

    val rdd = StoreMEMRD(executor, "", result.regionDag, result.metaDag, result.schema, sc)
    val count = rdd.count()
    println("Count = " + count)

//    server setOutputPath(outputPath) MATERIALIZE(result)
//    server.run()

  }


  /**
    * Complex query_4 = (DS1 MAP DS2) JOIN (DS3 MAP DS4) JOIN (DS5 MAP DS6)
    * @param datasets
    */
  def benchmarkCurrentSystem_ComplexQueries_4(datasets: String*): Unit = {
    conf.setAppName("Current System Complex Query 4")
    sc = new SparkContext(conf)
    val executor = new GMQLSparkExecutor(sc=sc, binSize = bS, outputFormat = GMQLSchemaFormat.TAB)
    server = new GmqlServer(executor)


    val ds1 = server READ(datasets(0) ) USING new CustomParser().setSchema(datasets(0))
    val ds2 = server READ(datasets(1) ) USING (new CustomParser().setSchema(datasets(1)))
    val ds3 = server READ(datasets(2) ) USING (new CustomParser().setSchema(datasets(2)))
    val ds4 = server READ(datasets(3) ) USING (new CustomParser().setSchema(datasets(3)))
    val ds5 = server READ(datasets(4) ) USING (new CustomParser().setSchema(datasets(4)))
    val ds6 = server READ(datasets(5) ) USING (new CustomParser().setSchema(datasets(5)))


    val ds12 = ds1.MAP(None, List(), ds2)
    val ds34 = ds3.MAP(None, List(), ds4)
    val ds56 = ds5.MAP(None, List(), ds6)

    val ds1234 = ds12.JOIN(None, List(new JoinQuadruple(distLess)), RegionBuilder.LEFT, ds34)
    val result = ds1234.JOIN(None, List(new JoinQuadruple(distLess)), RegionBuilder.LEFT, ds56)

    val rdd = StoreMEMRD(executor, "", result.regionDag, result.metaDag, result.schema, sc)
    val count = rdd.count()
    println("Count = " + count)

//    server setOutputPath(outputPath) MATERIALIZE(result)
//    server.run()

  }



  def benchmark_Single_Complex_2(datasets: String*): Unit = {
    conf.setAppName("Single - Complex 2")
    sc = new SparkContext(conf)

    val ds1 = loadDataset(datasets(0)).transformToSingleMatrix()
    val ds2 = loadDataset(datasets(1)).transformToSingleMatrix()
    val ds3 = loadDataset(datasets(2)).transformToSingleMatrix()
    val ds12 = Map_ArrArr_NoCartesian(sc, ds1, ds2, bin)
    val result = ArrArrJoin_NoCartesian(ds12, ds3, bin, RegionBuilder.LEFT, distLess, distGreater)

//    result.transformToRow().saveAsTextFile(outputPath)
    val count = result.transformToRow().count()
    println("Count = " + count)
  }

  def benchmark_Multi_Complex_2(datasets: String*): Unit = {
    conf.setAppName("Multi - Complex 2")
    sc = new SparkContext(conf)

    val ds1 = loadDataset(datasets(0)).transformToMultiMatrix()
    val ds2 = loadDataset(datasets(1)).transformToSingleMatrix()
    val ds3 = loadDataset(datasets(2)).transformToSingleMatrix()

    val ds12 = Map_ArrArr_Multimatrix(sc, ds1, ds2, bin)
    val result = ArrArrJoin_Multimatrix(ds12, ds3, bin, RegionBuilder.LEFT, distLess, distGreater)

//    result.transformToRow().saveAsTextFile(outputPath)
    val count = result.transformToRow().count()
    println("Count = " + count)
  }


  def benchmark_Single_Complex_3(datasets: String*): Unit = {
    conf.setAppName("Single - Complex 3")
    sc = new SparkContext(conf)

    val ds1 = loadDataset(datasets(0)).transformToSingleMatrix()
    val ds2 = loadDataset(datasets(1)).transformToSingleMatrix()
    val ds3 = loadDataset(datasets(2)).transformToSingleMatrix()
    val ds4 = loadDataset(datasets(3)).transformToSingleMatrix()

    val ds12 = Map_ArrArr_NoCartesian(sc, ds1, ds2, bin)
    val ds123 =  ArrArrJoin_NoCartesian(ds12, ds3, bin, RegionBuilder.LEFT, distLess, distGreater)
    val result = Map_ArrArr_NoCartesian(sc, ds123, ds4, bin)

//    result.transformToRow().saveAsTextFile(outputPath)
    val count = result.transformToRow().count()
    println("Count = " + count)
  }

  def benchmark_Multi_Complex_3(datasets: String*): Unit = {
    conf.setAppName("Multi - Complex 3")
    sc = new SparkContext(conf)

    val ds1 = loadDataset(datasets(0)).transformToMultiMatrix()
    val ds2 = loadDataset(datasets(1)).transformToSingleMatrix()
    val ds3 = loadDataset(datasets(2)).transformToSingleMatrix()
    val ds4 = loadDataset(datasets(3)).transformToSingleMatrix()

    val ds12 = Map_ArrArr_Multimatrix(sc, ds1, ds2, bin)
    val ds123 = ArrArrJoin_Multimatrix(ds12, ds3, bin, RegionBuilder.LEFT, distLess, distGreater)
    val result = Map_ArrArr_Multimatrix(sc, ds123, ds4, bin)

//    result.transformToRow().saveAsTextFile(outputPath)
    val count = result.transformToRow().count()
    println("Count = " + count)
  }


  def benchmark_Single_Complex_4(datasets: String*): Unit = {
    conf.setAppName("Single - Complex 4")
    sc = new SparkContext(conf)

    val ds1 = loadDataset(datasets(0)).transformToSingleMatrix()
    val ds2 = loadDataset(datasets(1)).transformToSingleMatrix()
    val ds3 = loadDataset(datasets(2)).transformToSingleMatrix()
    val ds4 = loadDataset(datasets(3)).transformToSingleMatrix()
    val ds5 = loadDataset(datasets(4)).transformToSingleMatrix()
    val ds6 = loadDataset(datasets(5)).transformToSingleMatrix()

    val ds12 = Map_ArrArr_NoCartesian(sc, ds1, ds2, bin)
    val ds34 = Map_ArrArr_NoCartesian(sc, ds3, ds4, bin)
    val ds56 = Map_ArrArr_NoCartesian(sc, ds5, ds6, bin)

    
    val ds1234 =  ArrArrJoin_NoCartesian(ds12, ds34, bin, RegionBuilder.LEFT, distLess, distGreater)
    val result = ArrArrJoin_NoCartesian(ds1234, ds56, bin, RegionBuilder.LEFT, distLess, distGreater)

//    result.transformToRow().saveAsTextFile(outputPath)
    val count = result.transformToRow().count()
    println("Count = " + count)
  }

  def benchmark_Multi_Complex_4(datasets: String*): Unit = {
    conf.setAppName("Multi - Complex 4")
    sc = new SparkContext(conf)

    val ds1 = loadDataset(datasets(0)).transformToMultiMatrix()
    val ds2 = loadDataset(datasets(1)).transformToSingleMatrix()
    val ds3 = loadDataset(datasets(2)).transformToSingleMatrix()
    val ds4 = loadDataset(datasets(3)).transformToSingleMatrix()
    val ds5 = loadDataset(datasets(4)).transformToSingleMatrix()
    val ds6 = loadDataset(datasets(5)).transformToSingleMatrix()

    val ds12 = Map_ArrArr_Multimatrix(sc, ds1, ds2, bin)
    val ds34 = Map_ArrArr_NoCartesian(sc, ds3, ds4, bin)
    val ds56 = Map_ArrArr_NoCartesian(sc, ds5, ds6, bin)

    val ds1234 = ArrArrJoin_Multimatrix(ds12, ds34, bin, RegionBuilder.LEFT, distLess, distGreater)
    val result =  ArrArrJoin_Multimatrix(ds1234, ds56, bin, RegionBuilder.LEFT, distLess, distGreater)

//    result.transformToRow().saveAsTextFile(outputPath)
    val count = result.transformToRow().count()
    println("Count = " + count)

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
  private def loadDataset(path: String): RDD[GRECORD] = {
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