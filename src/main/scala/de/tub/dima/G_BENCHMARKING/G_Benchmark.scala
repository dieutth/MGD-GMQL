package de.tub.dima.G_BENCHMARKING

import com.google.common.hash.Hashing
import de.tub.dima.opGvalue.join.{ArrArrJoin, ArrArrJoin_Multimatrix, ArrArrJoin_NoCartesian}
import de.tub.dima.opGvalue.map.{Map_ArrArr_Multimatrix, Map_ArrArr_NoCartesian}
import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.DataStructures.JoinParametersRD.{DistLess, JoinQuadruple, RegionBuilder}
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core._
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.loaders.{CustomParser, Loaders}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author dieutth, 06/06/2018
  */

object G_Benchmark{
    def apply(conf: SparkConf, refFilePath: String, expFilePath: String,
              outputPath: String) = new G_Benchmark(conf, refFilePath, expFilePath, outputPath)
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
    server = new GmqlServer(new GMQLSparkExecutor(sc=sc, binSize = bS, outputFormat = GMQLSchemaFormat.TAB))
    val startTime = System.currentTimeMillis()
    val ds1 = server READ(refFilePath ) USING (new CustomParser().setSchema(refFilePath))
    val ds2 = server READ(expFilePath ) USING (new CustomParser().setSchema(expFilePath))

    var map = ds1.MAP(None,List(),ds2)
    for (i <- Range(1, loop))
      map = map.MAP(None,List(),ds1)
    server setOutputPath(outputPath) MATERIALIZE(map)
    server.run()
    println ("Execution time of current system map: " + (System.currentTimeMillis() - startTime)/1000 + " loop = " + loop)

  }

  /**
    * Measure execution time of a JOIN operation using current system GMQL-Spark
    * @param loop
    */
  def benchmarkCurrentSystem_Join(loop: Int): Unit = {
    conf.setAppName("Current System Join")
    sc = new SparkContext(conf)
    server = new GmqlServer(new GMQLSparkExecutor(sc=sc, binSize = bS, outputFormat = GMQLSchemaFormat.TAB))
    val startTime = System.currentTimeMillis()
    val ds1 = server READ(refFilePath ) USING (new CustomParser().setSchema(refFilePath))
    val ds2 = server READ(expFilePath ) USING (new CustomParser().setSchema(expFilePath))

    var join = ds1.JOIN(None, List(new JoinQuadruple(distLess)), RegionBuilder.LEFT, ds2)

    for (i <- Range(1, loop))
      join = ds2.JOIN(None, List(new JoinQuadruple(distLess)), RegionBuilder.LEFT, join)

    server setOutputPath(outputPath) MATERIALIZE(join)
    server.run()
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

    join.transformToRow().saveAsTextFile(outputPath)
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

    join.transformToRow().saveAsTextFile(outputPath)
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

    join.transformToRow().saveAsTextFile(outputPath)
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

    map.transformToRow().saveAsTextFile(outputPath)
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

    map.transformToRow().saveAsTextFile(outputPath)
    println("Execution time for Map MultiMatrix: " + (System.currentTimeMillis() - startTime) / 1000)
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