package de.tub.dima.RegionsOperators.SelectRegions

import de.tub.dima.MGD_GMQLSparkExecutor
import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core.GMQLSchemaCoordinateSystem
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core.exception.SelectFormatException
import de.tub.dima.MGD_GMQLSparkExecutor
import de.tub.dima.loaders.writeMultiOutputFiles
import de.tub.dima.loaders.writeMultiOutputFiles.RDDMultipleTextOutputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.Map

/**
  * Created by abdulrahman kaitoua on 25/05/15.
  */
object StoreTABRD {
  private final val logger = LoggerFactory.getLogger(StoreTABRD.getClass);
  private final val ENCODING = "UTF-8"

  @throws[SelectFormatException]
  def apply(executor: MGD_GMQLSparkExecutor, path: String, value: RegionOperator, associatedMeta:MetaOperator, schema : List[(String, PARSING_TYPE)], coordinateSystem: GMQLSchemaCoordinateSystem.Value, sc: SparkContext): RDD[GRECORD] = {
    val regions = executor.implement_rd(value, sc)
//    val meta = executor.implement_md(associatedMeta,sc)

//    val conf = new Configuration();
//    val dfsPath = new org.apache.hadoop.fs.Path(path);
//    val fs = FileSystem.get(dfsPath.toUri(), conf);
//
//    val MetaOutputPath = path + "/meta/"
    val RegionOutputPath = path + "/exp/"

//    logger.debug(MetaOutputPath)
//    logger.debug(RegionOutputPath)
//    logger.debug(regions.toDebugString)
//    logger.debug(meta.toDebugString)
//
//
//    val outSample = "S"
//
//    val Ids = meta.keys.distinct()
//    val newIDS: Map[Long, Long] = Ids.zipWithIndex().collectAsMap()
//    val newIDSbroad = sc.broadcast(newIDS)
//
//    val regionsPartitioner = new HashPartitioner(Ids.count.toInt)
//
//    val keyedRDD =
//      regions.sortBy(s=>s._1).map{x =>
//        val newStart = if (coordinateSystem == GMQLSchemaCoordinateSystem.OneBased) (x._1._3 + 1) else x._1._3  //start: 0-based -> 1-based
//        (outSample+"_"+ "%05d".format(newIDSbroad.value.get(x._1._1).getOrElse(x._1._1))+".gdm",
//        x._1._2 + "\t" + newStart + "\t" + x._1._4 + "\t" + x._1._5 + "\t" + x._2.mkString("\t"))}
//          .partitionBy(regionsPartitioner)//.mapPartitions(x=>x.toList.sortBy{s=> val data = s._2.split("\t"); (data(0),data(1).toLong,data(2).toLong)}.iterator)
//
//    keyedRDD.saveAsHadoopFile(RegionOutputPath,classOf[String],classOf[String],classOf[RDDMultipleTextOutputFormat])
////    writeMultiOutputFiles.saveAsMultipleTextFiles(keyedRDD, RegionOutputPath)
//
//    val metaKeyValue = meta.sortBy(x=>(x._1,x._2)).map(x => (outSample+"_"+ "%05d".format(newIDSbroad.value.get(x._1).get) + ".gdm.meta", x._2._1 + "\t" + x._2._2)).partitionBy(regionsPartitioner)
//
////    writeMultiOutputFiles.saveAsMultipleTextFiles(metaKeyValue, MetaOutputPath)
//    metaKeyValue.saveAsHadoopFile(MetaOutputPath,classOf[String],classOf[String],classOf[RDDMultipleTextOutputFormat])
//    writeMultiOutputFiles.fixOutputMetaLocation(MetaOutputPath)
//
//    fs.listStatus(new Path(RegionOutputPath), new PathFilter {
//      override def accept(path: Path): Boolean = {println(path.getName); true}
//    })
//
////    fs.deleteOnExit(new Path(RegionOutputPath+"*.crc"))
//    fs.deleteOnExit(new Path(RegionOutputPath+"_SUCCESS"))

    regions.saveAsTextFile(RegionOutputPath)
    regions
  }
}
