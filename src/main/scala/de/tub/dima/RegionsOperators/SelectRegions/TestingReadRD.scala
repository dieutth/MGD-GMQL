package de.tub.dima.RegionsOperators.SelectRegions

import de.tub.dima.loaders.Loaders._
import it.polimi.genomics.core.DataTypes.{GRECORD, MetaType}
import it.polimi.genomics.core.GMQLLoader
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
  * Created by pietro on 01/10/15.
  */
object TestingReadRD {

  private final val logger = LoggerFactory.getLogger(TestingReadRD.getClass);

  def apply(path: List[String], loader: GMQLLoader[Any, Any, Any, Any], sc: SparkContext): RDD[GRECORD] = {
    def parser(x: (Long, String)) = loader.asInstanceOf[GMQLLoader[(Long, String), Option[GRECORD], (Long, String), Option[MetaType]]].region_parser(x)

    val files = path.flatMap { dirInput =>
      if (new java.io.File(dirInput).isDirectory)
        new java.io.File(dirInput).listFiles.filter(x => !(x.getName.endsWith(".meta") || x.isDirectory)).map(x => x.getPath)
      else List(dirInput)
    }
    sc.forPath(files.mkString(",")).LoadRegionsCombineFiles(parser)

  }
}
