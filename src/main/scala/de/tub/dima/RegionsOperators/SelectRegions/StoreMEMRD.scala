package de.tub.dima.RegionsOperators.SelectRegions

import de.tub.dima.MGD_GMQLSparkExecutor
import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core.exception.SelectFormatException
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
  * Created by abdulrahman kaitoua on 25/05/15.
  */
object StoreMEMRD {
  private final val logger = LoggerFactory.getLogger(StoreMEMRD.getClass);
  private final val ENCODING = "UTF-8"

  @throws[SelectFormatException]
  def apply(executor: MGD_GMQLSparkExecutor, path: String, value: RegionOperator, associatedMeta:MetaOperator, schema : List[(String, PARSING_TYPE)], sc: SparkContext): RDD[GRECORD] = {
     executor.implement_rd(value, sc)
  }
}
