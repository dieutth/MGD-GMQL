package de.tub.dima.opGvalue.join

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder.RegionBuilder
import it.polimi.genomics.core.DataStructures.JoinParametersRD.{DistGreater, DistLess, RegionBuilder}
import it.polimi.genomics.core.GValue
import org.apache.spark.rdd.RDD

/**
  * @author dieutth 06/06/2018
  *
  * Perform Join using single-matrix based representation for both REF and EXP dataset.
  *
  */
object ArrArrJoin {

   def apply(ref: RDD[((String, Long, Long, Short), (Array[Long],Array[Array[GValue]]))],
                  exp: RDD[((String, Long, Long, Short), (Array[Long],Array[Array[GValue]]))],
                  bin: Int,
                  joinType: RegionBuilder,
                  less: Option[DistLess],
                  greater: Option[DistGreater]): RDD[((String, Long, Long, Short), (Array[Long], Array[Array[GValue]]))] = {

    val binnedRef = (less, greater) match {
      case (Some(DistLess(lDist)), Some(DistGreater(gDist))) => {
        null
      }
      case (Some(DistLess(lDist)),None) => {
          ref.flatMap{
          x =>
            val startbin = ((x._1._2 - lDist).max(0))/bin
            val stopbin = (x._1._3 + lDist)/bin
           // yield: (chr,binNumber)(start, stop, strand, list_ids, list_features)
            for (i <- startbin to stopbin)
              yield ((x._1._1, i), (x._1._2, x._1._3, x._1._4, x._2._1, x._2._2))
        }
      }
      case (None, Some(DistGreater(gDist))) => {
        null
      }
      case (None, None) => null
    }

    val binnedExp = (less, greater) match {
      case (Some(DistLess(lDist)), Some(DistGreater(gDist))) => {
        null
      }
      case (Some(DistLess(lDist)),None) => {
        exp.flatMap{
          x =>
            val startbin = x._1._2/bin
            val stopbin = x._1._3/bin
            // yield: (chr,binNumber)(start, stop, strand, list_ids, list_features)
            for (i <- startbin to stopbin)
              yield ((x._1._1, i), (x._1._2, x._1._3, x._1._4, x._2._1, x._2._2))
        }
      }
      case (None, Some(DistGreater(gDist))) => {
        null
      }
      case (None, None) => null
    }

    execute(binnedRef, binnedExp, bin, joinType, less.get, null)

  }

  private def execute(ref: RDD[((String,Long),(Long,Long,Short,Array[Long],Array[Array[GValue]]))],
            exp: RDD[((String,Long),(Long,Long,Short,Array[Long],Array[Array[GValue]]))],
            binSize: Int,
            joinType: RegionBuilder,
            less: DistLess,
            greater: DistGreater): RDD[((String, Long, Long, Short), (Array[Long], Array[Array[GValue]]))] = {


    val RefJoinedExp: RDD[((String, Long, Long, Short),(Array[Long], Array[Array[GValue]]))] = ref.cogroup(exp)
      .flatMap{
        x =>
          val r: Iterable[(Long, Long, Short, Array[Long], Array[Array[GValue]])] = x._2._1
          val e: Iterable[(Long, Long, Short, Array[Long], Array[Array[GValue]])] = x._2._2
          val res =
            r.flatMap{refRecord =>
              e.flatMap{
                expRecord =>
                  //the start-stop of the expanding region coordinate
                  val refL: Long = (refRecord._1-less.limit).max(0)
                  val refR: Long = refRecord._2 + less.limit
                  if (
                      //region overlap
                      (refL < expRecord._2 && expRecord._1 < refR)
                      //strand equal or at least one of the strand is non-determine
                      &&  (refRecord._3.equals('*') || expRecord._3.equals('*') || refRecord._3.equals(expRecord._3))
                      && (x._1._2 == refL/binSize || x._1._2 == expRecord._1/binSize)
                  ){

                    /*key = chr, start, stop, strand
                    * value of key depends on the joinType: Left, Right, or Contig
                    * */
                    val key = joinType match {
                      case RegionBuilder.LEFT => (x._1._1, refRecord._1, refRecord._2, refRecord._3)
                      case RegionBuilder.RIGHT => (x._1._1, expRecord._1, expRecord._2, expRecord._3)
                      case RegionBuilder.CONTIG => (x._1._1, refRecord._1.min(expRecord._1), refRecord._2.max(expRecord._2), refRecord._3)
                    }

                    val ids: Array[Long] =
                    for (refId <- refRecord._4;
                         expId <- expRecord._4)
                            yield Hashing.md5().newHasher().putLong(refId).putLong(expId).hash().asLong

                    //concat value of features from refRecord and expRecord
                    val features = for (refFeature <- refRecord._5;
                                        expFeature <- expRecord._5)
                                          yield refFeature ++ expFeature
                    Some((key, (ids, features)))
                  }
                  else{
                    None

                  }
              }
            }
          res
      }

    val reduced = RefJoinedExp
      .reduceByKey(
        (l, r) =>
          (l._1 ++ r._1, l._2 ++ r._2)
      )
    reduced
  }
}
