package de.tub.dima.parquet

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder.RegionBuilder
import it.polimi.genomics.core.DataStructures.JoinParametersRD.{DistGreater, DistLess, RegionBuilder}
import org.apache.spark.rdd.RDD

object ArrArrJoin_NoCartesian {

  /**
    * Transform reference dataset from row-based to array based representation and then binning it.
    *
    * @param ref Reference dataset in row-based representation, obtained by reading directly from files.
    *            Each record in this dataset is of the form (id: Long, chr: Int, start: Long, stop: Long, strand: Short, features: Array[Double])
    * @param bin bin size
    * @param less     an Option for DistanceLessThan predicate
    * @param greater an Option for DistanceGreaterThan predicate
    * @return
    */
  private def transformRef
  (ref: RDD[(Long, Int, Long, Long, Short, Array[Double])], bin: Int, less: Option[DistLess], greater: Option[DistGreater])
  : RDD[((Int,Long),(Long,Long,Short,Array[Long],Array[Array[Double]]))] = {

    /*
    GroupBy records by coordinates (chr, start, stop, strand)
     */
    val refCompacted = ref.groupBy(x => (x._2, x._3, x._4, x._5))


    /*
    Binning ref dataset based on join predicate and bin size. There are 3 cases of join predicate:
    less only, greater only, less than and greater than
     */
    (less, greater) match {
      case (Some(DistLess(lDist)), Some(DistGreater(gDist))) => {
        null
      }
      case (Some(DistLess(lDist)),None) => {
        refCompacted.flatMap{
          x =>
            val startbin = ((x._1._2 - lDist).max(0))/bin
            val stopbin = (x._1._3 + lDist)/bin
            val features = x._2.map(_._6).toArray
            val ids = x._2.map(_._1).toArray
            /*
            yield: (chr,binNumber)(start, stop, strand, list_ids, list_features)
             */
            for (i <- startbin to stopbin)
              yield ((x._1._1, i), (x._1._2, x._1._3, x._1._4, ids, features))
        }

      }
      case (None, Some(DistGreater(gDist))) => {
        null
      }
      case (None, None) => null
    }


  }

  def apply(ref: RDD[((Int, Long, Long, Short), (Array[Long],Array[Array[Double]]))],
            exp: RDD[((Int, Long, Long, Short), (Array[Long],Array[Array[Double]]))],
            bin: Int,
            joinType: RegionBuilder,
            less: Option[DistLess],
            greater: Option[DistGreater]): RDD[((Int, Long, Long, Short), (Array[Long], Array[Array[Double]]))] = {


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

  private def execute(ref: RDD[((Int,Long),(Long,Long,Short,Array[Long],Array[Array[Double]]))],
                      exp: RDD[((Int,Long),(Long,Long,Short,Array[Long],Array[Array[Double]]))],
                      binSize: Int,
                      joinType: RegionBuilder,
                      less: DistLess,
                      greater: DistGreater): RDD[((Int, Long, Long, Short), (Array[Long], Array[Array[Double]]))] = {


    val RefJoinedExp //: RDD[((Int, Long, Long, Short),(Array[Long], Array[Array[Double]]))]
    = ref.cogroup(exp)
      .flatMap{
        x =>
          val r: Iterable[(Long, Long, Short, Array[Long], Array[Array[Double]])] = x._2._1
          val e: Iterable[(Long, Long, Short, Array[Long], Array[Array[Double]])] = x._2._2
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

                    val ids = (refRecord._4, expRecord._4)

                    val features = (refRecord._5, expRecord._5)
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
                ((l._1._1, l._1._2 ++ r._1._2), (l._2._1, (l._2._2 ++ r._2._2)))
            )
        .mapValues {
          x =>
            val ids = for (refId <- x._1._1;
                           expId <- x._1._2
                       ) yield Hashing.md5().newHasher().putLong(refId).putLong(expId).hash().asLong

            val features = for (refFeature <- x._2._1;
                                expFeature <- x._2._2)
              yield refFeature ++ expFeature
            (ids, features)
        }
    reduced
  }
}
