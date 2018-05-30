package de.tub.dima.parquet

import com.google.common.hash.Hashing
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/*
  @author: dieutth
 */

object ArrArrGMap {

  def apply(
             ref: RDD[((Int, Long, Long, Short), (Array[Long],Array[Array[Double]]))],
             exp: RDD[((Int, Long, Long, Short), (Array[Long],Array[Array[Double]]))],
             bin: Int
           ): RDD[((Int, Long, Long, Short), (Array[Long], Array[Array[Double]]))] = {

    val binnedRef = ref.flatMap{
      x =>
        val startbin = x._1._2/bin
        val stopbin = x._1._3/bin
        // yield: (chr,binNumber)(start, stop, strand, list_ids, list_features)
        for (i <- startbin to stopbin)
          yield ((x._1._1, i), (x._1._2, x._1._3, x._1._4, x._2._1, x._2._2))
    }

    val binnedExp = exp.flatMap{
      x =>
        val startbin = x._1._2/bin
        val stopbin = x._1._3/bin
        // yield: (chr,binNumber)(start, stop, strand, list_ids, list_features)
        for (i <- startbin to stopbin)
          yield ((x._1._1, i), (x._1._2, x._1._3, x._1._4, x._2._1, x._2._2))
    }

    execute(binnedRef, binnedExp, bin)
  }

  private def execute(ref: RDD[((Int,Long),(Long,Long,Short,Array[Long], Array[Array[Double]]))],
                      exp: RDD[((Int,Long),(Long,Long,Short,Array[Long], Array[Array[Double]]))],
                      bin: Int):
  RDD[((Int, Long, Long, Short), (Array[Long], Array[Array[Double]]))] = {

    val RefJoinedExp = ref.cogroup(exp)
      .flatMap {
        x =>
          val r: Iterable[(Long, Long, Short, Array[Long], Array[Array[Double]])] = x._2._1
          val e: Iterable[(Long, Long, Short, Array[Long], Array[Array[Double]])] = x._2._2
          val res =
            r.flatMap { refRecord =>
              val key = (x._1._1, refRecord._1, refRecord._2, refRecord._3)
              e.flatMap {
                expRecord =>
                  if (
                  //region intersect
                    (refRecord._1 < expRecord._2 && expRecord._1 < refRecord._2)
                      //strand equal or at least one of the strand is non-determine
                      && (refRecord._3.equals('*') || expRecord._3.equals('*') || refRecord._3.equals(expRecord._3))
                      //produce result only when bin contains the start of either REF region or EXP region
                      && (x._1._2 == refRecord._1 / bin || x._1._2 == expRecord._1 / bin)
                  ) {
                    val ids = (refRecord._4, expRecord._4)
//                    Some((key, (ids, refRecord._5, Array[Int]())))
                    Some((key, (ids, refRecord._5, Array.fill(expRecord._4.length)(1))))
                  }
                  else {
                    None
                  }

              }
            }


          res
      }

    val reduced = RefJoinedExp
      .reduceByKey{
        (l, r) =>
          val refIds = l._1._1
          val expIdsCountLeft = l._1._2 zip l._3
          val expIdsCountRight = r._1._2 zip r._3
          val features = l._2
          val (expIds, counts) = (expIdsCountLeft ++ expIdsCountRight groupBy(x=>x._1) mapValues(_.map(_._2).sum)).toSeq.unzip
          ((refIds, expIds.toArray), features, counts.toArray)
      }
      .mapValues {
        x =>
          val ids = for (refId <- x._1._1;
                         expId <- x._1._2
          ) yield Hashing.md5().newHasher().putLong(refId).putLong(expId).hash().asLong

          val features = for (refFeature <- x._2;
                              expFeature <- x._3)
            yield refFeature :+ expFeature.toDouble
          (ids, features)
      }

    reduced
  }
}
