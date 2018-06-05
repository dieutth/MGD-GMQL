package de.tub.dima.operators.map

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures.JoinParametersRD.{DistGreater, DistLess, RegionBuilder}
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder.RegionBuilder
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object Map_ArrArr_Multimatrix {
  def apply(
            sc: SparkContext,
            ref: RDD[((Int, Long, Long, Short), (Array[Array[Long]],Array[Array[Array[Double]]]))],
            exp: RDD[((Int, Long, Long, Short), (Array[Long],Array[Array[Double]]))],
            bin: Int
            ): RDD[((Int, Long, Long, Short), (Array[Array[Long]], Array[Array[Array[Double]]]))] = {

    val allExpIds = sc.broadcast(exp.flatMap(_._2._1).distinct().collect())
    val binnedRef =
        ref.flatMap{
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


    execute(binnedRef, binnedExp, bin, allExpIds)

  }

  private def execute(ref: RDD[((Int,Long),(Long,Long,Short,Array[Array[Long]],Array[Array[Array[Double]]]))],
                      exp: RDD[((Int,Long),(Long,Long,Short,Array[Long],Array[Array[Double]]))],
                      binSize: Int,
                      allExpIds: Broadcast[Array[Long]]
                      ): RDD[((Int, Long, Long, Short), (Array[Array[Long]], Array[Array[Array[Double]]]))] = {


    val RefJoinedExp //: RDD[((Int, Long, Long, Short),(Array[Long], Array[Array[Double]]))]
    = ref.cogroup(exp)
      .flatMap{
        x =>
          val r: Iterable[(Long, Long, Short, Array[Array[Long]], Array[Array[Array[Double]]])] = x._2._1
          val e: Iterable[(Long, Long, Short, Array[Long], Array[Array[Double]])] = x._2._2
          val res =
            r.flatMap{refRecord =>
              val key = (x._1._1, refRecord._1, refRecord._2, refRecord._3)
              if(e.isEmpty){
                val ids = refRecord._4 :+ Array[Long]()
                Some((key, (ids, refRecord._5, Array[Int]())))
              }else{
                e.flatMap {
                  expRecord =>
                    //the start-stop of the expanding region coordinate
                    if (
                    //region overlap
                      (refRecord._1 < expRecord._2 && expRecord._1 < refRecord._2)
                        //strand equal or at least one of the strand is non-determine
                        && (refRecord._3.equals('*') || expRecord._3.equals('*') || refRecord._3.equals(expRecord._3))
                        && (x._1._2 == refRecord._1 / binSize || x._1._2 == expRecord._1 / binSize)
                    ) {

                      val ids = refRecord._4 :+ expRecord._4
                      Some((key, (ids, refRecord._5, Array.fill(1)(expRecord._4.size))))

                    }
                    else {
                      val ids = refRecord._4 :+ Array[Long]()
                      Some((key, (ids, refRecord._5, Array[Int]())))

                    }
                }
              }
            }
          res
      }

    val reduced = RefJoinedExp
      .reduceByKey {
        (l, r) =>
          if (l._3.isEmpty)
            r
          else if (r._3.isEmpty)
            l
          else{

            val expIdsCountLeft = l._1.last zip l._3
            val expIdsCountRight = r._1.last zip r._3
            val features = l._2
            val (expIds, counts) = (expIdsCountLeft ++ expIdsCountRight groupBy (x => x._1) mapValues (_.map(_._2).sum)).toArray.unzip
            l._1(l._1.size-1) = expIds

//            if (l._1.head.size == 1){
//              val headId = l._1.head.head
//              val second = for (id <- l._1(1))
//                yield Hashing.md5().newHasher().putLong(headId).putLong(id).hash().asLong
//              l._1(1) = second
//              (l._1.slice(1, l._1.size), l._2, counts)
//            }else
              (l._1, l._2, counts)
          }
      }

      .mapValues {
        x =>
          val allExpIdsWithCount = allExpIds.value zip Array.fill(allExpIds.value.length)(0)
          val (expIds, counts) = (allExpIdsWithCount ++ (x._1.last zip x._3) groupBy(x=>x._1) mapValues(_.map(_._2).sum)).toArray.unzip

          x._1(x._1.length-1) = expIds
          val features = x._2 :+ counts.map(x => Array(x.toDouble))//Array[Array[Double]](counts.map(_.toDouble))

          (x._1, features)
      }
    reduced

  }

}