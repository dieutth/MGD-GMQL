package de.tub.dima.parquet

import com.google.common.hash.Hashing
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/*
  @author: dieutth
 */

object ArrArrGMap {

  def apply(ref: RDD[((Int,Long),(Long,Long,Short,Array[Long]))],
            exp: RDD[((Int,Long),(Long,Long,Short,Array[Long]))],
            bin: Int):
//    Unit={
     RDD[((Int, Long,Long,Short),Array[(Long, Int)])] = {

    val RefJoinedExp = ref.cogroup(exp)
      .flatMap {
        x =>
          val r: Iterable[(Long, Long, Short, Array[Long])] = x._2._1
          val e: Iterable[(Long, Long, Short, Array[Long])] = x._2._2
          val res =
            r.flatMap { refRecord =>

              //a new key of the tuple will contains only = start, stop, chr, strand
              val key = (x._1._1, refRecord._1, refRecord._2, refRecord._3)
              if (e.nonEmpty) {
                e.map {
                  expRecord =>
                    val xs = ListBuffer[Long]()
                    for (refId <- refRecord._4)
                      for (expId <- expRecord._4)
                        xs += Hashing.md5().newHasher().putLong(refId).putLong(expId).hash().asLong
                    val ids = xs.toArray

                    if (
                       //region intersect
                      (refRecord._1 < expRecord._2 && expRecord._1 < refRecord._2)
                        //strand equal or at least one of the strand is non-determine
                        && (refRecord._3.equals('*') || expRecord._3.equals('*') || refRecord._3.equals(expRecord._3))
                        //produce result only when bin contains the start of either REF region or EXP region
                        && (x._1._2 == refRecord._1 / bin || x._1._2 == expRecord._1 / bin)
                    ) {

                      val ids_counts = ids zip Array.fill(ids.length)(1)
                      (key, ids_counts)
                    }
                    else {
                      val ids_counts = ids zip Array.fill(ids.length)(0)
                      (key, ids_counts)

                    }
                }
              }
              else {
                Array((key, Array[(Long, Int)]()))
              }
            }

          res
      }

    val reduced = RefJoinedExp
              .reduceByKey{
                (l, r) =>
                  val res = l ++ r groupBy(_._1) mapValues (_.map(_._2).sum)
                  res.toArray
              }

    reduced
  }
}
