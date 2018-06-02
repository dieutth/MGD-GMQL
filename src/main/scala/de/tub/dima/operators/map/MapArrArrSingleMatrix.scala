//package de.tub.dima.parquet
//
//import com.google.common.hash.Hashing
//import org.apache.spark.SparkContext
//import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.rdd.RDD
//
//object MapArrArrSingleMatrix {
//  def apply(
//             sc: SparkContext,
//             ref: RDD[((Int, Long, Long, Short), (Array[Long],Array[Array[Double]]))],
//             exp: RDD[((Int, Long, Long, Short), (Array[Long],Array[Array[Double]]))],
//             bin: Int
//           ): RDD[((Int, Long, Long, Short), (Array[Long], Array[Array[Double]]))] = {
//
//    val allExpIds = exp.flatMap(_._2._1).distinct()
//    val allRefIds = ref.flatMap(_._2._1).distinct()
//    val allRefExpIds = sc.broadcast(allRefIds.cartesian(allExpIds).map(x => Hashing.md5().newHasher().putLong(x._1).putLong(x._2).hash().asLong).collect())
//
//
//    val binnedRef = ref.flatMap{
//      x =>
//        val startbin = x._1._2/bin
//        val stopbin = x._1._3/bin
//        // yield: (chr,binNumber)(start, stop, strand, list_ids, list_features)
//        for (i <- startbin to stopbin)
//          yield ((x._1._1, i), (x._1._2, x._1._3, x._1._4, x._2._1, x._2._2))
//    }
//
//    val binnedExp = exp.flatMap{
//      x =>
//        val startbin = x._1._2/bin
//        val stopbin = x._1._3/bin
//        // yield: (chr,binNumber)(start, stop, strand, list_ids, list_features)
//        for (i <- startbin to stopbin)
//          yield ((x._1._1, i), (x._1._2, x._1._3, x._1._4, x._2._1, x._2._2))
//    }
//
//    execute(binnedRef, binnedExp, bin, allRefExpIds)
//  }
//
//  private def execute(ref: RDD[((Int,Long),(Long,Long,Short,Array[Long], Array[Array[Double]]))],
//                      exp: RDD[((Int,Long),(Long,Long,Short,Array[Long], Array[Array[Double]]))],
//                      bin: Int,
//                      allRefExpIds: Broadcast[Array[Long]]):
//  RDD[((Int, Long, Long, Short), (Array[Long], Array[Array[Double]]))] = {
//
//    val RefJoinedExp = ref.cogroup(exp)
//      .flatMap {
//        x =>
//          val r: Iterable[(Long, Long, Short, Array[Long], Array[Array[Double]])] = x._2._1
//          val e: Iterable[(Long, Long, Short, Array[Long], Array[Array[Double]])] = x._2._2
//          val res =
//            r.flatMap { refRecord =>
//              val key = (x._1._1, refRecord._1, refRecord._2, refRecord._3)
//              if (e.isEmpty) {
//                None
//              }
//              else {
//                e.flatMap {
//                  expRecord =>
//                    if (
//                    //region intersect
//                      (refRecord._1 < expRecord._2 && expRecord._1 < refRecord._2)
//                        //strand equal or at least one of the strand is non-determine
//                        && (refRecord._3.equals('*') || expRecord._3.equals('*') || refRecord._3.equals(expRecord._3))
//                        //produce result only when bin contains the start of either REF region or EXP region
//                        && (x._1._2 == refRecord._1 / bin || x._1._2 == expRecord._1 / bin)
//                    ) {
//                      val ids: Array[Long] =
//                        for (refId <- refRecord._4;
//                             expId <- expRecord._4)
//                          yield Hashing.md5().newHasher().putLong(refId).putLong(expId).hash().asLong
//
//                      val features = for (f <- refRecord._5;
//                                          i <- 1 to expRecord._4.length)
//                                      yield f
//                      Some((key, (ids, features, Array.fill(features.length)(1))))
//                    }
//                    else {
//                      None
//                    }
//                }
//              }
//            }
//
//          res
//      }
//
//    val reduced = RefJoinedExp
//      .reduceByKey{
//      (x, y) =>
//        val ids_features = (x._1 ++ y._1) zip (x._2 ++ y._2) zip (x._3 ++ y._3)
//        val tmp = ids_features groupBy(_._1)
//        val counts = tmp mapValues (_.map(_._2).sum)
//        val t = counts.toArray.unzip
//        val idF = t._1.unzip
//        (idF._1, idF._2, t._2)
//
//    }
//      .mapValues {
//        x =>
//          val featureWithCounts = for (refFeature <- x._2;
//                                       expFeature <- x._3)
//                                          yield refFeature :+ expFeature.toDouble
//
//          val allRefExpIdsWithCount = allRefExpIds.value zip Array.fill(allRefExpIds.value.length)(0)
////          val (ids, counts) = (allRefExpIdsWithCount ++ (x._1) groupBy(x=>x._1) mapValues(_.map(_._2).sum)).toSeq.unzip
////
////
////          (ids, features)
//          null
//
//      }
//
//    reduced
//  }
//
//}
