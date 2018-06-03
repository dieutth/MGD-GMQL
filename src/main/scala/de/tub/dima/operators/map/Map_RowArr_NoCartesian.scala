package de.tub.dima.operators.map

import com.google.common.hash.Hashing
import it.polimi.genomics.core.{GDouble, GRecordKey, GValue}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/*
  @author: dieutth
 */

object Map_RowArr_NoCartesian {

  def apply(
             sc: SparkContext,
             ref: RDD[(GRecordKey, Array[GValue])],
             exp: RDD[((Int, Long, Long, Short), (Array[Long],Array[Array[Double]]))],
             bin: Int
           ):
            RDD[((Int, Long, Long, Short), (Array[Long], Array[Array[Double]]))] = {
//    RDD[(GRecordKey, Array[GValue])] ={
    val allExpIds = sc.broadcast(exp.flatMap(_._2._1).distinct().collect())
    val binnedRef = ref.flatMap{
      x =>
        val y = x._1.chrom.substring(3)
        val chr: Int =
          try {
              y.toInt
          } catch {
            case e: Exception => if (y == "X") 23 else 24
          }
        val features = x._2.flatMap(
          _ match {
            case GDouble(t) => Some(t)
            case _ => None
          })


        val startbin = (x._1.start/bin).toInt
        val stopbin = (x._1.stop/bin).toInt

        // yield: (chr,binNumber)(start, stop, strand, ID, features)
        for (i <- startbin to stopbin)
          yield ((chr, i), (x._1.start, x._1.stop, x._1.strand.toShort, x._1.id, features))
    }

    val binnedExp = exp.flatMap{
      x =>
        val startbin = (x._1._2/bin).toInt
        val stopbin = (x._1._3/bin).toInt
        // yield: (chr,binNumber)(start, stop, strand, list_ids, list_features)
        for (i <- startbin to stopbin)
          yield ((x._1._1, i), (x._1._2, x._1._3, x._1._4, x._2._1, x._2._2))
    }

    execute(binnedRef, binnedExp, bin, allExpIds)
  }

  private def execute(ref: RDD[((Int,Int),(Long,Long,Short,Long, Array[Double]))],
                      exp: RDD[((Int,Int),(Long,Long,Short,Array[Long], Array[Array[Double]]))],
                      bin: Int,
                      allExpIds: Broadcast[Array[Long]]):
  RDD[((Int, Long, Long, Short), (Array[Long], Array[Array[Double]]))] = {
//  RDD[(GRecordKey, Array[GValue])] ={

    val RefJoinedExp = ref.cogroup(exp)
      .flatMap {
        x =>
          val r: Iterable[(Long, Long, Short, Long, Array[Double])] = x._2._1
          val e: Iterable[(Long, Long, Short, Array[Long], Array[Array[Double]])] = x._2._2
          val res =
            r.flatMap { refRecord =>
              //chr, start, stop, strand
              val key =  (x._1._1, refRecord._1, refRecord._2, refRecord._3)
              if (e.isEmpty) {
//                val refId = refRecord._4
                val ids = (Array(refRecord._4), Array[Long]())
                Some((key, (ids, Array(refRecord._5), Array[Int]())))
              }
              else {
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
                      val ids = (Array(refRecord._4), expRecord._4)
//                      val ids = expRecord._4
                      //                    Some((key, (ids, refRecord._5, Array[Int]())))
                      Some((key, (ids, Array(refRecord._5), Array.fill(expRecord._4.length)(1))))
                    }
                    else {
//                      val ids = Array[Long]()
                      val ids = (Array(refRecord._4), Array[Long]())
                      Some((key, (ids, Array(refRecord._5), Array[Int]())))
                    }
                }
              }
            }

          res
      }

//    val reduced = RefJoinedExp
//      .reduceByKey{
//        (l, r) =>
//          if (l._2.isEmpty)
//            r
//          else if (r._2.isEmpty)
//            l
//          else {
//            val expIdsCountLeft = l._1 zip l._2
//            val expIdsCountRight = r._1 zip r._2
////            val (expIds, counts) = (expIdsCountLeft ++ expIdsCountRight groupBy (x => x._1) mapValues (_.map(_._2).sum)).toArray.unzip
//            val (expIds, counts) = (expIdsCountLeft ++ expIdsCountRight groupBy (x => x._1) mapValues (_.map(_._2).sum)).toArray.unzip
//
//            (expIds, counts)
//          }
//      }

    val reduced = RefJoinedExp
      .reduceByKey{
        (l, r) =>
//          if (l._3.isEmpty && r._3.isEmpty){
//
//            val left = for (i <- 0 until l._2.length)
//                          yield Rep(l._1._1(i), l._2(i))
//            val right = for (i <- 0 until r._2.length)
//                            yield Rep(r._1._1(i), r._2(i))
//
//            val (refIds, features) = (left ++ right).distinct.toArray.map(x => (x.id, x.feature)).unzip
//            ((refIds, l._1._2), features, l._3)
//          }
//          else
          {

            val left = for (i <- 0 until l._2.length)
              yield Rep(l._1._1(i), l._2(i))
            val right = for (i <- 0 until r._2.length)
              yield Rep(r._1._1(i), r._2(i))
            val lr = (left ++ right)
            val lr_distinct = lr.distinct
//            val repeated = lr.size - lr_distinct.size
            val (refIds, features) = lr_distinct.toArray.map(x => (x.id, x.feature)).unzip
            val repeated = refIds.size - refIds.distinct.size
            val expIdsCountLeft = l._1._2 zip l._3
            val expIdsCountRight = r._1._2 zip r._3

            val (expIds, counts) = (expIdsCountLeft ++ expIdsCountRight groupBy (x => x._1) mapValues (_.map(_._2).sum - repeated)).toSeq.unzip
            ((refIds, expIds.toArray), features, counts.toArray)
          }
      }
      .mapValues {
        x =>
          val allExpIdsWithCount = allExpIds.value zip Array.fill(allExpIds.value.length)(0)
          val (expIds, counts) = (allExpIdsWithCount ++ (x._1._2 zip x._3) groupBy(x=>x._1) mapValues(_.map(_._2).sum)).toSeq.unzip

          val ids = for (refId <- x._1._1;
                         expId <- expIds
          ) yield Hashing.md5().newHasher().putLong(refId).putLong(expId).hash().asLong

          val features = for (refFeature <- x._2;
                              expFeature <- counts)
            yield refFeature :+ expFeature.toDouble
          (ids, features)
      }

    reduced
  }
}


//case class MapKey(id: Long, chr: Int, start: Long, stop: Long, strand: Short, feature: Array[Double]){
//  override def equals(obj: scala.Any): Boolean = {
//    if (obj.isInstanceOf[MapKey]){
//      val that = obj.asInstanceOf[MapKey]
//        this.id == that.id &&
//          this.chr == that.chr &&
//        this.start == that.start &&
//        this.stop == that.stop &&
//        this.strand == that.strand &&
//          (this.feature zip that.feature).forall(x=>x._1 == x._2)
//    }
//    else false
//
//  }
//  override def hashCode: Int = {
//    chr
//  }
//  def getK(): (Int, Long, Long, Short) = {
//    (chr, start, stop, strand)
//  }
//}
//
case class Rep(id: Long, feature: Array[Double]){
  override def equals(obj: scala.Any): Boolean = {
    if (obj.isInstanceOf[Rep]) {
      val that = obj.asInstanceOf[Rep]
      this.id == that.id &&
      this.feature.sameElements(that.feature)
    }else
      false
  }

  override def hashCode(): Int = id.hashCode()
}