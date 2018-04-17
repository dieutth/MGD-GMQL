//package de.tub.dima.RegionsOperators.GenometricMap
//
//import com.google.common.hash.Hashing
//import de.tub.dima.MGD_GMQLSparkExecutor
//import de.tub.dima.dimension.Coordinate
//import it.polimi.genomics.core.DataStructures._
//import it.polimi.genomics.core.DataTypes.GRECORD
//import it.polimi.genomics.core.{GDouble, GNull, GRecordKey, GValue}
//import it.polimi.genomics.core.exception.SelectFormatException
//import org.apache.spark.{SparkContext, rdd}
//import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.rdd.RDD
//import org.slf4j.LoggerFactory
//
//import scala.collection.mutable.ArrayBuffer
//import scala.util.hashing.MurmurHash3
//
//class ParquetGMap {
//  private final val logger = LoggerFactory.getLogger(this.getClass)
//
//
//  @throws[SelectFormatException]
//  def apply(executor: MGD_GMQLSparkExecutor, grouping: OptionalMetaJoinOperator, aggregator: List[RegionAggregate.RegionsToRegion], ref: RDD[(GRecordKey, Array[GValue])], exp: RDD[(GRecordKey, Array[GValue])], BINNING_PARAMETER: Long, REF_PARALLILISM: Int, sc: SparkContext): RDD[GRECORD] = {
//    logger.info("----------------ParquetGMap executing -------------")
//
//
//
//    val binningParameter = 10000
//    //      if (BINNING_PARAMETER == 0)
//    //        Long.MaxValue
//    //      else
//    //        BINNING_PARAMETER
//
//    execute(executor, grouping, aggregator, ref, exp, binningParameter, REF_PARALLILISM, sc)
//  }
//
//  case class MapKey(/*sampleId: Long, */ newId: Long, refChr: Int, refStart: Long, refStop: Long, refStrand: Char, refValues: List[GValue]) {
//
//    override def equals(obj: scala.Any): Boolean = {
//      if (## == obj.##) {
//        val that = obj.asInstanceOf[MapKey]
//        // val result = this.productIterator.zip(that.productIterator).map(x=>x._1 equals x._2).reduce(_ && _)
//        this.refStart == that.refStart &&
//          this.refStop == that.refStop &&
//          this.refStrand == that.refStrand &&
//          //  this.sampleId == that.sampleId &&
//          this.newId == that.newId &&
//          this.refChr == that.refChr &&
//          this.refValues == that.refValues
//      }
//      else
//        false
//    }
//
//    @transient override lazy val hashCode: Int = MurmurHash3.productHash(this)
//  }
//
//  //possible solution is generate GRecordKey and add the others as list
//
//  @throws[SelectFormatException]
//  def execute(executor: MGD_GMQLSparkExecutor, grouping: OptionalMetaJoinOperator, aggregator: List[RegionAggregate.RegionsToRegion], ref: RDD[GRECORD], exp: RDD[GRECORD], BINNING_PARAMETER: Long, REF_PARALLILISM: Int, sc: SparkContext): RDD[GRECORD] = {
//    val groups = executor.implement_mjd(grouping, sc).flatMap { x => x._2.map(s => (x._1, s)) }
//
//    val refGroups: Broadcast[collection.Map[Long, Iterable[Long]]] = sc.broadcast(groups.groupByKey().collectAsMap())
//
//
//    implicit val orderGRECORD: Ordering[(GRecordKey, Array[GValue])] = Ordering.by { ar: GRECORD => ar._1 }
//
//    val expBinned = exp
//        .map{x=> {
//          (Coordinate(x._1.start, x._1.stop, x._1.chrom, x._1.strand.toShort), Array((x._1.id, x._2)))
//    }}
//       .binDS(BINNING_PARAMETER, aggregator)
//
//    val refBinnedRep = ref
//      //      .repartition(sc.defaultParallelism * 32 - 1)
//      .repartition(32)
//      .binDS(BINNING_PARAMETER, refGroups)
//
//    val emptyArrayGValue = Array.empty[GValue]
//    val emptyArrayInt = Array.empty[Int]
//
//
//    val indexedAggregator = aggregator.zipWithIndex
//
//    val reduceFunc: ((Array[GValue], Int, Array[Int]), (Array[GValue], Int, Array[Int])) => (Array[GValue], Int, Array[Int]) = {
//      case ((leftValues: Array[GValue], leftCount: Int, leftCounts: Array[Int]), (rightValues: Array[GValue], rightCount: Int, rightCounts: Array[Int])) =>
//        val values: Array[GValue] =
//          if (leftValues.nonEmpty && rightValues.nonEmpty) {
//            indexedAggregator.map { case (a, i) =>
//              a.fun(List(leftValues(i), rightValues(i)))
//            }.toArray
//          } else if (rightValues.nonEmpty)
//            rightValues
//          else
//            leftValues
//
//        (values, leftCount + rightCount, leftCounts.zipAll(rightCounts, 0, 0).map(s => s._1 + s._2))
//    }
//
//    val RefExpJoined: RDD[(Long, (GRecordKey, Array[Array[(Long, Double)]], Array[Double], Int))] = refBinnedRep.cogroup(expBinned)
//      .flatMap { grouped => val key: (Long, String, Int) = grouped._1;
//        val ref: Iterable[(Long, Long, Long, Char, Array[Array[(Long, Double)]])] = grouped._2._1//.toList.sortBy(x=>(x._1,x._2,x._3));
//        val exp: Iterable[(Long, Long, Char, Array[Double])] = grouped._2._2//.toList.sortBy(x=>(x._1,x._2))
//
//        ref.flatMap { refRecord =>
//          val hashID = Hashing.md5().newHasher().putLong(refRecord._1).putLong(key._1).hash().asLong;
//          val aggregation = Hashing.md5().newHasher().putString(hashID + key._2 + refRecord._2 + refRecord._3 + refRecord._4 + refRecord._5.mkString("/"), java.nio.charset.Charset.defaultCharset()).hash().asLong()
//          if (exp.nonEmpty){
//            exp.map { expRecord =>
//              if ( /* space overlapping */
//                (refRecord._2 < expRecord._2 && expRecord._1 < refRecord._3)
//                  && /* same strand */
//                  (refRecord._4.equals('*') || expRecord._3.equals('*') || refRecord._4.equals(expRecord._3))
//              ) {
//                //              refRecord.count += 1
//                (aggregation, (new GRecordKey(hashID, key._2, refRecord._2, refRecord._3, refRecord._4), refRecord._5, expRecord._4, 1))
//              } else {
//                (aggregation, (new GRecordKey(hashID, key._2, refRecord._2, refRecord._3, refRecord._4), refRecord._5, Array[Double](), 0))
//              }
//            }
//          }else {
//            Array((aggregation, (new GRecordKey(hashID, key._2, refRecord._2, refRecord._3, refRecord._4), refRecord._5, Array[Double](), 0)))
//          }
//        }
//
//        // sweep(key,ref.iterator,exp.iterator,BINNING_PARAMETER)
//
//      } //.cache()
//
//    val reduced  = RefExpJoined.reduceByKey{(l,r)=>
//      (l._1,l._2,l._3,l._4+r._4)
//    }//cache()
//
//    //    RefExpJoined.unpersist(true)
//    //Aggregate Exp Values (reduced)
//
//    //    RefExpJoined.collect().map(x => println(x._2._1.toString() + "\t"+x._2._4))
//    val output: RDD[(GRecordKey,Array[Array[(Long, Double)]], Int)] = reduced.map{ res =>
//      (res._2._1,res._2._2, res._2._4)
//    }
//
//    //    reduced.unpersist()
//
//    //    output.collect.map(x=> println(x._1.toString() + x._2.map( e => (e.mkString("|"))).mkString(", \t") + "||" + x._3))
//    output.groupBy(x => x._1).map{ case (x,y)=>
//
//      (x,  y.flatMap{case (a,b, c) =>
//        val ids = b.head.map(_._1)
//        val counts: Array[(Long, Double)] = ids.map((_,c.toDouble))
//        b :+ counts
//      }.toArray)
//    }
//  }
//
//  implicit class Binning(rdd: RDD[(Coordinate, Array[(Long, Array[GValue])])]) {
//
//    def binDS(bin: Long, aggregator: List[RegionAggregate.RegionsToRegion]): RDD[((Int, Int), (Long, Long, Char, Array[(Long, Array[GValue])]))] =
//      rdd.flatMap { x =>
//        //        if (bin > 0) {
//
//        val startBin = (x._1.start / bin).toInt
//        val stopBin = (x._1.stop / bin).toInt
//        val chr = {
//          val c = x._1.chr.substring(3)
//          if (c == "X")
//            23
//          else
//            c.toInt
//        }
//
//        val newVal: Array[(Long, Array[GValue])] = for (item <- x._2)
//          yield
//            (item._1, aggregator
//              .map((f: RegionAggregate.RegionsToRegion) => {
//                item._2(f.index)
//              }).toArray)
//
//        (startBin to stopBin).map(i => ((chr, i), (x._1.start, x._1.stop, x._1.strand.toChar, newVal)))
//      }
//  }
//
//  implicit  class RefBinning(rdd: RDD[GRECORD]){
//    def binDS(bin: Long, Bgroups: Broadcast[collection.Map[Long, Iterable[Long]]]): RDD[((Long, Int, Int), (Long, Long, Long, Char, Array[GValue]))] =
//      rdd.flatMap { x =>
//        val startBin = (x._1._3 / bin).toInt
//        val stopBin = (x._1._4 / bin).toInt
//        val chr ={
//          val c =  x._1.chrom.substring(3)
//          if (c == "X")
//            23
//          else
//            c.toInt
//        }
//
//        Bgroups.value.getOrElse(x._1._1, Iterable[Long]()).flatMap { exp_id =>
//          val newID = Hashing.md5().newHasher().putLong(x._1._1).putLong(exp_id).hash().asLong
//
//          (startBin to stopBin).map { i =>
//            ((exp_id, chr , i), (newID, x._1._3, x._1._4, x._1._5, x._2))
//          }
//        }
//
//      }
//
//  }
//
//}
