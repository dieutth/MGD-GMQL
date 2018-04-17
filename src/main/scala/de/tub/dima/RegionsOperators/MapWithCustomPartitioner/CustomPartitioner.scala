package de.tub.dima.RegionsOperators.MapWithCustomPartitioner

import java.lang.Exception

import com.google.common.hash.{Hasher, Hashing}
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core.GRecordKey
import org.apache.spark.Partitioner

class CustomPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts

//    override def getPartition(key: Any): Int = {
//      val convertedKey = key.asInstanceOf[GRecordKey]
//      convertedKey.chrom.substring(3).toInt - 1
//   }
//
  override def getPartition(key: Any): Int = {
//    val c = key.asInstanceOf[GRECORD]._1.chrom
//    val chrNumber = key.asInstanceOf[GRECORD]._1.chrom.substring(3)
//    if (key.isInstanceOf[Long]){
//      println("Wrong key type: key = ", key)
//      23
//    }
//    else{
      val k =  key.asInstanceOf[(Long,Int)]
//          k._2 - 1
  Hashing.md5().newHasher().putLong(k._1).putInt(k._2).hash().asInt() % numParts

//    }
  }

  override def equals(other: Any): Boolean =
    other match {
    case partitioner: CustomPartitioner =>
      partitioner.numPartitions == numPartitions
    case _ =>
      false
  }
}
