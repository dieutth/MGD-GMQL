package de.tub.dima.operators

import com.google.common.hash.Hashing
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.input_file_name

object WriteParquet {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Test Parquet")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryoserializer.buffer", "64")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.sql.tungsten.enabled", "true")
      .set("spark.executor.heartbeatInterval", "300s")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", "/tmp/spark-events")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._


        val ds1 = spark.read.text("/home/dieutth/data/gmql/uncompressed/TADs_Aiden/*.gdm")
                      .withColumn("filename", input_file_name)
                          .map(x =>
                            {
                              val line = x.getString(0).split("\t")
                              val uri = x.getString(1).substring(x.getString(1).lastIndexOf("/")+1)
                              val uriExt =uri.substring(uri.lastIndexOf(".")+1,uri.size)
                              val URLNoMeta = if(!uriExt.equals("meta"))uri.substring(0,uri.size) else  uri.substring(0,uri.lastIndexOf("."))
                              val hashKey = Hashing.md5().newHasher().putString(URLNoMeta.replaceAll("/",""),java.nio.charset.StandardCharsets.UTF_8).hash().asLong()
                              Key(
                                hashKey,
                                line(0),
                                line(1).toLong,
                                line(2).toLong,
                                line(3).charAt(0).toShort
                              )
                            }).toDF()


        ds1.write.parquet("/home/dieutth/data/gmql/parquet/TAD_Aidens_id")


  }
}
case class Key(id:Long, chrom:String, start:Long, stop:Long, strand:Short)