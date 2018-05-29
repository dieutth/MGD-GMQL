package de.tub.dima

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}

object Experiment {
  def main(args: Array[String]): Unit = {

    def fileLines(file: java.io.File) =
      scala.io.Source.fromFile(file).getLines().toList


    val lines = fileLines(new File("/home/dieutth/allgdm.txt"))

    val regions = lines.map(x => x.split("\t")).map(
      x => (x(0), x(1).toLong, x(2).toLong, x(3))
    )

    val newR = for (
                r <- regions;
                r2 <- regions
                if r._1 == r2._1
                if (r._3 > r2._2-70000 && r._2 < r2._3 + 70000)// ||  (r._2 <= 140150000+100)
    ) yield r

//    val pw = new PrintWriter("/home/dieutth/exp.txt")
//    val pw = new BufferedWriter(new FileWriter(new File("/home/dieutth/exp.txt")))
//    newR.foreach(println)
//    pw.close()
    println(newR.size)

    val f = for (
      r <- regions;
      r2 <- newR
      if r._1 == r2._1
      if (r._3 > r2._2-70000 && r._2 < r2._3 + 70000)// ||  (r._2 <= 140150000+100)
    ) yield r

    var c = 0
     for (
      r <- regions;
      r2 <- f
      if r._1 == r2._1
      if (r._3 > r2._2-70000 && r._2 < r2._3 + 70000)// ||  (r._2 <= 140150000+100)
    ) c += 1
    println(c)

//    var count = 0
//    for (r <- regions){
//        for (r2 <- regions)
//            if (r._1 == r2._1 && (r._3 >= r2._2-70000 && r._2 <= r2._3 + 70000)) {
////              println(r)
//              count += 1
//            }
////    println("round")
//    }
//    println(count)


  }
}
