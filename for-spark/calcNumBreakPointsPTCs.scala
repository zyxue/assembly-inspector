import scala.collection.immutable.Map

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders

import spark.implicits._
import org.apache.spark.sql.types._


case class Span(
  ref_name: String,
  bc: String,
  beg: Int,
  end: Int,
  read_count: Int)


val spanSchema = StructType(
  Array(
    StructField("ref_name", StringType, true),
    StructField("bc", StringType, true),
    StructField("beg", IntegerType, true),
    StructField("end", IntegerType, true),
    StructField("read_count", IntegerType, true)
  )
)


// PCT: PointCoverageTransition
case class PCT(
  loc: Int,
  cov: Int,
  nextCov: Int
)

type Coverage = Array[PCT]
def Coverage(xs: PCT*) = Array(xs: _*)

val cov1 = Coverage(PCT(-1, 0, 10), PCT(3, 10, 20), PCT(10, 20, 0))
val cov2 = Coverage(PCT(5, 0, 3), PCT(12, 3, 0))


def mergeCoverages(cov1: Coverage, cov2: Coverage): Coverage = {
  if (cov1.length == 0) {
    cov2
  } else if (cov2.length == 0) {
    cov1
  } else {
    val cc = (cov1 ++ cov2).sortBy(_.loc)
    var newCov = Coverage()
    for (pct <- cc) {
      if (newCov.length == 0) {
        newCov :+= pct
      } else {
        var ti = newCov.length - 1 // tail index
        var tailPCT = newCov(ti)

        if (pct.loc == tailPCT.loc) {
          newCov(ti) = PCT(pct.loc,
                           tailPCT.cov,
                           tailPCT.nextCov + pct.nextCov - pct.cov)
        } else {
          newCov :+= PCT(pct.loc,
                         tailPCT.nextCov,
                         tailPCT.nextCov + pct.nextCov - pct.cov)
        }
      }
    }
    newCov
  }
}


object CalcBreakPoints extends Aggregator[Span, Coverage, Array[Int]] {
  def zero: Coverage = Coverage()

  def reduce(buffer: Coverage, span: Span): Coverage = {
    val cov = Coverage(PCT(span.beg - 1, 0, 1), PCT(span.end, 1, 0))
    mergeCoverages(buffer, cov)
  }

  def merge(cov1: Coverage, cov2: Coverage): Coverage = {
    mergeCoverages(cov1, cov2)
  }

  def finish(cov: Coverage): Array[Int] = {
    val cov_cutoff = 100;
    val bp = cov
      .filter(i => (i.cov >= cov_cutoff && i.nextCov < cov_cutoff) || (i.cov <= cov_cutoff && i.nextCov > cov_cutoff))
      .map(_.loc)
    bp
  }

  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[Coverage] = Encoders.kryo

  // Specifies the Encoder for the final output value type
  // def outputEncoder: Encoder[Int] = Encoders.scalaInt
  def outputEncoder: Encoder[Array[Int]] = Encoders.kryo
}

def time[R](block: => R): R = {
  val t0 = System.nanoTime()
  val result = block    // call-by-name
  val t1 = System.nanoTime()
  println("Elapsed time: " + (t1 - t0) + "ns")
  result
}

// val ds = spark.read.option("sep", "\t").schema(spanSchema).csv("/projects/btl/zxue/assembly_correction/celegans/toy_cov.csv").as[Span]
// val ds = spark.read.option("sep", "\t").schema(spanSchema).csv("/projects/btl/zxue/assembly_correction/celegans/cov.csv").as[Span]
// val df = spark.read.csv("/projects/btl/zxue/assembly_correction/spruce/BX/agg_cov.csv/*.csv")
val ds = spark.read.schema(spanSchema).csv("/projects/btl/zxue/assembly_correction/spruce/BX/agg_cov.concat.csv").as[Span]

// println(ds.count())
// println(ds.filter("read_count > 20").count())

val cc = CalcBreakPoints.toColumn.name("bp")
//.length >0 remove ref_names without break points detected

val res = ds.filter("read_count > 20").groupByKey(a => a.ref_name).agg(cc)
// reformatting into (ref_name, break_point) format for easier later processing
val colNames = Seq("ref_name", "break_point")
val out = res  .filter(_._2.length > 0).flatMap(i => i._2.map(j => (i._1, j))).toDF(colNames: _*)
out.persist
time {out.write.format("parquet").mode("overwrite").save("/projects/btl/zxue/assembly_correction/for-spark/lele.parquet")}


// // def initMap(beg: Int, end: Int): Map[Int, Int] = {
// //   Map[Int, Int]() ++ (beg until end).map(i => (i -> 1))
// // }


// // def mergeMap(m1: Map[Int, Int], m2: Map[Int, Int]): Map[Int, Int] = {
// //   m2.foreach {
// //     case (key, value) => m1 += (key -> (value + m1.getOrElse[Int](key, 0)))
// //   }
// //   m1
// // }


// // def initCov(beg: Int, end: Int): IndexedSeq[(Int, Int)] = {
// //   (beg until end).map(i => (i -> 1))
// // }


// // def mergeCov(m1: IndexedSeq[(Int, Int)], m2: IndexedSeq[(Int, Int)]): IndexedSeq[(Int, Int)] = {
// //   (m1 ++ m2)
// //     .groupBy(i => i._1)
// //     .map(j => (j._1, j._2.map(k => k._2).sum))
// //     .toIndexedSeq
// //     // .sortBy(i => i._1)
// // }


// // val r = ds.map(span => (span.ref_name, initCov(span.beg, span.end)))

// // val r2 = r.groupByKey(i => i._1)

// // val r3 = r2.reduceGroups((a, b) => (a._1, mergeCov(a._2, b._2)))

// // val r4 = r3.collect()


// // val r =  ds.flatMap(span => Array.range(span.beg, span.end))
// // val r2 = r.map(c => (c, 1))
// // val r3 = r2.groupByKey(_._1).reduceGroups((a, b) => (a._1, (a._2 + b._2)).map(_._2)


// val input =  sc.textFile("/projects/btl/zxue/assembly_correction/celegans/toy_cov.csv")
// // val input =  sc.textFile("/projects/btl/zxue/assembly_correction/celegans/cov.csv")
// // // Split up into words.
// // val words = input.map(line => line.split("\t")).flatMap(ls => Array.range(ls(2).toInt, ls(3).toInt))
// // // Transform into word and count.
// // val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
// // // Save the word count back out to a text file, causing evaluation.
// // counts.saveAsTextFile("/projects/btl/zxue/assembly_correction/for-spark/lele")
