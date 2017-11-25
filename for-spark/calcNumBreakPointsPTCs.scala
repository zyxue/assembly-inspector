spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.executor.memory", "300g")

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
  println("Elapsed time: " + (t1 - t0) / 1e9 + "s")
  result
}

// val ds = spark.read.option("sep", "\t").schema(spanSchema).csv("/projects/btl/zxue/assembly_correction/celegans/toy_cov.csv").as[Span]
// val ds = spark.read.option("sep", "\t").schema(spanSchema).csv("/projects/btl/zxue/assembly_correction/celegans/cov.csv").as[Span]
val ds = spark.read.schema(spanSchema).csv("/projects/btl/zxue/assembly_correction/spruce/BX/agg_cov.concat.csv").as[Span]

// println(ds.count())
// println(ds.filter("read_count > 20").count())

val cc = CalcBreakPoints.toColumn.name("bp")


// val res = ds.groupByKey(a => a.ref_name).agg(cc)
val res = ds.filter("read_count > 5").groupByKey(a => a.ref_name).agg(cc)
res.persist

//.length >0 remove ref_names without break points detected
// reformatting into (ref_name, break_point) format for easier later processing
val colNames = Seq("ref_name", "break_point")
val out = res.filter(_._2.length > 0).flatMap(i => i._2.map(j => (i._1, j))).toDF(colNames: _*)

time {out.write.format("parquet").mode("overwrite").save("/projects/btl/zxue/assembly_correction/for-spark/filter-by-read-count-gt-5.parquet")}
