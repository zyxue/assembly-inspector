// For implicit conversions from RDDs to DataFrames
import scala.collection.mutable.Map

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

// case class Average(var sum: Long, var count: Long)

// object MyAverage extends Aggregator[Employee, Average, Double] {
//   // A zero value for this aggregation. Should satisfy the property that any b + zero = b
//   def zero: Average = Average(0L, 0L)

//   // Combine two values to produce a new value. For performance, the function may modify `buffer`
//   // and return it instead of constructing a new object
//   def reduce(buffer: Average, employee: Employee): Average = {
//     buffer.sum += employee.salary
//     buffer.count += 1
//     buffer  
//   }

//   // Merge two intermediate values
//   def merge(b1: Average, b2: Average): Average = {
//     b1.sum += b2.sum
//     b1.count += b2.count
//     b1
  
//   }
//   // Transform the output of the reduction
//   def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

//   // Specifies the Encoder for the intermediate value type
//   def bufferEncoder: Encoder[Average] = Encoders.product

//   // Specifies the Encoder for the final output value type
//   def outputEncoder: Encoder[Double] = Encoders.scalaDouble
// }

// case class SinglePointCoverage(var: ref_name, var position: Long, var count: Long)
// case class BreakPoint(var: ref_name, var position)

// object CalcBreakPoints extends Aggregator[Span, Map[Int, Int], Int] {
//   // Reduce an array of spans to coverage, then to break points

//   // A zero value for this aggregation. Should satisfy the property that any b + zero = b
//   def zero: Map[Int, Int] = Map[Int, Int]()

//   // Combine two values to produce a new value. For performance, the function
//   // may modify `buffer` and return it instead of constructing a new object
//   def reduce(buffer: Map[Int, Int], span: Span): Map[Int, Int] = {
//     (span.beg until span.end).foreach(
//       i => buffer += (i -> (buffer.getOrElse[Int](i, 0) + 1)))
//     buffer
//   }

//   // Merge two intermediate values
//   def merge(b1: Map[Int, Int], b2: Map[Int, Int]): Map[Int, Int] = {
//     b2.foreach {
//       case (key, value) => b1 += (key -> value)
//     }
//     b1
//   }

//   // Transform the output of the reduction, convert to BreakPoint
//   def finish(reduction: Map[Int, Int]): Int = {
//     reduction.toList.length
//   }

//   // Specifies the Encoder for the intermediate value type
//   def bufferEncoder: Encoder[Map[Int, Int]] = Encoders.kryo

//   // Specifies the Encoder for the final output value type
//   def outputEncoder: Encoder[Int] = Encoders.scalaInt
// }


object CalcBreakPoints extends Aggregator[Span, Map[Int, Int], Array[Int]] {
  // Reduce an array of spans to coverage, then to break points

  // A zero value for this aggregation. Should satisfy the property that any b + zero = b
  def zero: Map[Int, Int] = Map[Int, Int]()

  // Combine two values to produce a new value. For performance, the function
  // may modify `buffer` and return it instead of constructing a new object
  def reduce(buffer: Map[Int, Int], span: Span): Map[Int, Int] = {
    (span.beg until span.end).foreach(
      i => buffer += (i -> (buffer.getOrElse[Int](i, 0) + 1)))
    buffer
  }

  // Merge two intermediate values
  def merge(b1: Map[Int, Int], b2: Map[Int, Int]): Map[Int, Int] = {
    b2.foreach {
      case (key, value) => b1 += (key -> value)
    }
    b1
  }

  // Transform the output of the reduction, convert to BreakPoint
  def finish(coverage: Map[Int, Int]): Array[Int] = {
    val cov_cutoff = 20;
    val f = (i: Int) => if (i >= cov_cutoff) 1 else 0

    val coords = coverage.keys.toArray.sorted;


    val bp = coords.slice(1, coords.length).map(
      c => {
        val current = f(coverage(c))
        val previous_step = f(coverage.getOrElse(c - 1, 0))
        (c, current - previous_step)
      })
      .filter { case(c, d) => d != 0}
      .map {case (c, d) => c}

    // val qualified = qualified.slice(1, qualified.length).map {
    //   case (c, b) => 
    //   c => if (coverage(c) >= read_count_cutoff) (c, 1) else (c, 0))
    // val diff = coords.slice(1, coords.length).map(c => (c, (reduction(c) - reduction.getOrElse(c - 1, 0))))
    // val bp = diff.filter {case (c, d) => d != 0} map {case (c, d) => c}
    bp
  }

  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[Map[Int, Int]] = Encoders.kryo

  // Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[Array[Int]] = Encoders.kryo
}



// val df = spark.read.option("sep", "\t").csv("/projects/btl/zxue/assembly_correction/celegans/cov.csv")
// val df = spark.read.csv("/projects/btl/zxue/assembly_correction/spruce/BX/agg_cov.csv/*.csv")
// val df = spark.read.csv("/projects/btl/zxue/assembly_correction/spruce/BX/agg_cov.concat.csv")

// val names = Seq("ref_name", "bc", "beg", "end", "read_count")
// val ndf = df.toDF(names: _*)

// val df = spark.read.option("sep", "\t").schema(spanSchema).csv("/projects/btl/zxue/assembly_correction/celegans/toy_cov.csv")
// val df = spark.read.option("sep", "\t").csv("/projects/btl/zxue/assembly_correction/celegans/toy_cov.csv").as[Span]

val ds = spark.read.option("sep", "\t").schema(spanSchema).csv("/projects/btl/zxue/assembly_correction/celegans/toy_cov.csv").as[Span]

val cc = CalcBreakPoints.toColumn.name("bp")
val res = ds.groupByKey(a => a.ref_name).agg(cc)  
res.write.format("parquet").save("/projects/btl/zxue/assembly_correction/for-spark/lele.parquet")



//for test

// scala> c
// res33: scala.collection.mutable.Map[Int,Int] = Map(8 -> 0, 2 -> 0, 5 -> 10, 4 -> 10, 7 -> 0, 1 -> 0, 9 -> 0, 3 -> 0, 6 -> 10, 0 -> 0)

// scala> c
// res34: scala.collection.mutable.Map[Int,Int] = Map(8 -> 0, 2 -> 0, 5 -> 10, 4 -> 10, 7 -> 0, 1 -> 0, 9 -> 0, 3 -> 0, 6 -> 10, 0 -> 0)

// scala> c.key
// keySet   keys   keysIterator

// scala> c.keys.toList.map(i => c(i)
//                            | )
// res35: List[Int] = List(0, 0, 10, 10, 0, 0, 0, 0, 10, 0)

// scala> coords
// res36: Array[Int] = Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)

// scala> coords.map(i => c(i))
// res37: Array[Int] = Array(0, 0, 0, 0, 10, 10, 10, 0, 0, 0)

// scala> coords.map(i => coverage(i))
// res38: Array[Int] = Array(0, 0, 0, 0, 10, 10, 10, 0, 0, 0)

// scala> coords.map(i => f(coverage(i)))
// res39: Array[Int] = Array(0, 0, 0, 0, 1, 1, 1, 0, 0, 0)

// scala> c.keys.toList.sorted.map(i => c(i)
//                                   | )
// res40: List[Int] = List(0, 0, 0, 0, 10, 10, 10, 0, 0, 0)
