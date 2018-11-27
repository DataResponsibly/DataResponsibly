import DataAnalysisUtils.getSpark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.math.round

/**
  * Created by jugal112 on 5/21/17.
  */
object Histogram {
  val Equiwidth = "equiwidth"
  val Equidepth = "equidepth"
  val Equivariant = "equivariant"

  class Histogram(df: DataFrame, col: String, buckets_desired: Int, histogram_type: String, categorical: Boolean) extends Serializable {
    val columnData: DataFrame = df.select(col)

    var max: Number = null
    var min: Number = null
    var is_categorical = categorical
    var numDistinct = df.select(approxCountDistinct(col)).collect()(0).getAs[Long](0)
    var buckets = Array(numDistinct, buckets_desired, 256).reduceLeft(_ min _)
    val data: DataFrame = build_histogram()

    def this(df: DataFrame, col: String, buckets: Int, categorical: Boolean) {
      this(df, col, buckets, Equiwidth, categorical)
    }

    def this(df: DataFrame, col: String, categorical: Boolean) {
      this(df, col, 256, Equiwidth, categorical)
    }

    def show() = {
      data.show()
    }

    private[this] def build_histogram(): DataFrame = {
      val dt = columnData.schema(col).dataType
      max = columnData.agg((col, "max")).collect()(0).getAs[Number](0)
      min = columnData.agg((col, "min")).collect()(0).getAs[Number](0)
      //      var max:Either[Number, String] = null
      //      var min:Either[Number, String] = null
      //      val numericType = IntegerType.getClass.getSuperclass.getSuperclass.getSuperclass
      //      dt match {
      //        /* There must be a way to check if the column is any NumericType.
      //        still figuring this out as NumericType is inaccessible here.*/
      //        case numericType => {
      //          max = Left(columnData.agg((col, "max")).collect()(0).getAs[Number](0))
      //          min = Left(columnData.agg((col, "max")).collect()(0).getAs[Number](0))
      //        }
      //        //Todo: handle string columns
      //        /*
      //        String columns need to be handled in a different way. Naively, we can first check the size of the column.
      //        If the number of distinct values is greater than the buckets, then we will require a function to generate a
      //        list of buckets for us. I.e. 3 buckets = ['A', 'J', 'S'] which corresponds to ['A-I', 'J-R', 'S-Z']
      //        As we have more than 26 buckets, then it becomes more interesting...
      //
      //        Trying to think of a way to do with quantitatively if we can convert letters into numbers.
      //         */
      //
      //        case StringType => {
      //          max = Right(columnData.agg((col, "max")).collect()(0).getString(0))
      //          min = Right(columnData.agg((col, "max")).collect()(0).getString(0))
      //        }
      //        case _ => {
      //          printf("Column type %s is not handled yet.", dt.toString())
      //          return df
      //        }
      //      }
      if (is_categorical || buckets==numDistinct) {
        is_categorical = true
        return df.withColumn(col, df.col(col).cast(LongType)).groupBy(col).count().withColumnRenamed(col, "bucket").sort("bucket")
      } else {
        histogram_type match {
          case "equiwidth" => {
            return equiwidth_histogram()
          }
          case "equidepth" => {
            return equidepth_histogram()
          }
          case "equivariant" => {
            //not finished yet. failling over to equiwidth_histogram for now.
            return equiwidth_histogram()
          }
        }
      }
    }

    private[this] def equiwidth_histogram(): DataFrame = {
      val range = max.doubleValue() - min.doubleValue()
      val increment = range / (buckets-1)
      val rows = columnData.rdd.map(r => Row.fromSeq(r.toSeq ++ compute_bucket(r.getAs[Number](col).doubleValue(), min.doubleValue(), increment)))
      //columnData = columnData.withColumn("bucket", compute_bucket(columnData(col), min, increment))
      val schema = StructType(columnData.schema.fields ++ Array(StructField("min", DoubleType), StructField("max", DoubleType)))
      val result = getSpark().createDataFrame(rows, schema).groupBy("min", "max").count().sort("min")
      //var result = columnData.groupBy("bucket").count().sort("bucket")
      return result.select("min", "max", "count")
    }

    private[this] def equidepth_histogram(): DataFrame = {
      var sorted = columnData.sort(col)
      val count = sorted.count()
      val increment = (count - 1) / (buckets.toDouble - 1)
      var indices: Set[Long] = Set()
      for (i <- 0 to buckets.toInt) {
        indices += round(i * increment)
      }

      var dictionary = sorted.rdd.zipWithIndex().collect({
        case (r, i) if (indices.contains(i)) => Row.fromSeq(r.toSeq ++ Seq(i))
      }).map((r) => (r.getLong(1), r.getAs[Number](0).doubleValue())).collectAsMap()

      var indexed = sorted.rdd.zipWithIndex().collect({
        case (r, i) => Row.fromSeq(r.toSeq ++ compute_bucket(i, 0, increment).map((x) => dictionary(round(x))))
      })

      val schema = StructType(columnData.schema.fields ++ Array(StructField("min", DoubleType), StructField("max", DoubleType)))
      val result = getSpark().createDataFrame(indexed, schema).groupBy("min", "max").count().sort("min", "max")
      //var result = columnData.groupBy("bucket").count().sort("bucket")
      return result.select("min", "max", "count")
    }

    //Not yet implemented
    private[this] def equivariant_histogram(): DataFrame = {
      return df
    }
//    private[this] def equidepth_histogram2(): DataFrame = {
//      var sorted = columnData.sort(col)
//      val increment = sorted.count() / buckets
//      var indexed = sorted.rdd.zipWithIndex().collect(
//        { case (r, i) => Row.fromSeq(r.toSeq ++ Array[Long](
//          (i / increment) * increment))
//        }
//      )
//      println(indexed)
//      var bucketed = sorted.rdd.zipWithIndex().collect(
//        { case (r, i) if i % increment == 0 => Row.fromSeq(r.toSeq ++ Array[Long](
//          i))
//        }
//      )
//
//
//      val newSchema = StructType(sorted.schema.fields ++ Array(StructField("index", LongType)))
//
//      //val mapping: Map[String, Column => Column] = Map("min" - min, "max" -> max, "count", count)
//      var keys = getSpark().createDataFrame(bucketed, newSchema)
//      var result = getSpark().createDataFrame(indexed, newSchema).groupBy("index").count().join(keys, "index").sort(col).drop("index")
//      return result.withColumnRenamed(col, "bucket").select("bucket", "count")
//    }

    private[this] def calculate_selection(min: Number, max: Number, count: Long, lower: Number, higher: Number): Double = {
      if (higher.doubleValue() < lower.doubleValue()) {
        println("Warning: Higher bound is less than Lower")
        return 0
      }
      if (lower.doubleValue() < min.doubleValue() && higher.doubleValue() > max.doubleValue()) {
        return count
      }
      else if (max.doubleValue() < lower.doubleValue() || min.doubleValue() > higher.doubleValue()) {
        return 0
      }
      else {
        if (is_categorical) {
          return 0
        }
        var l = 0.0
        var u = 1.0
        if (lower.doubleValue() >= min.doubleValue() && lower.doubleValue() <= max.doubleValue()) {
          l = (lower.doubleValue() - min.doubleValue()) / (max.doubleValue() - min.doubleValue())
        }
        if (higher.doubleValue() >= min.doubleValue() && higher.doubleValue() <= max.doubleValue()) {
          u = (higher.doubleValue() - min.doubleValue()) / (max.doubleValue() - min.doubleValue())
        }
        return (u - l) * count
      }
    }

    private[this] def calculate_selection(min: Number, max: Number, count: Long, value: Number): Double = {
      if (value.doubleValue() > min.doubleValue() && value.doubleValue() < max.doubleValue()) {
        return 1.0 / (max.doubleValue() - min.doubleValue() + 1)
      }
      else {
        return 0
      }
    }

    private[this] def sum_squared_error(data: DataFrame, histogram: DataFrame): Double = {
      val map = mutable.HashMap
      return 0.0
    }

    def selectivity(lower: Number, higher: Number): Double = {
      var rows:RDD[Row] = null
      val (mincol, maxcol) = if (is_categorical) ("bucket", "bucket") else ("min", "max")
      rows = data.rdd.map(r => Row.fromSeq(
        r.toSeq ++ Seq(
          calculate_selection(
            r.getAs[Long](mincol),
            r.getAs[Long](maxcol),
            r.getAs[Long]("count"),
            lower,
            higher
          )
        )))
      val scheme = StructType(data.schema.fields ++ Array(StructField("selected", DoubleType)))
      val selected = getSpark().createDataFrame(rows, scheme)
      val sum = selected.select("selected").rdd.map(_ (0).asInstanceOf[Double]).reduce(_ + _)
      val count = selected.select("count").rdd.map(_ (0).asInstanceOf[Long]).reduce(_ + _)
      return sum / count
    }

    def selectivity(values: List[Number]): Double = {
      val numberSet = values.toSet
      var sum = 0.0
      for (n <- values) {
        sum += selectivity(n)
      }
      return sum
    }

    def selectivity(value: Number): Double = {
      if (is_categorical) {
        var sum = data.rdd.map(r => (if (r.getAs[Number]("bucket").longValue() == value.longValue()) r.getAs[Long]("count") else 0)).reduce(_ + _)
        var count = data.select("count").rdd.map(_ (0).asInstanceOf[Long]).reduce(_ + _)
        return sum.toDouble / count
      } else {
        val rows = data.rdd.map(r => Row.fromSeq(
          r.toSeq ++ Seq(
            calculate_selection(
              r.getAs[Double]("min"),
              r.getAs[Double]("max"),
              r.getAs[Long]("count"),
              value.doubleValue(),
              value.doubleValue() + 1
            )
          )))
        val scheme = StructType(data.schema.fields ++ Array(StructField("selected", DoubleType)))
        val selected = getSpark().createDataFrame(rows, scheme)
        val sum = selected.select("selected").rdd.map(_ (0).asInstanceOf[Double]).reduce(_ + _)
        val count = selected.select("count").rdd.map(_ (0).asInstanceOf[Long]).reduce(_ + _)
        return sum / count
      }
    }

    def equijoinwith(other: DataFrame): DataFrame = {
      val joined = data.join(other, bucket_intersect(data, other))
      return joined
    }

    def bucket_intersect(df1: DataFrame, df2: DataFrame): Column = {
      val minimum1 = df1.col("min")
      val minimum2 = df2.col("min")
      val maximum1 = df1.col("max")
      val maximum2 = df2.col("max")
      return minimum1.geq(minimum2).and(minimum1.leq(maximum2)).or(maximum1.geq(minimum2).and(maximum1.leq(maximum2)))
    }

    private[this] def compute_bucket(col_value: Double, min: Double, increment: Double): Seq[Double] = {
      var bucket = ((col_value - min) / increment)
      //This is to make sure that buckets are inclusive of their end values.
      if (((bucket.toLong - bucket) == 0) && bucket != 0) {
        bucket -= 1
      }
      bucket = bucket.toLong
      //Todo: perhaps we want to generate two columns. one for the lower and one for the higher bound of the bucket
      //As of right now, we are only giving the lower bound of the bucket
      return Seq(bucket * increment + min, (bucket + 1) * increment + min)
    }

  }

}
