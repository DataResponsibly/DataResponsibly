import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.apache.spark.sql.types._
import DataAnalysisUtils._
import org.apache.spark.sql.functions.{approxCountDistinct}

import scala.collection.mutable
import scala.math.round

/**
  * Created by jugal112 on 3/15/17.
  */
object DataFrameUtils {

  implicit class DataFrameUtils(val df: DataFrame) extends java.io.Serializable {

    //Todo: need one more argument for "categorical"
    /*
      If the column is categorical then we will just have to groupby that column. Can't think of a reason why we
      would want to bucket categories together... unless the categories have an ordering/precedence.
        Example: ["PhD", "Masters", "Bachelors", "Associates", "High School Diploma", "None"]
        If we use 2 buckets, do we want ["PhD-Bachelors", "Associates-None"]?
     */
    //Todo: Do we want equidepth vs equiwidth as an argument passed into the function or in a separate function?
    /*
    To calculate equidepth, we need to sort first, and then we also need to index the rows so that we can pull out the
    appropriate bounds. This can be done with the rdd.zipWithIndex function
     */

    private[this] def calculate_selection(min: Number, max: Number, count: Long, lower: Number, higher: Number): Double = {
      if (higher.doubleValue() <= lower.doubleValue()) {
        println("Warning: Higher bound is less than or equal to Lower")
        return 0
      }
      if (lower.doubleValue() < min.doubleValue() && higher.doubleValue() > max.doubleValue()) {
        return count
      }
      else if (max.doubleValue() < lower.doubleValue() || min.doubleValue() > higher.doubleValue()) {
        return 0
      }
      else {
        var l = 0.0
        var u = 1.0
        if (lower.doubleValue() >= min.doubleValue() && lower.doubleValue() <= max.doubleValue()) {
          l = (lower.doubleValue() - min.doubleValue())/(max.doubleValue() - min.doubleValue())
        }
        if (higher.doubleValue() >= min.doubleValue() && higher.doubleValue() <= max.doubleValue()) {
          u = (higher.doubleValue() - min.doubleValue()) / (max.doubleValue() - min.doubleValue())
        }
        return (u - l)*count
      }
    }

    private[this] def calculate_selection(min: Number, max: Number, count: Long, value:Number): Double = {
      if (value.doubleValue() > min.doubleValue() && value.doubleValue() < max.doubleValue()) {
        return 1.0/(max.doubleValue()-min.doubleValue()+1)
      }
      else {
        return 0
      }
    }

    private[this] def calculate_selection(min: Number, max: Number, count: Long, values:List[Number]): Double = {
      var sum = 0.0
      for (value <- values) {
        if (value.doubleValue() > min.doubleValue() && value.doubleValue() < max.doubleValue()) {
          sum += 1.0/(max.doubleValue()-min.doubleValue()+1)
        }
      }
      return sum
    }

    private[this] def sum_squared_error(data:DataFrame, histogram:DataFrame): Double = {
      val map = mutable.HashMap
      return 0.0
    }

    def selectivity(lower: Number, higher: Number): Double = {
      //Apply a lambda function on bucket min and bucket max and count.
      //Map function(min, max, count) on dataframe and return estimated count
      //Sum column
      val rows = df.rdd.map(r=> Row.fromSeq(
        r.toSeq ++ Seq(
          calculate_selection(
            r.getAs[Double]("min"),
            r.getAs[Double]("max"),
            r.getAs[Long]("count"),
            lower,
            higher
          )
        )))
      val scheme = StructType(df.schema.fields ++ Array(StructField("selected", DoubleType)))
      val selected = getSpark().createDataFrame(rows, scheme)
      val sum = selected.select("selected").rdd.map(_(0).asInstanceOf[Double]).reduce(_+_)
      val count = selected.select("count").rdd.map(_(0).asInstanceOf[Long]).reduce(_+_)
      selected.show(10)
      return sum/count
    }

    def selectivity(values: List[Number]): Double = {
      val rows = df.rdd.map(r=> Row.fromSeq(
        r.toSeq ++ Seq(
            calculate_selection(
            r.getAs[Double]("min"),
            r.getAs[Double]("max"),
            r.getAs[Long]("count"),
            values
            )
        )))
      val scheme = StructType(df.schema.fields ++ Array(StructField("selected", DoubleType)))
      val selected = getSpark().createDataFrame(rows, scheme)
      val sum = selected.select("selected").rdd.map(_(0).asInstanceOf[Double]).reduce(_+_)
      val count = selected.select("count").rdd.map(_(0).asInstanceOf[Long]).reduce(_+_)
      selected.show(10)
      return sum/count
    }

    def selectivity(value: Number): Double = {
      val rows = df.rdd.map(r=> Row.fromSeq(
        r.toSeq ++ Seq(
          calculate_selection(
          r.getAs[Double]("min"),
          r.getAs[Double]("max"),
          r.getAs[Long]("count"),
          value,
          value.doubleValue()+1
        )
        )))
      val scheme = StructType(df.schema.fields ++ Array(StructField("selected", DoubleType)))
      val selected = getSpark().createDataFrame(rows, scheme)
      val sum = selected.select("selected").rdd.map(_(0).asInstanceOf[Double]).reduce(_+_)
      val count = selected.select("count").rdd.map(_(0).asInstanceOf[Long]).reduce(_+_)
      selected.show(10)
      return sum/count
    }

    def equijoinwith(other: DataFrame): DataFrame = {
      val joined = df.join(other, bucket_intersect(df, other))
      return joined
    }

    def bucket_intersect(df1: DataFrame, df2: DataFrame): Column = {
      val minimum1 = df1.col("min")
      val minimum2 = df2.col("min")
      val maximum1 = df1.col("max")
      val maximum2 = df2.col("max")
      return minimum1.geq(minimum2).and(minimum1.leq(maximum2)).or(maximum1.geq(minimum2).and(maximum1.leq(maximum2)))
    }

    private[this] def compute_bucket(col: Double, min: Double, increment: Double): Seq[Double] = {
      val bucket = round((col - min) / increment)
      //Todo: perhaps we want to generate two columns. one for the lower and one for the higher bound of the bucket
      //As of right now, we are only giving the lower bound of the bucket
      return Seq(bucket * increment + min, (bucket+1)* increment + min)
    }
  }

}
