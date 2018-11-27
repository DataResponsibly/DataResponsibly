import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructField
import Histogram._

import scala.collection.mutable

/**
  * Created by jugal112 on 5/6/17.
  */
class HistoFrame (df:DataFrame, buckets: Int, categorical_cols: List[String]) extends PopulationModel {

  def this(df: DataFrame, buckets: Int) = {
    this(df, buckets, List[String]())
  }

  var schema = df.schema.fields
  var histograms = new mutable.HashMap[String, Histogram]()
  for (field <- schema) {
    try {
      histograms(field.name) = new Histogram(df, field.name, buckets, Equiwidth, categorical_cols.contains(field.name))
      printf("Created Histogram for %s\n", field.name)
    } catch {
      case e:Exception => {
        printf("Cannot handle %s yet for column %s\n", field.dataType, field.name)
      }
    }
  }

  def showDF() = {
    df.show()
  }

  def show() = {
    for (field <- schema) {
      try {
        histograms(field.name).show()
      } catch {
        case e:Exception => {printf("Couldn't print %s\n", field.name)}
      }
    }
  }

  override def distinct_values(col: String): List[Any] = {
    var histogram = histograms(col)
    var column_of_interest = if (histogram.is_categorical) "bucket" else "max"
    var values =  histogram.data.select(column_of_interest).rdd.map(r => r.get(0)).collect().toList
    if (histogram.is_categorical) {
	return values
    } else {
	return histogram.min :: values
    }
  }

  override def selectivity(p: Predicate): Double = {
    val hist = histograms(p.variable)
    p.operator match {
      case "<" => {
        return hist.selectivity(Double.MinValue, p.term.asInstanceOf[Number])
      }
      case ">" => {
        return hist.selectivity(p.term.asInstanceOf[Number], Double.MaxValue)
      }
      case "<=" => {
        return (hist.selectivity(Double.MinValue, p.term.asInstanceOf[Number])
        + hist.selectivity(p.term.asInstanceOf[Number]))
      }
      case ">=" => {
        return (hist.selectivity(p.term.asInstanceOf[Number], Double.MaxValue)
        + hist.selectivity(p.term.asInstanceOf[Number]))
      }
      case "=" => {
        return hist.selectivity(p.term.asInstanceOf[Number])
      }
      case "IN" => {
        return hist.selectivity(p.term.asInstanceOf[List[Number]])
      }
    }
  }


}
