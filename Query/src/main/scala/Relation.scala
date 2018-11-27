import org.apache.spark.sql.{DataFrame, Row}
import DataAnalysisUtils._
import org.apache.spark.rdd.RDD

/**
  * Created by jugal112 on 5/21/17.
  */
class Relation(df: DataFrame) extends PopulationModel {

  override def selectivity(p: Predicate): Double = {
    return 1.0
  }

  override def distinct_values(col: String): List[Any] = {
    return df.select(col).distinct().rdd.map(r => r.get(0)).collect().toList
  }

  override def selectivity(b: Bias): Double = {
    df.createOrReplaceTempView("temp")
    var q: String = "select * from temp where "
    var preds: List[String] = List()
    for (p <- b.predicates) {
      var pred = ""
      if (p.operator == "IN") {
        pred = "%s %s (%s)".format(p.variable, p.operator, p.term.asInstanceOf[List[Number]].mkString(", "))
      } else {
        pred = "%s %s %s".format(p.variable, p.operator, p.term)
      }
      preds = pred :: preds
    }
    q += preds.mkString(" and ")
    val result = getSpark().sql(q)
    return result.count().toDouble/df.count()
  }

  def query(predicates: List[Predicate]): Relation = {
    df.createOrReplaceTempView("temp")
    var q: String = "select * from temp where "
    var preds: List[String] = List()
    for (p <- predicates) {
      var pred = ""
      if (p.operator == "IN") {
        pred = "%s %s (%s)".format(p.variable, p.operator, p.term.asInstanceOf[List[Number]].mkString(", "))
      } else {
        pred = "%s %s %s".format(p.variable, p.operator, p.term)
      }
      preds = pred :: preds
    }
    q += preds.mkString(" and ")
    return new Relation(getSpark().sql(q))
  }

  override def show() = {
    df.show()
  }
}
