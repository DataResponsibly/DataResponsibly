import java.io.{File, PrintWriter}

import DataFrameUtils._
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import Histogram._
import DataAnalysisUtils._

import scala.math.round

object Main {
  def main(args: Array[String]): Unit = {

    val spark = getSpark()
    LogManager.getRootLogger.setLevel(Level.OFF)

    val path = args(0)
    var df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    df = df.drop("priors_count.1")
    df.printSchema()
    df.show()

    var predicates: List[Predicate] = List()
    predicates = new SelectionPredicate("race").isEqualTo() :: predicates
    predicates = new SelectionPredicate("sex").isEqualTo() :: predicates
    predicates = new SelectionPredicate("age").isLessThan() :: predicates

    def generatePossibilities(ls: List[Predicate], pm: PopulationModel): List[List[Predicate]] = {
      var possibilities: List[List[Predicate]] = List()
      for (predicate <- ls) {
        val distinct_values = pm.distinct_values(predicate.variable)
        var enumerated: List[Predicate] = List(null)
        for (value <- distinct_values) {
          enumerated = predicate.copy().setTerm(value) :: enumerated
        }
        possibilities = enumerated :: possibilities
      }
      return possibilities
    }

    def combinationList[T](ls:List[List[T]]):List[List[T]] = ls match {
      case Nil => Nil::Nil
      case head :: tail => val rec = combinationList[T](tail)
        rec.flatMap(r => head.map(t => if (t != null) t::r else r))
    }

    var bias = new Bias().add(new SelectionPredicate("v_decile_score").isGreaterOrEqualTo(1))

    val P = new HistoFrame(df, 20, List("race", "sex", "MarriageStatus", "c_charge_degree", "is_recid", "is_violent_recid"))
    val querySelections = combinationList(generatePossibilities(predicates, P))

    var biases: List[Bias] = List()
    for (i <- 1 to 10) {
      biases = new Bias().add(new SelectionPredicate("v_decile_score").isGreaterOrEqualTo(i)) :: biases
    }

    val propublica = new Relation(df)

    var pw = new PrintWriter(new File("output.csv"))
    pw.write(List("Query", "Bias", "P", "Q").mkString(",")+"\n")

    for (querySelect <- querySelections; bias <- biases) {
      println("Query: " + querySelect.mkString(" and "))
      println("Bias: " + bias)
      var s1 = P.selectivity(bias)
      println("P = "+ s1)

      var Q = propublica.query(querySelect)
      var s2 = Q.selectivity(bias)
      println("Q = " + s2)
			println("")      
      pw.write(List(querySelect.mkString(" and "), bias, s1, s2).mkString(",") + "\n")

    }
    pw.close()

    spark.stop()

  }

}
