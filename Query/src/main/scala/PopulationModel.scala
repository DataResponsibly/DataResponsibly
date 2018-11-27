import org.antlr.v4.runtime.atn.SemanticContext.Predicate

/**
  * Created by jugal112 on 5/17/17.
  */
abstract class PopulationModel {

  def selectivity(b: Bias): Double = {
    var selection = 1.0
    for (p <- b.predicates) {
      selection *= selectivity(p)
    }
    return selection
  }

  def distinct_values(col: String): List[Any]

  def selectivity(p: Predicate): Double

  def show()
}
