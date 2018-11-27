/**
  * Created by jugal112 on 5/18/17.
  */
class Bias {
  var predicates: List[Predicate] = List()

  def add(p:Predicate): Bias = {
    predicates = p :: predicates
    return this
  }

  override def toString: String = predicates.mkString(" and ")
}
