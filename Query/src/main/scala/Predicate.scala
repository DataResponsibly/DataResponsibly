/**
  * Created by jugal112 on 5/18/17.
  */
class Predicate(col: String) {
  var variable: String = col
  var operator: String = null
  var term: Any = null

  def this() = {
    this(null)
  }

  def copy(): Predicate = {
    new Predicate(col).setOperator(operator).setTerm(term)
  }

  def setTerm(v: Any): Predicate = {
    this.term = v
    return this
  }

  def setOperator(s: String): Predicate = {
    this.operator = s
    return this
  }

  override def toString():String = String.format(
    "%s %s %s",
    variable.toString(),
    operator.toString(),
    term.toString())
  
  def isGreaterThan(v: Any = null): Predicate = {
    return this.setOperator(">").setTerm(v)
  }

  def isLessThan(v: Any = null): Predicate = {
    return this.setOperator("<").setTerm(v)
  }

  def isGreaterOrEqualTo(v: Any = null): Predicate = {
    return this.setOperator(">=").setTerm(v)
  }

  def isLessOrEqualTo(v: Any = null): Predicate = {
    return this.setOperator("<=").setTerm(v)
  }

  def isEqualTo(v: Any = null): Predicate = {
    return this.setOperator("=").setTerm(v)
  }

  def isIn(v: List[Any]): Predicate = {
    return this.setOperator("IN").setTerm(v)
  }

}