package ppl.dsl.optiql.baseline.ordering


class ReverseComparer[TElement](forwardComparer: Ordering[TElement]) extends Ordering[TElement] {
  def compare(x: TElement, y: TElement) = forwardComparer.compare(y,x)
}