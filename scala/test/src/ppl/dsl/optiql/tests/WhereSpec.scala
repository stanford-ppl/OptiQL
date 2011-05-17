package ppl.dsl.optiql.tests

import org.scalatest.WordSpec
import ppl.dsl.optiql.baseline.OptiQL

class WhereSpec extends WordSpec {

  import OptiQL._

   "Where" should {

     "throw NullPointerException if it is invoked on a null source" in {
       val source: Iterable[Int] = null
       intercept[NullPointerException] {
         source Where(_ > 5)
       }
     }

     "throw IllegalArgumentExpetion if it is passed a null Predicate" in {
       val source = List(1, 3, 7, 9, 10);
       val pred: (Int) => Boolean = null
       intercept[IllegalArgumentException] {
         source Where(pred)
       }
     }
   }

  "Where with Index" should {
    "throw NullPointerException if it is invoked on a null source" in {
      val source: Iterable[Int] = null
      intercept[NullPointerException] {
       source Where((x, index) => x > 5)
      }
    }

    "throw IllegalArgumentExpetion if it is passed a null Predicate" in {
      val source = List(1, 3, 7, 9, 10);
      val pred: (Int, Int) => Boolean = null
      intercept[IllegalArgumentException] {
       source Where(pred)
      }

    }

  }

}