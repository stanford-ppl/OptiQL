package ppl.dsl.optiql.baseline

import containers.{Grouping, DataTable}
import collection.mutable.{ArrayBuffer, HashMap}
import java.lang.{RuntimeException}
import ordering.{ReverseComparer, ProjectionComparer, OrderedQueryable}
import util.DateOrdering

object OptiQL {

  implicit def convertIterableToQueryable[T](i: Iterable[T]) = new Queryable[T](i)
  implicit def convertIterableToDataTable[T](i: Iterable[T]) : DataTable[T] = {
    if(i.isInstanceOf[DataTable[T]]) {
      i.asInstanceOf[DataTable[T]]
    }
    else if(i.isInstanceOf[ArrayBuffer[T]]) {

      return new DataTable[T] {

        data = i.asInstanceOf[ArrayBuffer[T]]

        def addRecord(arr: Array[String]) {
          throw new RuntimeException("Cannot add Record into a projected DataTable")
        }
      }

    }
    else {
      val arrbuf = new ArrayBuffer[T]();
      for (e <- i) {
        arrbuf.append(e)
      }
      return new DataTable[T] {

        data = arrbuf

        def addRecord(arr: Array[String]) {
          throw new RuntimeException("Cannot add Record into a projected DataTable")
        }
      }
      //throw new RuntimeException("Could not convert iterable to DataTable")
    }
  }


  implicit val dateOrdering = new DateOrdering

  class Overloaded1
  class Overloaded2
  class Overloaded3
  class Overloaded4
  implicit val overloaded1 = new Overloaded1
  implicit val overloaded2 = new Overloaded2
  implicit val overloaded3 = new Overloaded3
  implicit val overloaded4 = new Overloaded4


}

class Queryable[TSource](source: Iterable[TSource]) {
  import OptiQL._

  def Where(predicate: TSource => Boolean) =  {
    if(predicate == null) throw new IllegalArgumentException("Predicate is Null")
    val res = new DataTable[TSource] {
      def addRecord(fields: Array[String]) = throw new RuntimeException("Cannot add records to filtered table")
    }
    val resData = res.data
    var idx = 0
    val size = source.size
    val data = source.data
    while(idx < size) {
      val candidate = data(idx)
      if(predicate(candidate))
        resData append candidate
      idx += 1
    }
    res
  }

  def Where(predicate: (TSource, Int) => Boolean) = {
    if(predicate == null) throw new IllegalArgumentException("Predicate is Null")
    val res = new ArrayBuffer[TSource]
    res.sizeHint(source.size/2)
    var i = 0
    for(element <- source) {
      if(predicate(element,i))
        res.append(element)
      i += 1
    }
    res
  }

  def Select[TResult](selector: TSource => TResult) = {
    source.map(selector)
  }

  def Count = source.size

  def Count(predicate: TSource => Boolean) = source.filter(predicate).size

  def Exists() = source.size > 0

  def Exists(predicate: TSource => Boolean) = source.filter(predicate).size > 0

  def OrderBy[TKey](keySelector: TSource => TKey)(implicit comparer: Ordering[TKey]) = {
    new OrderedQueryable(source, new ProjectionComparer(keySelector))
  }

  def OrderByDescending[TKey](keySelector: TSource => TKey)(implicit comparer: Ordering[TKey]) = {
    new OrderedQueryable(source, new ReverseComparer(new ProjectionComparer(keySelector)))
  }



  def GroupBy[TKey](keySelector: TSource => TKey) = {
    //println("constructing hash-table for grouping operation")
    val (hTable, keys) = buildHash(source,keySelector)
    val result = new DataTable[Grouping[TKey,TSource]] {
      def addRecord(fields: Array[String]) = throw new RuntimeException("Cannot add records to a grouping table")
      override val grouped = true
    }
    for(key <- keys) {
      result.data += new Grouping(key,hTable.getOrElse(key, new ArrayBuffer[TSource]))
    }
    result
  }

  //TODO: Hash join implementation, maybe there is a faster way
  def OldJoin[TInner, TKey, TResult](inner: Iterable[TInner])(sourceKeySelector: TSource => TKey,
                                  innerKeySelector: TInner => TKey,
                                  resultSelector: (TSource,TInner) => TResult): Iterable[TResult] = {

    if(source.size < inner.size)
      hashJoin(source,sourceKeySelector, inner, innerKeySelector, resultSelector, true)
    else
      hashJoin(inner, innerKeySelector, source,sourceKeySelector, resultSelector, false)

  }

  //MultiWay Joins
  def Join[TSecond](second: Iterable[TSecond]) = new JoinableOps(second)

  class JoinableOps[TSecond](second: Iterable[TSecond]) {
    def Where(predicate: TSecond => Boolean) = new JoinableOps(second.Where(predicate))
    def WhereEq[TKey1](firstKeySelector: TSource => TKey1,secondKeySelector: TSecond => TKey1) = new Joinable2(source, second, firstKeySelector, secondKeySelector)
  }

  class Joinable2[TFirst, TSecond, TKey2](val first: Iterable[TFirst],
                                          val second: Iterable[TSecond],
                                          val firstKeySelector: TFirst => TKey2,
                                          val secondKeySelector: TSecond => TKey2) {
    def With[TThird](third: Iterable[TThird]) = new Joinable2Ops(third)
    class Joinable2Ops[TThird](third: Iterable[TThird]) {
      def Where(predicate: TThird => Boolean) = new Joinable2Ops(third.Where(predicate))
      def WhereEq[TKey3](firstKeySelector: TFirst => TKey3, thirdKeySelector: TThird => TKey3) = new Joinable3(Joinable2.this, first, third, firstKeySelector, thirdKeySelector)
      def WhereEq[TKey3](secondKeySelector: TSecond => TKey3,thirdKeySelector: TThird => TKey3)(implicit overloaded: Overloaded1) = new Joinable3(Joinable2.this, second, third, secondKeySelector, thirdKeySelector)
    }
    def preJoin():(ArrayBuffer[TFirst], ArrayBuffer[TSecond]) = {
      //join first two


      val (hashTable, _) = buildHash(first, firstKeySelector)
      val firsts = new ArrayBuffer[TFirst]
      val seconds = new ArrayBuffer[TSecond]

      for(row <- second) {
        val tryjoin = hashTable.getOrElse(secondKeySelector(row), null)
        if(tryjoin != null)
          for(e <- tryjoin) {
            firsts.append(e)
            seconds.append(row)
          }
      }
      (firsts, seconds)
    }
    def Select[TResult](resultSelector: (TFirst, TSecond) => TResult) = {
      val (firsts, seconds) = preJoin()
      val result = new DataTable[TResult] {
        def addRecord(fields: Array[String]) = throw new RuntimeException("Cannot add records to joined table")
      }
      var idx = 0
      while(idx < firsts.size) {
        result.data.append(resultSelector(firsts(idx), seconds(idx)))
        idx += 1
      }
      result
    }
  }

  class Joinable3[TFirst, TSecond, TRJoined3, TThird, TKey2, TKey3](
    val join2: Joinable2[TFirst, TSecond, TKey2],
    val rejoined: Iterable[TRJoined3],
    val third: Iterable[TThird],
    val rejoinedKeySelector: TRJoined3 => TKey3,
    val thirdKeySelector: TThird => TKey3) {
    def With[TFourth](fourth: Iterable[TFourth]) = new Joinable3Ops(fourth)
    class Joinable3Ops[TFourth](fourth: Iterable[TFourth]) {
      def Where(predicate: TFourth => Boolean) = new Joinable3Ops(fourth.Where(predicate))
      def WhereEq[TKey4](firstKeySelector: TFirst => TKey4, fourthKeySelector: TFourth => TKey4) = new Joinable4(Joinable3.this, join2.first, fourth, firstKeySelector, fourthKeySelector)
      def WhereEq[TKey4](secondKeySelector: TSecond => TKey4,fourthKeySelector: TFourth => TKey4)(implicit overloaded: Overloaded1) = new Joinable4(Joinable3.this, join2.second, fourth, secondKeySelector, fourthKeySelector)
      def WhereEq[TKey4](thirdKeySelector: TThird => TKey4,fourthKeySelector: TFourth => TKey4)(implicit overloaded: Overloaded2) = new Joinable4(Joinable3.this, third, fourth, thirdKeySelector, fourthKeySelector)
    }
    def preJoin():(ArrayBuffer[TFirst], ArrayBuffer[TSecond], ArrayBuffer[TThird]) = {
      val (pfirsts, pseconds) = join2.preJoin()
      //join next set
      val (hashTable, keys) = buildHash(third, thirdKeySelector)
      val firsts = new ArrayBuffer[TFirst](pfirsts.length)
      val seconds = new ArrayBuffer[TSecond](pseconds.length)
      val thirds = new ArrayBuffer[TThird]
      val scanned = (rejoined match {
        case join2.first => pfirsts
        case join2.second => pseconds
      }).asInstanceOf[ArrayBuffer[TRJoined3]]
      var idx = 0;
      for(row <- scanned) {
        val tryjoin = hashTable.getOrElse(rejoinedKeySelector(row), null)
        if(tryjoin != null
        )
          for(e <- tryjoin) {
            firsts.append(pfirsts(idx))
            seconds.append(pseconds(idx))
            thirds.append(e)
          }
        idx += 1
      }
      (firsts, seconds, thirds)
    }
    def Select[TResult](resultSelector: (TFirst, TSecond, TThird) => TResult) = {
      val (firsts, seconds, thirds) = preJoin()
      val result = new DataTable[TResult] {
        def addRecord(fields: Array[String]) = throw new RuntimeException("Cannot add records to joined table")
      }
      var idx = 0
      while(idx < firsts.size) {
        result.data.append(resultSelector(firsts(idx), seconds(idx), thirds(idx)))
        idx += 1
      }
      result
    }
  }


  class Joinable4[TFirst, TSecond, TRJoined3, TThird, TKey2, TKey3, TRJoined4, TFourth, TKey4](
    val join3: Joinable3[TFirst, TSecond, TRJoined3, TThird, TKey2, TKey3],
    val rejoined: Iterable[TRJoined4],
    val fourth: Iterable[TFourth],
    val rejoinedKeySelector: TRJoined4 => TKey4,
    val fourthKeySelector: TFourth => TKey4) {

    def With[TFifth](fifth: Iterable[TFifth]) = new Joinable4Ops(fifth)
    class Joinable4Ops[TFifth](fifth: Iterable[TFifth]) {
      def Where(predicate: TFifth=> Boolean) = new Joinable4Ops(fifth.Where(predicate))
      def WhereEq[TKey5](firstKeySelector: TFirst => TKey5, fifthKeySelector: TFifth => TKey5) = new Joinable5(Joinable4.this, join3.join2.first, fifth, firstKeySelector, fifthKeySelector)
      def WhereEq[TKey5](secondKeySelector: TSecond => TKey5,fifthKeySelector: TFifth => TKey5)(implicit overloaded: Overloaded1) = new Joinable5(Joinable4.this, join3.join2.second, fifth, secondKeySelector, fifthKeySelector)
      def WhereEq[TKey5](thirdKeySelector: TThird => TKey5,fifthKeySelector: TFifth => TKey5)(implicit overloaded: Overloaded2) = new Joinable5(Joinable4.this, join3.third, fifth, thirdKeySelector, fifthKeySelector)
      def WhereEq[TKey5](fourthKeySelector: TFourth => TKey5,fifthKeySelector: TFifth => TKey5)(implicit overloaded: Overloaded3) = new Joinable5(Joinable4.this, fourth, fifth, fourthKeySelector, fifthKeySelector)
    }

    def preJoin():(ArrayBuffer[TFirst], ArrayBuffer[TSecond], ArrayBuffer[TThird], ArrayBuffer[TFourth]) = {
      val (pfirsts, pseconds, pthirds) = join3.preJoin()
      //join next set
      val (hashTable, _) = buildHash(fourth, fourthKeySelector)
      val firsts = new ArrayBuffer[TFirst](pfirsts.length)
      val seconds = new ArrayBuffer[TSecond](pseconds.length)
      val thirds = new ArrayBuffer[TThird](pthirds.length)
      val fourths = new ArrayBuffer[TFourth]
      val scanned = (rejoined match {
        case join3.join2.first => pfirsts
        case join3.join2.second => pseconds
        case join3.third => pthirds
      }).asInstanceOf[ArrayBuffer[TRJoined4]]
      var idx = 0;
      for(row <- scanned) {
        val tryjoin = hashTable.getOrElse(rejoinedKeySelector(row), null)
        if(tryjoin != null
        )
          for(e <- tryjoin) {
            firsts.append(pfirsts(idx))
            seconds.append(pseconds(idx))
            thirds.append(pthirds(idx))
            fourths.append(e)
          }
        idx += 1
      }
      (firsts, seconds, thirds, fourths)
    }
    def Select[TResult](resultSelector: (TFirst, TSecond, TThird, TFourth) => TResult) = {
      val (firsts, seconds, thirds, fourths) = preJoin()
      val result = new DataTable[TResult] {
        def addRecord(fields: Array[String]) = throw new RuntimeException("Cannot add records to joined table")
      }
      var idx = 0
      while(idx < firsts.size) {
        result.data.append(resultSelector(firsts(idx), seconds(idx), thirds(idx), fourths(idx)))
        idx += 1
      }
      result
    }


  }

  class Joinable5[TFirst, TSecond, TRJoined3, TThird, TKey2, TKey3, TRJoined4, TFourth, TKey4, TRJoined5, TFifth, TKey5](
    val join4: Joinable4[TFirst, TSecond, TRJoined3, TThird, TKey2, TKey3, TRJoined4, TFourth, TKey4],
    val rejoined: Iterable[TRJoined5],
    val fifth: Iterable[TFifth],
    val rejoinedKeySelector: TRJoined5 => TKey5,
    val fifthKeySelector: TFifth => TKey5) {

    def preJoin():(ArrayBuffer[TFirst], ArrayBuffer[TSecond], ArrayBuffer[TThird], ArrayBuffer[TFourth], ArrayBuffer[TFifth]) = {
      val (pfirsts, pseconds, pthirds, pfourths) = join4.preJoin()
      //join next set
      val (hashTable, _) = buildHash(fifth, fifthKeySelector)
      val firsts = new ArrayBuffer[TFirst](pfirsts.length)
      val seconds = new ArrayBuffer[TSecond](pseconds.length)
      val thirds = new ArrayBuffer[TThird](pthirds.length)
      val fourths = new ArrayBuffer[TFourth](pfourths.length)
      val fifths = new ArrayBuffer[TFifth]
      val scanned = (rejoined match {
        case join4.join3.join2.first => pfirsts
        case join4.join3.join2.second => pseconds
        case join4.join3.third => pthirds
        case join4.fourth => pfourths
      }).asInstanceOf[ArrayBuffer[TRJoined5]]
      var idx = 0;
      for(row <- scanned) {
        val tryjoin = hashTable.getOrElse(rejoinedKeySelector(row), null)
        if(tryjoin != null
        )
          for(e <- tryjoin) {
            firsts.append(pfirsts(idx))
            seconds.append(pseconds(idx))
            thirds.append(pthirds(idx))
            fourths.append(pfourths(idx))
            fifths.append(e)
          }
        idx += 1
      }
      (firsts, seconds, thirds, fourths, fifths)
    }
    def Select[TResult](resultSelector: (TFirst, TSecond, TThird, TFourth, TFifth) => TResult) = {
      val (firsts, seconds, thirds, fourths, fifths) = preJoin()
      val result = new DataTable[TResult] {
        def addRecord(fields: Array[String]) = throw new RuntimeException("Cannot add records to joined table")
      }
      var idx = 0
      while(idx < firsts.size) {
        result.data.append(resultSelector(firsts(idx), seconds(idx), thirds(idx), fourths(idx), fifths(idx)))
        idx += 1
      }
      result
    }


  }


  private def hashJoin[THashed, TScanned, TKey, TResult, TInner](hashed: Iterable[THashed],
                                                           hashedSelect: THashed => TKey,
                                                           scanned: Iterable[TScanned],
                                                           scannedSelect: TScanned => TKey,
                                                           resultSelect: (TSource, TInner) => TResult,
                                                           hashedFirst: Boolean) = {

    //println("constructing hash-table for smaller relation")
    val (hashTable, keys) = buildHash(hashed, hashedSelect)

    val result = new DataTable[TResult] {
      def addRecord(fields: Array[String]) = throw new RuntimeException("Cannot add records to joined table")
    }

    //println("scanning through larger relation")
    for(row <- scanned) {
      val joinables = hashTable.getOrElse(scannedSelect(row),new ArrayBuffer[THashed])
      val joined =
        if(hashedFirst)
          joinables.map(h => {
            resultSelect(h.asInstanceOf[TSource], row.asInstanceOf[TInner])
          })
        else
          joinables.map(h => {
            resultSelect(row.asInstanceOf[TSource], h.asInstanceOf[TInner])
          })


      result.data.appendAll(joined)
    }

    result
  }


  def Min[@specialized T:Numeric, TS](selector: TSource => T): TSource = {
    val n = implicitly[Numeric[T]]
    import n._
    if(source.data.size == 0) return null.asInstanceOf[TSource]
    var min = source.data(0)
    for(e <- source) {
      if(selector(min) > selector(e))
        min = e
    }
    min
  }


  def Max[@specialized T:Numeric, TS](selector: TSource => T): TSource = {
    val n = implicitly[Numeric[T]]
    import n._
    if(source.data.size == 0) return null.asInstanceOf[TSource]
    var max = source.data(0)
    for(e <- source) {
      if(selector(max) < selector(e))
        max = e
    }
    max
  }


  def Sum[@specialized T:Numeric](selector: TSource => T): T = {
    val n = implicitly[Numeric[T]]
    import n._
    var sum = n.zero
    for(e <- source) {
      sum += selector(e)
    }
    sum
  }


  def Average[@specialized T:Numeric](selector: TSource => T): Float = {
    val n = implicitly[Numeric[T]]
    import n._
    Sum(selector).toFloat/Count
  }


  /*****
   * Internal Implementation functions
   */
  private def buildHash[TSource,TKey](source:Iterable[TSource], keySelector: TSource => TKey) = {
    val hash = HashMap[TKey, ArrayBuffer[TSource]]()
    val keys = new ArrayBuffer[TKey]
    for (elem <- source; key = keySelector(elem)) {
      hash.getOrElseUpdate(key,{
        keys.append(key)
        new ArrayBuffer[TSource]() //if there is no key
      }) += elem
    }
    (hash,keys)
  }

  private def ni = throw new RuntimeException("Not Implemented")

}