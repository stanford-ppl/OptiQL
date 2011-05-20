package ppl.dsl.optiql.baseline

import containers.{Grouping, DataTable}
import collection.mutable.{ArrayBuffer, HashMap, Buffer}
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


}

class Queryable[TSource](source: Iterable[TSource]) {
  import OptiQL._

  def Where(predicate: TSource => Boolean) =  {
    if(predicate == null) throw new IllegalArgumentException("Predicate is Null")
    source.filter(predicate)
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

  def OrderBy[TKey](keySelector: TSource => TKey)(implicit comparer: Ordering[TKey]) = {
    new OrderedQueryable(source, new ProjectionComparer(keySelector))
  }

  def OrderByDescending[TKey](keySelector: TSource => TKey)(implicit comparer: Ordering[TKey]) = {
    new OrderedQueryable(source, new ReverseComparer(new ProjectionComparer(keySelector)))
  }



  def GroupBy[TKey](keySelector: TSource => TKey) = {
    println("constructing hash-table for grouping operation")
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
  def Join[TInner, TKey, TResult](inner: Iterable[TInner])(sourceKeySelector: TSource => TKey,
                                  innerKeySelector: TInner => TKey,
                                  resultSelector: (TSource,TInner) => TResult): Iterable[TResult] = {

    if(source.size < inner.size)
      hashJoin(source,sourceKeySelector, inner, innerKeySelector, resultSelector, true)
    else
      hashJoin(inner, innerKeySelector, source,sourceKeySelector, resultSelector, false)

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