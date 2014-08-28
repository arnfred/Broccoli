package broccoli.core

import scala.collection.concurrent.TrieMap
import scala.collection.Map
import scala.util.Random

case class Revision(key : Int)

case class Value[V](data : V, timestamp : Long)

class BroccoliTable[K, V] {

  var headRev : Revision = Revision(0)
  val head : TrieMap[K, Value[V]] = TrieMap.empty
  val revisions : TrieMap[Revision, Map[K, Value[V]]] = TrieMap.empty
  val inverse : TrieMap[Value[V], K] = TrieMap.empty


  // Inserts data that is converted to a value automatically
  def put(key: K, data: V): Revision = {
    put(key, Value(data, System.currentTimeMillis))
  }

  // Inserts or updates value at position key. This operation is idempotent
  // A revision is returned if a value is updated. Otherwise None is returned
  def put(key : K, value : Value[V]) : Revision = {
    val rev = Revision(inverse.computeHash(value))
    var currentRev = headRev
    // putIfAbsent is Some(value) if key already exists
    for (past <- head.putIfAbsent(key, value)) {
      // only update if value isn't the current value
      if (value.data != past.data) {
        var snapshot = head.readOnlySnapshot()
        while (revisions.putIfAbsent(currentRev, snapshot) != None) {
          currentRev = headRev
          snapshot = head.readOnlySnapshot()
        }
        headRev = rev
        head.update(key, value)
        currentRev = rev
      }
    }
    currentRev
  }

  // Fetches value stored at key. Returns None if key isn't set
  def get(key : K) : Option[Value[V]] = {
    for (value <- head.get(key)) yield value
  }

  def get(key : K, rev : Revision) : Option[Value[V]] = {
    // In the simple case this is an old revision
    if (rev != headRev) {
      for (revMap <- revisions.get(rev);
           value <- revMap.get(key)) yield value
    }
    // Otherwise, we have to be careful that we don't overwrite the value
    // in a new revision concurrently with fetching it
    else {
      val value = for (value <- head.get(key)) yield value
      if (rev != headRev) {
        for (value <- revisions(rev).get(key)) yield value
      }
      else value
    }
  }

  // Update map by applying a function to the current value
  // Return None if key isn't set. This function is not idempotent
  def update(key : K, valueFun : (V => V)) : Option[Value[V]] = {
    for (past_val <- head.get(key)) yield {
      var past = past_val // We need a var
      var next = Value(valueFun(past.data), past.timestamp)
      var i = 0
      // Loop until we managed to update the value
      while (!head.replace(key, past, next) &&
             !head.replace(key, next, Value(valueFun(next.data), next.timestamp))) {
        past = head(key)
        next = Value(valueFun(past.data), past.timestamp)
      }
      next
    }
  }

  // Update map by applying a function to an old value
  // Return None if key isn't set. This function is idempotent
  def updateRevision(key : K,
                     valueFun : (V => V),
                     revision : Revision) : Option[(Value[V], Revision)] = {
    for (past <- get(key, revision)) yield {
      val next = Value(valueFun(past.data), System.currentTimeMillis)
      (next, put(key, next))
    }
  }
}
