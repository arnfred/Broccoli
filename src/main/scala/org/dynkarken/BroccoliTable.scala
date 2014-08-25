package broccoli.core

import scala.collection.concurrent.TrieMap
import scala.collection.Map

case class Revision(key : Int)

class BroccoliTable[K, V] {

  var headRev : Revision = Revision(0)
  val head : TrieMap[K, V] = TrieMap.empty
  val revisions : TrieMap[Revision, Map[K, V]] = TrieMap.empty
  val inverse : TrieMap[V, K] = TrieMap.empty


  // Inserts or updates value at position key. This operation is idempotent
  // A revision is returned if a value is updated. Otherwise None is returned
  def put(key : K, value : V) : Revision = {
    val rev = Revision(inverse.computeHash(value))
    var currentRev = headRev
    // putIfAbsent is Some(value) if key already exists
    for (past <- head.putIfAbsent(key, value)) {
      // only update if value isn't the current value
      if (value != past) {
        var snapshot = head.readOnlySnapshot()
        while (revisions.putIfAbsent(currentRev, snapshot) != None) {
          currentRev = headRev
          snapshot = head.readOnlySnapshot()
        }
        head.update(key, value)
        headRev = rev
        currentRev = rev
      }
    }
    currentRev
  }

  // Fetches value stored at key. Returns None if key isn't set
  def get(key : K) : Option[V] = head.get(key)

  def get(key : K, rev : Revision) : Option[V] = {
    // In the simple case this is an old revision
    if (rev != headRev) {
      for (revMap <- revisions.get(rev); value <- revMap.get(key)) yield value
    }
    // Otherwise, we have to be careful that we don't overwrite the value
    // in a new revision concurrently with fetching it
    else {
      val result = head.get(key)
      if (rev != headRev) revisions(rev).get(key)
      else result
    }
  }

  // Update map by applying a function to the current value
  // Return None if key isn't set. This function is not idempotent
  def update(key : K, valueFun : (V => V)) : Option[V] = {
    for (past_val <- head.get(key)) yield {
      var (past, next) = (past_val, valueFun(past_val))
      // Loop until we managed to update the value
      while (!head.replace(key, past, next)) {
        var past = head(key)
        var next = valueFun(past)
      }
      next
    }
  }

  // Update map by applying a function to an old value
  // Return None if key isn't set. This function is idempotent
  def updateRevision(key : K,
                     valueFun : (V => V),
                     revision : Revision) : Option[(V, Revision)] = {
    for (past <- get(key, revision)) yield {
      val next = valueFun(past)
      (next, put(key, next))
    }
  }
}
