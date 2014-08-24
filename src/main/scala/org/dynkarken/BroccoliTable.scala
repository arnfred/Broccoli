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
    // putIfAbsent is Some(value) if key already exists
    val revOption = for (past <- head.putIfAbsent(key, value)) yield {
      snapshot(key, value)
    }
    // If key doesn't already exist, return headRev
    revOption.getOrElse(headRev)
  }

  // Fetches value stored at key. Returns None if key isn't set
  def get(key : K) : Option[V] = head.get(key)

  def get(key : K, rev : Revision) : Option[V] = {
    for (revMap <- revisions.get(rev); value <- revMap.get(key)) yield value
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


  // Currently this risks overwriting earlier revisions. TODO: Make revisions
  // unique by including a timestamp. Once we have several nodes though, we
  // can't rely on timestamp from each being identical. Then when we are later
  // looking up a revision on two nodes, they will use different revision
  // values based on their timestamps.
  private def snapshot(key : K, value : V) : Revision = {
    val rev = Revision(inverse.computeHash(value))
    head.update(key, value)
    val s : Map[K,V] = head.readOnlySnapshot()
    val oldRev = headRev
    s(key) match {
      // If snapshow contains value then compute revision and return
      case value => {
        revisions.put(rev, s)
        headRev = rev
        rev
      }
      // otherwise try again
      case _ => snapshot(key, value)
    }
  }
}
