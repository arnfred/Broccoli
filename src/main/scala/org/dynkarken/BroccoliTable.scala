package broccoli.core

import scala.collection.concurrent.TrieMap
import scala.collection.Map

case class Revision(key : Int)

class BroccoliTable[K, V] {

  val current : TrieMap[K, V] = TrieMap.empty
  val revisions : TrieMap[Revision, Map[K, V]] = TrieMap.empty
  val inverse : TrieMap[V, K] = TrieMap.empty


  // Inserts or updates value at position key. This operation is idempotent
  // A revision is returned if a value is updated. Otherwise None is returned
  def put(key : K, value : V) : Option[Revision] = {
    // Past is Some(value) if key already exists
    for (past <- current.putIfAbsent(key, value)) yield {
      var rev : Option[Revision] = None
      while (rev == None) {
        rev = snapshot(key, value)
      }
      rev.get
    }
  }

  // Fetches value stored at key. Returns None if key isn't set
  def get(key : K, revision : Option[Revision]) : Option[V] = revision match {
    case None       => current.get(key)
    case Some(rev)  => {
      for (revMap <- revisions.get(rev); value <- revMap.get(key)) yield value
    }
  }

  // Update map by applying a function to the current value
  // Return None if key isn't set. This function is not idempotent
  def update(key : K, valueFun : (V => V)) : Option[V] = {
    for (past_val <- current.get(key)) yield {
      var (past, next) = (past_val, valueFun(past_val))
      // Loop until we managed to update the value
      while (!current.replace(key, past, next)) {
        var past = current(key)
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
    for (past <- get(key, Some(revision));
         next <- Some(valueFun(past));
         rev <- put(key, next)) yield (next, rev)
  }


  // Currently this risks overwriting earlier revisions. TODO: Make revisions
  // unique by including a timestamp. Once we have several nodes though, we
  // can't rely on timestamp from each being identical. Then when we are later
  // looking up a revision on two nodes, they will use different revision
  // values based on their timestamps.
  private def snapshot(key : K, value : V) : Option[Revision] = {
    current.update(key, value)
    val s : Map[K,V] = current.readOnlySnapshot()
    if (s(key) == value) {
      val rev = Revision(inverse.computeHash(value))
      revisions.put(rev, s)
      Some(rev)
    } else {
      None
    }
  }

}
