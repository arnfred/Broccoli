package broccoli.core

import scala.collection.concurrent.TrieMap
import scala.collection.Map
import scala.util.Random

case class Revision(key : Int)

case class Value[V](data : V, timestamp : Long, salt: Int)

class BroccoliTable[K, V] {

  var headRev : Revision = Revision(0)
  val head : TrieMap[K, Value[V]] = TrieMap.empty
  val revisions : TrieMap[Revision, Map[K, Value[V]]] = TrieMap.empty
  val inverse : TrieMap[Value[V], K] = TrieMap.empty


  // Inserts data that is converted to a value automatically
  // TODO: update to return value?
  def put(key: K, data: V): Revision = {
    put(key, Value(data, System.currentTimeMillis, Random.nextInt))
  }

  // Inserts or updates value at position key. This operation is idempotent
  // A revision is returned if a value is updated. Otherwise None is returned
  def put(key : K, value : Value[V]) : Revision = {
    val rev = Revision(inverse.computeHash(value))
    var currentRev = headRev
    var not_saved = true
    // `head.putIfAbsent` is Some(value) if key already exists
    for (past_val <- head.putIfAbsent(key, value)) {
      var past = past_val
      while (not_saved) {
        // only update if value isn't the current value
        if (value.data != past.data) {
          overwrite(key, value, rev)
          currentRev = rev
          not_saved = false
        }
        // It's possible that the value changed after we assigned `currentRev`
        // If `currentRev` is still equal to `headRev` or the value in the 
        // `currentRev` revision is equal to value then all is fine
        else if (currentRev == headRev || get(key, currentRev) == Some(past)) {
          not_saved = false
        }
        else { // Update `currentRev` and `past` and try again
          currentRev = headRev
          past = head(key)
        }
      }
    }
    // It is possible that `headRev` was updated after `currentRev` was
    // assigned and the call to `head.putIfAbsent` added the value to a later
    // revision. In this case we overwrite the value and return `rev`.
    if (not_saved && currentRev != headRev && get(key, currentRev).get.data != value.data) {
      overwrite(key, value, rev)
      currentRev = rev
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
      var next = Value(valueFun(past.data), past.timestamp, past.salt)
      var i = 0
      // Loop until we managed to update the value
      while (!head.replace(key, past, next) &&
             !head.replace(key, next, 
               Value(valueFun(next.data), next.timestamp, next.salt))) {
        past = head(key)
        next = Value(valueFun(past.data), past.timestamp, next.salt)
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
      val next = Value(valueFun(past.data), System.currentTimeMillis, past.salt)
      (next, put(key, next))
    }
  }


  // Overwrite already existing value
  private def overwrite(key: K, value: Value[V], newRev: Revision): Unit = {
    // We cannot overwrite a revision, so if a thread has already taken a
    // snapshot with current value of headRev, we block until that thread
    // updates headRev
    var currentRev = headRev
    var snapshot = head.readOnlySnapshot()
    while (revisions.putIfAbsent(currentRev, snapshot) != None) {
      currentRev = headRev
      snapshot = head.readOnlySnapshot()
    }
    // The order here is important. The update must come before we reassign
    // headRev, since any other process is blocked in the snapshot loop until
    // headRev is reassigned
    head.update(key, value)
    headRev = newRev
  }

}
