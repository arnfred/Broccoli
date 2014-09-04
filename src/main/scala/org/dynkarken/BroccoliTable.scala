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
    var saved = false
    //println(s"""Adding value: $value under key: $key\nwith rev: $rev and headRev/currentRev: $headRev""")
    // `head.putIfAbsent` is Some(value) if key already exists
    val savedRev = for (past_val <- head.putIfAbsent(key, value)) yield {
      //println(s"""Key already exists as `$past_val`, updating...""")
      var past = past_val
      while (!saved) {
        // When the data is already present we just check that we have an up to
        // date `currentRev` before returning
        if (value.data == past.data) {
          if (currentRev == headRev) saved = true
          else {
            currentRev = headRev
            past = get(key, currentRev).get
          }
        }
        // only update if value isn't the current value
        else {
          overwrite(key, value, rev)
          currentRev = rev
          saved = true
        }
      }
      currentRev
    }
    // We need to check that the revision is in order
    if (savedRev == None) {
      if (revisions.get(currentRev) != None &&
          get(key, currentRev).map(_.data) != Some(value.data)) {
        currentRev = put(key, value)
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
    // Otherwise, we have to be careful that another thread won't overwrite the
    // value in a new revision concurrently with fetching it
    else {
      val value = head.get(key)
      if (rev != headRev) revisions(rev).get(key)
      else value
    }
  }


  /**
   * We guarantee that when `rev = put(key, val)` then `get(key, rev) == val`
   *
   * We have a `headRev` containing the current revision and a set of `revisions`
   * containing all past revisions. When a value is updated, we save `headRev`
   * to `revisions` and proceed to update `head` afterwards
   *
   * To uphold the guarantee we need to careful with two scenarios:
   * 1. If there is a gap between saving a revision and updating the new value,
   *    then another process can update the same value and save our revision
   *    before we can add the value to it, violating the guarantee
   * 2. If we take a snapshot and a new value is added to the revision before
   *    we save the snapshot to `revisions`, then the revision is saved without
   *    the new value
   *
   * To protect against #1 we never overwrite a revision that is saved, but
   * instead waits for `headRev` to be updated before saving
   *
   * To protect against #2 we always stake a claim for a revision by adding an
   * empty revision before creating the snapshot and overwriting it. This makes
   * it possible to check in `put` if the revision has already been staked, in
   * which case we add the value under a new revision
   */
  def update(key : K, valueFun : (V => V)) : Option[Value[V]] = {
    for (past_val <- head.get(key)) yield {
      var past = past_val // We need a var
      var next = Value(valueFun(past.data), past.timestamp, past.salt)
      var i = 0
      // Loop until we managed to update the value
      while (!head.replace(key, past, next) &&
             !head.replace(key, next, 
               Value(valueFun(next.data), past.timestamp, past.salt))) {
        past = head(key)
        next = Value(valueFun(past.data), past.timestamp, past.salt)
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
      val next = Value(valueFun(past.data), System.currentTimeMillis, Random.nextInt)
      (next, put(key, next))
    }
  }


  // Overwrite already existing value
  private def overwrite(key: K, value: Value[V], newRev: Revision): Unit = {
    // We cannot overwrite a revision, so if a thread has already taken a
    // snapshot with current value of headRev, we block until that thread
    // updates headRev
    var currentRev = headRev // MUST come before snapshot
    val empty : Map[K, Value[V]] = Map.empty
    revisions.putIfAbsent(currentRev, empty)
    var snapshot = head.readOnlySnapshot()
    while (!revisions.replace(currentRev, empty, snapshot)) {
      currentRev = headRev // MUST come before snapshot
      revisions.putIfAbsent(currentRev, empty)
      snapshot = head.readOnlySnapshot()
    }
    // The order here is important. The update must come before we reassign
    // headRev, since any other process is blocked in the snapshot loop until
    // headRev is reassigned
    head.update(key, value)
    headRev = newRev
  }

}
