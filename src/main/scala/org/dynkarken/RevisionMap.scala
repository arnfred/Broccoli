package broccoli.core

import scala.collection.concurrent.TrieMap
import scala.collection.Map
import scala.util.Random

case class Revision(key : Int)

case class Value[V](data : V) {
  val salt: Int  = Random.nextInt
}

class RevisionMap[K, V] {

  // The current revision
  var headRev : Revision = Revision(0)
  // The Triemap containing the most recent revision
  val head : TrieMap[K, Value[V]] = TrieMap.empty
  // A Triemap of snapshots taken for every destructive update
  val snapshots : TrieMap[Revision, Map[K, Value[V]]] = TrieMap.empty
  // A function for calculating the hash of a `Value`
  val valueHash : (Value[V] => Int) = TrieMap.empty.computeHash(_)
  // A Triemap that contains alternate snapshot names.
  val revAlias : TrieMap[Revision, Revision] = TrieMap.empty


  // Inserts data that is converted to a value automatically
  // TODO: update to return value?
  def put(key: K, data: V): (Value[V], Revision) = {
    val value = Value(data)
    (value, put(key, value))
  }

  // Creates a revision based on a value
  def getRevision(value : Value[V]) : Revision = {
    Revision(valueHash(value) + value.salt)
  }

  // Inserts or updates value at position key. This operation is idempotent
  // A revision is returned if a value is updated. Otherwise None is returned
  def put(key : K, value : Value[V]) : Revision = {
    var rev = getRevision(value)
    var currentRev = headRev
    var saved = false
    // `head.putIfAbsent` is `Some(value)` if key already exists
    for (past_val <- head.putIfAbsent(key, value)) {
      var past = past_val
      while (!saved) {
        // When the data is already present we just check that we have an up to
        // date `currentRev` before returning
        if (value.data == past.data) {
          if (currentRev == headRev) {
            saved = true
            rev = getRevision(past)
          }
          else {
            currentRev = headRev
            past = get(key, currentRev).get
          }
        }
        // only update if value isn't the current value
        else {
          overwrite(key, value, rev)
          saved = true
        }
      }
    }
    // We need to check that the revision isn't staked i.e. that another process
    // isn't in the progress of saving it and might already have made a snapshot
    // before `value` was added to the map.
    if (!saved) {
      if (snapshots.get(currentRev) != None) {
        overwrite(key, value, rev)
      }
      else {
        revAlias.put(rev, currentRev)
      }
    }
    rev
  }


  // Fetches value stored at key. Returns None if key isn't set
  def get(key : K) : Option[Value[V]] = head.get(key)


  def get(key : K, rev : Revision) : Option[Value[V]] = {
    // multiple snapshots point to the same snapshot
    val storedRev : Revision = revAlias.get(rev) match {
      case None => rev
      case Some(alias) => alias
    }
    // In the simple case this is an old revision
    if (storedRev != headRev) {
      for (revMap <- snapshots.get(storedRev);
           value <- revMap.get(key)) yield value
    }
    // Otherwise, we have to be careful that another thread won't overwrite the
    // value in a new revision concurrently with us fetching it
    else {
      val value = head.get(key)
      if (storedRev != headRev) snapshots(storedRev).get(key)
      else value
    }
  }


  def update(key : K, valueFun : (V => V)) : Option[Value[V]] = {
    for (past_val <- head.get(key)) yield {
      var past = past_val // We need a var
      var next = Value(valueFun(past.data))
      var i = 0
      // Loop until we managed to update the value
      while (!head.replace(key, past, next)) {
        past = head(key)
        next = Value(valueFun(past.data))
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
      val next = Value(valueFun(past.data))
      (next, put(key, next))
    }
  }


  /**
   * We guarantee that when `rev = put(key, val)` then `get(key, rev) == val`
   *
   * We have a `headRev` containing the current revision and a set of `snapshots`
   * containing all past snapshots. When a value is updated, we save `headRev`
   * to `snapshots` and proceed to update `head` afterwards
   *
   * To uphold the guarantee we need to careful with two scenarios:
   * 1. If there is a gap between saving a revision and updating the new value,
   *    then another process can update the same value and save our revision
   *    before we can add the value to it, violating the guarantee
   * 2. If we take a snapshot and a new value is added to the revision before
   *    we save the snapshot to `snapshots`, then the revision is saved without
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
  private def overwrite(key: K, value: Value[V], newRev: Revision): Unit = {
    // We cannot overwrite a revision, so if a thread has already taken a
    // snapshot with current value of headRev, we block until that thread
    // updates headRev
    var currentRev = headRev // MUST come before snapshot
    val empty : Map[K, Value[V]] = Map.empty
    snapshots.putIfAbsent(currentRev, empty)
    var currentSnapshot = head.readOnlySnapshot()
    while (!snapshots.replace(currentRev, empty, currentSnapshot)) {
      currentRev = headRev // MUST come before snapshot
      snapshots.putIfAbsent(currentRev, empty)
      currentSnapshot = head.readOnlySnapshot()
    }
    // The order here is important. The update must come before we reassign
    // headRev, since any other process is blocked in the snapshot loop until
    // headRev is reassigned
    head.update(key, value)
    headRev = newRev
  }
}
