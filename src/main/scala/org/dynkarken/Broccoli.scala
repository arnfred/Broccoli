package broccoli.core

import scala.collection.concurrent.TrieMap

case class Revision(key : String)

class Broccoli[K, V] {

  val ctrie : TrieMap[K,V] = TrieMap.empty


  // Inserts or updates value at position key. This operation is idempotent
  // A revision is returned if a value is updated. Otherwise None is returned
  def put(key : K, value : V) : Option[Revision] = {
    Some(Revision("blah"))
  }

  // Fetches value stored at key. Returns None if key isn't set
  def get(key : K, revision : Option[Revision]) : Option[V] = {
    None
  }

  // Update map by applying a function to the current value
  // Return None if key isn't set. This function is non idempotent
  def update(key : K, 
             valueFun : (V => V), 
             revision : Option[Revision]) : Option[(V, Revision)] = {
    None
  }


}
