import org.scalacheck._
import org.scalatest._
import broccoli.core._

class BroccoliSpec extends FlatSpec with Matchers {

  "BroccoliTable" should "return the last value inserted for a given key" in {
    val broc = new BroccoliTable[Int, Int]
    broc.put(0,1)
    broc.put(0,2)
    broc.get(0) should be (Some(2))
  }

  it should "return None when a key isn't found" in {
    val broc = new BroccoliTable[Int, Int]
    broc.get(0) should be (None)
  }

  it should "save a revision for each destructive update" in {
    val broc = new BroccoliTable[Int, Int]
    val no_revision = broc.put(0,1)
    val revision_1 = broc.put(0,2)
    val revision_2 = broc.put(0,3)
    no_revision should be (Revision(0))
    broc.get(0, revision_1) should be (Some(2))
    broc.get(0, revision_2) should be (Some(3))
    broc.get(0) should be (Some(3))
  }
}

//object BroccoliSpecification extends Properties("Broccoli") {
//  import Prop.forAll
//
//  property("reverse") = forAll { l: List[String] => l.reverse.reverse == l }
//
//  property("concat") = forAll { (s1: String, s2: String) =>
//    (s1 + s2).endsWith(s2)
//  }
//
//}
