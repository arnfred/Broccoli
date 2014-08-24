import concurrent.Future
import concurrent.duration.Duration
import concurrent.ExecutionContext
import java.util.concurrent.Executors
import org.scalacheck._
import org.scalatest._
import broccoli.core._

class BroccoliSpec extends FlatSpec with Matchers {
  val executorService = Executors.newFixedThreadPool(4)
  implicit val executionContext = ExecutionContext.fromExecutorService(executorService)

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

  it should "apply all updates atomically" in {
    val broc = new BroccoliTable[Int, Int]
    broc.put(0,0) //  Initialize to 0
    val f1 : Future[Int] = Future { Util.inc(200, broc) }
    val f2 : Future[Int] = Future { Util.inc(200, broc) }
    val f3 : Future[Int] = Future { Util.inc(200, broc) }
    val f4 : Future[Int] = Future { Util.inc(200, broc) }
    val all = f1.zip(f2).zip(f3).zip(f4)
    all.onComplete { _ =>
      broc.get(0).get should be < (400)
    }
  }

  it should "verify that inc is done concurrently" in {
    val broc = new BroccoliTable[Int, Int]
    broc.put(0,0) //  Initialize to 0
    val f1 : Future[Int] = Future { Util.incBad(200, broc) }
    val f2 : Future[Int] = Future { Util.incBad(200, broc) }
    val f3 : Future[Int] = Future { Util.incBad(200, broc) }
    val f4 : Future[Int] = Future { Util.incBad(200, broc) }
    val all = f1.zip(f2).zip(f3).zip(f4)
    all.onComplete { _ =>
      broc.get(0).get should be < (400)
    }
  }
}

object Util {
  def inc(n: Int, broc: BroccoliTable[Int, Int]): Int = {
    for (i <- 1 to n) {
      broc.update(0, { k => k + 1 })
      Thread.sleep(1)
    }
    n
  }

  def incBad(n: Int, broc: BroccoliTable[Int, Int]): Int = {
    for (i <- 1 to n) {
      for (past_val <- broc.head.get(0)) yield {
        val next = past_val + 1
        broc.head.put(0, next)
      }
      Thread.sleep(1)
    }
    n
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
