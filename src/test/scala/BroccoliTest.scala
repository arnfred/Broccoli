import concurrent.{Future, Await}
import scala.concurrent.duration._
import concurrent.ExecutionContext
import java.util.concurrent.Executors
import scala.util.{Success, Failure}
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
    broc.get(0).get.data should be (2)
  }

  it should "return None when a key isn't found" in {
    val broc = new BroccoliTable[Int, Int]
    broc.get(0) should be (None)
  }

  it should "Be idempotent for put and updateRevision" in {
    val broc = new BroccoliTable[Int, Int]
    broc.put(0,1)
    val revision_1 = broc.put(0,1)
    val revision_2 = broc.put(0,1)
    revision_1 should be (revision_2)
    broc.get(0).get.data should be (1)
    val Some((_, revision_3)) = broc.updateRevision(0, (v => v + 1), revision_1)
    val Some((_, revision_4)) = broc.updateRevision(0, (v => v + 1), revision_1)
    val Some((_, revision_5)) = broc.updateRevision(0, (v => v + 1), revision_1)
    revision_3 should be (revision_5)
    broc.get(0).get.data should be (2)
  }

  it should "save a revision for each destructive update" in {
    val broc = new BroccoliTable[Int, Int]
    val no_revision = broc.put(0,1)
    val revision_1 = broc.put(0,2)
    val revision_2 = broc.put(0,3)
    no_revision should be (Revision(0))
    broc.get(0, revision_1).get.data should be (2)
    broc.get(0, revision_2).get.data should be (3)
    broc.get(0, revision_1).get.timestamp should be <= broc.get(0, revision_2).get.timestamp
    broc.get(0, revision_2).get.data should be (3)
    broc.get(0).get.data should be (3)
  }

  it should "apply all updates atomically" in {
    val broc = new BroccoliTable[Int, Int]
    broc.put(0,0) //  Initialize to 0
    val f1 : Future[Int] = Future { Util.inc(2000, broc) }
    val f2 : Future[Int] = Future { Util.inc(2000, broc) }
    val f3 : Future[Int] = Future { Util.inc(2000, broc) }
    val f4 : Future[Int] = Future { Util.inc(2000, broc) }
    val all = f1.zip(f2).zip(f3).zip(f4)
    Await.result(all, 4 seconds)
    broc.get(0).get.data should be (8000)
  }

  it should "verify that inc is done concurrently" in {
    val broc = new BroccoliTable[Int, Int]
    broc.put(0,0) //  Initialize to 0
    val f1 : Future[Int] = Future { Util.incBad(2000, broc) }
    val f2 : Future[Int] = Future { Util.incBad(2000, broc) }
    val f3 : Future[Int] = Future { Util.incBad(2000, broc) }
    val f4 : Future[Int] = Future { Util.incBad(2000, broc) }
    val all = f1.zip(f2).zip(f3).zip(f4)
    Await.result(all, 4 seconds)
    broc.get(0).get.data should be < (8000)
  }

  it should "return a revision that contains the value passed" in {
    val broc = new BroccoliTable[Int, Int]
    val f1 : Future[List[(Revision, Int)]] = Future { 
      (for (k <- 0 to 2000) yield { 
        (broc.put(0, k), k)
      }).toList
    }
    val f5 : Future[List[(Revision, Int)]] = Future { 
      (for (k <- 0 to 2000) yield { 
        (broc.put(0, k*5), k)
      }).toList
    }
    val all : Future[(List[(Revision, Int)],List[(Revision, Int)])] = f1.zip(f5)
    // Wait for things to calm down
    val (rev_f1s, rev_f5s) = Await.result(all, 4 seconds)
    for ((rev,i) <- rev_f1s) {
      broc.get(0, rev).get.data should be (i)
    }
    for ((rev,i) <- rev_f5s) {
      broc.get(0, rev).get.data should be (i*5)
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
        val next = past_val.data + 1
        broc.head.put(0, Value(next, System.currentTimeMillis))
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
