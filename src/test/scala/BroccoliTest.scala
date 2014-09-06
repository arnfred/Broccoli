import concurrent.{Future, Await}
import scala.concurrent.duration._
import concurrent.ExecutionContext
import java.util.concurrent.Executors
import scala.util.{Success, Failure}
import scala.util.Random
import org.scalacheck._
import org.scalatest._
import broccoli.core._

class BroccoliSpec extends FlatSpec with Matchers {
  val executorService = Executors.newFixedThreadPool(4)
  implicit val executionContext = ExecutionContext.fromExecutorService(executorService)
  val amounts = List(523, 1134, 585, 594, 1482, 1242, 488, 1745, 1027, 1508)


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
    broc.get(0, revision_2).get.data should be (3)
    broc.get(0).get.data should be (3)
  }


  it should "apply all updates atomically" in {
    val broc = new BroccoliTable[Int, Int]
    def getFuture(n : Int) : Future[Int] = Future { 
      for (i <- 1 to n) {
        broc.update(0, { k => k + 1 })
      }
      n
    }
    broc.put(0,0) //  Initialize to 0
    val futures = Future.sequence(amounts.map(getFuture))
    Await.result(futures, 4 seconds)
    broc.get(0).get.data should be (amounts.sum)
  }


  // I want this test because it assures that the above test can fail
  // under the current system setup
  it should "verify that inc is done concurrently" in {
    val broc = new BroccoliTable[Int, Int]
    def getFuture(n : Int) : Future[Int] = Future { 
      Util.incBad(n, broc)
    }
    broc.put(0,0) //  Initialize to 0
    val futures = Future.sequence(amounts.map(getFuture))
    Await.result(futures, 4 seconds)
    broc.get(0).get.data should be < (amounts.sum)
  }


  it should """assure that for every `put`, the revision contains the value
  that was put in. (Across values)""" in {
    val broc = new BroccoliTable[Int, Int]
    def getFuture(n : Int) : Future[List[(Revision, Int)]] = Future { 
      (for (k <- 0 to n) yield { 
        (broc.put(0, k), k)
      }).toList
    }
    val futures = Future.sequence(amounts.map(getFuture))
    val results = Await.result(futures, 4 seconds)
    for (revs <- results; (rev, k) <- revs) {
      broc.get(0, rev).get.data should be (k)
    }
  }


  it should """assure that for every `put`, the revision contains the value
  that was put in. (Across keys)""" in {
    val broc = new BroccoliTable[Int, Int]
    def getFuture(n : Int) : Future[List[(Revision, Int)]] = Future { 
      (for (k <- 0 to n) yield { 
        (broc.put(k, 0), k)
      }).toList
    }
    val futures = Future.sequence(amounts.map(getFuture))
    val results = Await.result(futures, 4 seconds)
    for (revs <- results; (rev, k) <- revs) {
      broc.get(k, rev).get.data should be (0)
    }
  }


  it should """assure that for every `put`, the revision contains the values 
  that was put in. (Across values and keys)""" in {
    val broc = new BroccoliTable[Int, Int]
    def getFuture(n : Int) : Future[List[(Revision, Int, Int)]] = Future { 
      (for (k <- 0 to n) yield { 
        var value = Random.nextInt % 100
        var key = Random.nextInt % 500
        (broc.put(key, value), key, value)
      }).toList
    }
    val futures = Future.sequence(amounts.map(getFuture))
    val results = Await.result(futures, 4 seconds)
    for (revs <- results; (rev, k, v) <- revs) {
      broc.get(k, rev) match {
        case Some(value) => value.data should be (v)
        case None => fail(s"""k: $k, rev: $rev""")
      }
    }
  }


  it should """guarantee that `get` will retrieve the right value""" in {
    val broc = new BroccoliTable[Int, Int]
    def getFuture(n : Int) : Future[List[(Int, Option[Value[Int]])]] = Future { 
      (for (k <- 0 to n) yield { 
        val value = Random.nextInt % 100
        val key = Random.nextInt % 10
        val rev = broc.put(key, value)
        val ret = broc.get(key, rev)
        (value, ret)
      }).toList
    }
    val futures = Future.sequence(amounts.map(getFuture))
    val results = Await.result(futures, 4 seconds)
    for (revs <- results; (value, ret) <- revs) {
      ret match {
        case Some(retval) => value should be (retval.data)
        case None => fail(s"""value: $value wasn't stored""")
      }
    }
  }

}

object Util {

  def amounts(n : Int, loft : Int = 2500) : List[Int] = {
    for (k <- 1 to n) yield Math.abs(Random.nextInt % loft)
  } toList

  def incBad(n: Int, broc: BroccoliTable[Int, Int]): Int = {
    for (i <- 1 to n) {
      for (past_val <- broc.head.get(0)) yield {
        val next = past_val.data + 1
        broc.head.put(0, Value(next))
      }
    }
    n
  }
}
