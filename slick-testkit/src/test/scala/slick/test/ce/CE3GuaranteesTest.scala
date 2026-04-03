package slick.test.ce

import cats.effect.{Deferred, IO}
import cats.syntax.parallel.*
import munit.CatsEffectSuite

import slick.jdbc.H2Profile.api.*
import slick.jdbc.{DataSourceJdbcDataSource, DriverDataSource, JdbcBackend}

/** Tests for CE3-specific guarantees in the Slick 4 interpreter.
  *
  * Covers:
  *   - Fiber cancellation during a transaction triggers rollback
  *   - Connection-slot backpressure (Semaphore correctly limits concurrency)
  *   - Streaming with back-pressure (FS2 pull model)
  *   - `DBIO.from` / `DBIO.liftF` lifting CE3 effects into DBIO
  */
class CE3GuaranteesTest extends CatsEffectSuite {

  class Items(tag: Tag) extends Table[Int](tag, "ITEMS") {
    def v = column[Int]("V")
    def * = v
  }
  val items = TableQuery[Items]

  private def h2Source(name: String) = {
    val ds = new DriverDataSource(s"jdbc:h2:mem:$name;DB_CLOSE_DELAY=-1", driverClassName = "org.h2.Driver")
    new DataSourceJdbcDataSource(ds, keepAliveConnection = false, maxConnections = None)
  }

  /** A database with a single connection slot — easy to saturate for backpressure tests. */
  val singleSlotDb = ResourceFunFixture(
    JdbcBackend.Database.forSource[IO](h2Source("ce3test_single"), maxConnections = Some(1))
  )

  /** A database with several connection slots for concurrency tests. */
  val multiSlotDb = ResourceFunFixture(
    JdbcBackend.Database.forSource[IO](h2Source("ce3test_multi"), maxConnections = Some(4))
  )

  // ---------------------------------------------------------------------------
  // Fiber cancellation → rollback
  // ---------------------------------------------------------------------------

  singleSlotDb.test("fiber cancellation during transaction triggers rollback") { db =>
    for {
      _     <- db.run(items.schema.create)
      // A gate that lets us cancel the fiber at a known point inside the transaction
      gate  <- Deferred[IO, Unit]
      fiber <- db.run(
                 (for {
                   _ <- items += 42
                   // Lift an IO that signals readiness then blocks until cancelled
                   _ <- DBIO.from(gate.complete(()) >> IO.never[Unit])
                 } yield ()).transactionally
               ).start
      _     <- gate.get        // wait until the insert has run and IO.never is reached
      _     <- fiber.cancel    // cancel: should trigger rollback in the guaranteeCase finalizer
      _     <- fiber.join      // wait for finalizers to complete
      result <- db.run(items.result)
      _      = assertEquals(result, Vector.empty[Int], "rolled-back insert must not be visible")
      _     <- db.run(items.schema.drop)
    } yield ()
  }

  // ---------------------------------------------------------------------------
  // Connection-slot backpressure
  // ---------------------------------------------------------------------------

  singleSlotDb.test("semaphore serialises concurrent db.run calls when maxConnections = 1") { db =>
    // With maxConnections = 1 the second parTupled call must wait for the first to
    // release its connection.  Both should complete successfully with the same data.
    for {
      _ <- db.run(items.schema.create)
      _ <- db.run(items += 1)
      _ <- db.run(items += 2)
      result <- (db.run(items.result), db.run(items.result)).parTupled
      _  = assertEquals(result._1.toSet, Set(1, 2))
      _  = assertEquals(result._2.toSet, Set(1, 2))
      _ <- db.run(items.schema.drop)
    } yield ()
  }

  multiSlotDb.test("excess concurrent db.run calls are queued, not dropped") { db =>
    // With maxConnections = 4, launch 8 concurrent queries.  The 4 excess fibers
    // suspend on the semaphore and are served once a slot frees up.
    for {
      _ <- db.run(items.schema.create)
      _ <- db.run(DBIO.sequence((1 to 8).map(items += _)))
      results <- (1 to 8).toList.map(_ => db.run(items.result)).parSequence
      _  = assert(results.forall(_.size == 8), "every concurrent query must return all 8 rows")
      _ <- db.run(items.schema.drop)
    } yield ()
  }

  // ---------------------------------------------------------------------------
  // Streaming back-pressure
  // ---------------------------------------------------------------------------

  singleSlotDb.test("take(n) on a stream fetches only n rows (pull model)") { db =>
    for {
      _ <- db.run(items.schema.create)
      _ <- db.run(DBIO.sequence((1 to 10).map(items += _)))
      result <- db.stream(items.result).take(3).compile.toVector
      _  = assertEquals(result.size, 3)
      _ <- db.run(items.schema.drop)
    } yield ()
  }

  singleSlotDb.test("stream releases connection after full consumption") { db =>
    // If the connection is not released after the stream finishes, the next db.run
    // would deadlock forever on the single-slot semaphore.
    for {
      _ <- db.run(items.schema.create)
      _ <- db.run(DBIO.sequence((1 to 5).map(items += _)))
      _ <- db.stream(items.result).compile.drain
      result <- db.run(items.result)  // would deadlock if connection was leaked
      _  = assertEquals(result.size, 5)
      _ <- db.run(items.schema.drop)
    } yield ()
  }

  singleSlotDb.test("stream releases connection after early cancellation via take") { db =>
    for {
      _ <- db.run(items.schema.create)
      _ <- db.run(DBIO.sequence((1 to 5).map(items += _)))
      _ <- db.stream(items.result).take(2).compile.drain  // early termination
      result <- db.run(items.result)  // would deadlock if connection was leaked
      _  = assertEquals(result.size, 5)
      _ <- db.run(items.schema.drop)
    } yield ()
  }

  // ---------------------------------------------------------------------------
  // DBIO.from / DBIO.liftF lifting CE3 effects into DBIO
  // ---------------------------------------------------------------------------

  singleSlotDb.test("DBIO.from lifts an IO value into a DBIO action") { db =>
    for {
      _ <- db.run(items.schema.create)
      result <- db.run(for {
                  v <- DBIO.from(IO(42))
                  _ <- items += v
                  r <- items.result
                } yield r)
      _  = assertEquals(result, Vector(42))
      _ <- db.run(items.schema.drop)
    } yield ()
  }

  singleSlotDb.test("DBIO.liftF is an alias for DBIO.from") { db =>
    for {
      _ <- db.run(items.schema.create)
      result <- db.run(for {
                  v <- DBIO.liftF(IO(99))
                  _ <- items += v
                  r <- items.result
                } yield r)
      _  = assertEquals(result, Vector(99))
      _ <- db.run(items.schema.drop)
    } yield ()
  }

  singleSlotDb.test("DBIO.from failure propagates as DBIO failure") { db =>
    val boom = new RuntimeException("boom")
    db.run(DBIO.from(IO.raiseError[Int](boom))).attempt.map { result =>
      assert(result.isLeft)
      assertEquals(result.left.toOption.get.getMessage, "boom")
    }
  }

  singleSlotDb.test("DBIO.from IO error inside transactionally triggers rollback") { db =>
    for {
      _ <- db.run(items.schema.create)
      _ <- db.run(
             (items += 7).andThen(DBIO.from(IO.raiseError[Unit](new RuntimeException("fail"))))
               .transactionally
           ).attempt
      result <- db.run(items.result)
      _  = assertEquals(result, Vector.empty[Int], "IO error inside transactionally must roll back")
      _ <- db.run(items.schema.drop)
    } yield ()
  }
}
