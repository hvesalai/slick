package slick.test.stream

import scala.util.control.NonFatal

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import slick.jdbc.{H2Profile, JdbcProfile}


class JdbcPublisherTest extends RelationalPublisherTest[JdbcProfile](H2Profile, 1000L) {

  import profile.api.*


  def createDB = {
    val (db, _) = Database.forURL[IO]("jdbc:h2:mem:DatabasePublisherTest", driver = "org.h2.Driver", keepAliveConnection = true).allocated.unsafeRunSync()
    // Wait until the database has been initialized and can process queries:
    try {
      db.run(sql"SELECT 1".as[Int]).unsafeRunSync()
    } catch {
      case NonFatal(_) =>
    }
    db
  }
}
