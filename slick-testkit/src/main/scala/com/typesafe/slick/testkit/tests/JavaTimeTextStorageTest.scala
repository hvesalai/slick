package com.typesafe.slick.testkit.tests

import java.time._

import slick.jdbc._

import com.typesafe.slick.testkit.util.{AsyncTest, JdbcTestDB}

/** `javaTimeStorage = text`: the mappings of Slick 3 still create their old column types and round-trip values,
  * so that tables created by Slick 3 keep working. */
class JavaTimeTextStorageTest extends AsyncTest[JdbcTestDB] {

  /** The test database's profile with `javaTimeStorage = text` */
  lazy val textProfile: JdbcProfile = tdb.profile match {
    case _: H2Profile        => new H2Profile { override protected lazy val javaTimeStorage = "text" }
    case _: PostgresProfile  => new PostgresProfile { override protected lazy val javaTimeStorage = "text" }
    case _: MySQLProfile     => new MySQLProfile { override protected lazy val javaTimeStorage = "text" }
    case _: SQLServerProfile => new SQLServerProfile { override protected lazy val javaTimeStorage = "text" }
    case _: OracleProfile =>
      new OracleProfile with JdbcActionComponent.OneRowPerStatementOnly {
        override protected lazy val javaTimeStorage = "text"
      }
    case _: DB2Profile       => new DB2Profile { override protected lazy val javaTimeStorage = "text" }
    case _: DerbyProfile     => new DerbyProfile { override protected lazy val javaTimeStorage = "text" }
    case _: HsqldbProfile    => new HsqldbProfile { override protected lazy val javaTimeStorage = "text" }
    case _: SQLiteProfile    => new SQLiteProfile { override protected lazy val javaTimeStorage = "text" }
  }

  def testTextStorageRoundTrip = {
    val p = textProfile
    import p.api._

    class T(tag: Tag) extends Table[(Int, LocalDate, LocalTime, LocalDateTime, Instant, OffsetTime, OffsetDateTime,
      ZonedDateTime)](tag, "jt_text") {
      def id = column[Int]("id")
      def ld = column[LocalDate]("ld")
      def lt = column[LocalTime]("lt")
      def ldt = column[LocalDateTime]("ldt")
      def i = column[Instant]("i")
      def ot = column[OffsetTime]("ot")
      def odt = column[OffsetDateTime]("odt")
      def zdt = column[ZonedDateTime]("zdt")
      def * = (id, ld, lt, ldt, i, ot, odt, zdt)
    }
    val ts = TableQuery[T]
    // Millisecond values, outside DST transitions: the Slick 3 mappings round-trip these on every database
    val ldt = LocalDateTime.of(2024, 6, 15, 12, 34, 56, 789000000)
    val row = (1, ldt.toLocalDate, ldt.toLocalTime, ldt, Instant.parse("2024-06-15T12:34:56.789Z"),
      OffsetTime.of(ldt.toLocalTime, ZoneOffset.ofHoursMinutes(5, 30)),
      OffsetDateTime.of(ldt, ZoneOffset.ofHoursMinutes(5, 30)), ZonedDateTime.of(ldt, ZoneId.of("Europe/Helsinki")))

    db.run(seq(
      ts.schema.create,
      ts += row,
      ts.result.map(_ shouldBe Seq(row)),
      ts.filter(t => t.ld === row._2.bind && t.lt === row._3.bind && t.ldt === row._4.bind && t.i === row._5.bind &&
        t.ot === row._6.bind && t.odt === row._7.bind && t.zdt === row._8.bind).map(_.id).result
        .map(_ shouldBe Seq(1)),
      ts.schema.drop
    ))
  }
}
