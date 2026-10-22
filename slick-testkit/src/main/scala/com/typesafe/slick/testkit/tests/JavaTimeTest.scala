package com.typesafe.slick.testkit.tests

import java.time._

import slick.jdbc._

import com.typesafe.slick.testkit.util.{AsyncTest, JdbcTestDB}

/** The native java.time mappings: full-precision round trips, comparisons and ordering (offset values compare as
  * instants), values in the Europe/Helsinki DST gap and overlap, and columns created outside Slick. */
class JavaTimeTest extends AsyncTest[JdbcTestDB] {
  import tdb.profile.api._

  lazy val expect = new JavaTimeExpectations(tdb.profile)

  val helsinki: ZoneId = ZoneId.of("Europe/Helsinki")
  val newYork: ZoneId = ZoneId.of("America/New_York")
  val plus0530: ZoneOffset = ZoneOffset.ofHoursMinutes(5, 30)
  val minus02: ZoneOffset = ZoneOffset.ofHours(-2)

  def testFullPrecision = {
    class T(tag: Tag) extends Table[(Int, LocalDate, LocalTime, LocalDateTime, Instant, OffsetTime, OffsetDateTime,
      ZonedDateTime)](tag, "jt_precision") {
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
    val ld = LocalDate.of(2024, 6, 15)
    val lt = LocalTime.of(12, 34, 56, 123456789)
    val ldt = LocalDateTime.of(ld, lt)
    val i = Instant.parse("2024-06-15T12:34:56.123456789Z")
    val ot = OffsetTime.of(lt, plus0530)
    val odt = OffsetDateTime.of(ldt, plus0530)
    val zdt = ZonedDateTime.of(ldt, newYork)
    val expected = (1, ld, expect.localTime(lt), expect.localDateTime(ldt), expect.instant(i), expect.offsetTime(ot),
      expect.offsetDateTime(odt), expect.zonedDateTime(zdt))

    seq(
      ts.schema.create,
      ts += (1, ld, lt, ldt, i, ot, odt, zdt),
      ts.result.map(_ shouldBe Seq(expected)),
      // The values as stored are found by bound values
      ts.filter(t => t.ld === expected._2.bind && t.lt === expected._3.bind && t.ldt === expected._4.bind &&
        t.i === expected._5.bind && t.ot === expected._6.bind && t.odt === expected._7.bind &&
        t.zdt === expected._8.bind).map(_.id).result.map(_ shouldBe Seq(1)),
      ts.schema.drop
    )
  }

  /** Inserts three values, given in ascending order (by instant for the offset types), in a different order, and
    * checks `<`, `>`, `sortBy`, `max` and `min`. */
  private def ordering[T: BaseColumnType](name: String, ascending: Seq[T], expected: T => T) = {
    class O(tag: Tag) extends Table[(Int, T)](tag, s"jt_order_$name") {
      def id = column[Int]("id")
      def v = column[T]("v")
      def * = (id, v)
    }
    val ts = TableQuery[O]
    val low = ascending(0)
    val mid = ascending(1)
    val high = ascending(2)
    val literals = implicitly[BaseColumnType[T]].hasLiteralForm

    seq(
      ts.schema.create,
      // Single inserts: Derby moves a time in a DST gap of the JVM time zone in a batch (see DerbyProfile)
      ts += (1, mid),
      ts += (2, high),
      ts += (3, low),
      mark(s"$name sortBy", ts.sortBy(_.v).map(_.id).result).map(_ shouldBe Seq(3, 1, 2)),
      mark(s"$name sortBy desc", ts.sortBy(_.v.desc).map(_.id).result).map(_ shouldBe Seq(2, 1, 3)),
      mark(s"$name > bind", ts.filter(_.v > mid.bind).map(_.id).result).map(_ shouldBe Seq(2)),
      mark(s"$name < bind", ts.filter(_.v < mid.bind).map(_.id).result).map(_ shouldBe Seq(3)),
      if (literals) seq(
        mark(s"$name > literal", ts.filter(_.v > LiteralColumn(mid)).map(_.id).result).map(_ shouldBe Seq(2)),
        mark(s"$name <= literal", ts.filter(_.v <= LiteralColumn(mid)).sortBy(_.id).map(_.id).result)
          .map(_ shouldBe Seq(1, 3))
      ) else DBIO.successful(()),
      mark(s"$name max", ts.map(_.v).max.result).map(_ shouldBe Some(expected(high))),
      mark(s"$name min", ts.map(_.v).min.result).map(_ shouldBe Some(expected(low))),
      ts.schema.drop
    )
  }

  def testLocalDateOrdering =
    ordering[LocalDate]("ld", Seq(LocalDate.of(2023, 12, 31), LocalDate.of(2024, 1, 2), LocalDate.of(2024, 10, 1)),
      identity)

  def testLocalTimeOrdering =
    ordering[LocalTime]("lt", Seq(LocalTime.of(9, 0), LocalTime.of(9, 0, 0, 500000000), LocalTime.of(10, 0, 1)),
      expect.localTime)

  // The first value is in the Europe/Helsinki DST gap, the second in its overlap
  def testLocalDateTimeOrdering =
    ordering[LocalDateTime]("ldt", Seq(LocalDateTime.of(2024, 3, 31, 3, 30, 0, 654000000),
      LocalDateTime.of(2024, 10, 27, 3, 30), LocalDateTime.of(2024, 10, 27, 3, 30, 0, 1000000)), expect.localDateTime)

  // The last two are the same wall-clock time (03:30) in Europe/Helsinki, before and after the clocks go back
  def testInstantOrdering =
    ordering[Instant]("i", Seq(Instant.parse("2024-03-31T00:30:00.001Z"), Instant.parse("2024-10-27T00:30:00Z"),
      Instant.parse("2024-10-27T01:30:00Z")), expect.instant)

  // In UTC: 06:30, 09:00, 10:00. The local times are in a different order.
  def testOffsetTimeOrdering =
    ordering[OffsetTime]("ot", Seq(OffsetTime.of(12, 0, 0, 0, plus0530), OffsetTime.of(9, 0, 0, 0, ZoneOffset.UTC),
      OffsetTime.of(8, 0, 0, 0, minus02)), expect.offsetTime)

  def testOffsetDateTimeOrdering =
    ordering[OffsetDateTime]("odt", Seq(OffsetDateTime.of(2024, 6, 15, 12, 0, 0, 0, plus0530),
      OffsetDateTime.of(2024, 6, 15, 9, 0, 0, 0, ZoneOffset.UTC), OffsetDateTime.of(2024, 6, 15, 8, 0, 0, 0, minus02)),
      expect.offsetDateTime)

  // 00:30Z and 01:30Z are both 03:30 in Helsinki (before and after the clocks go back), then 04:00Z in New York
  def testZonedDateTimeOrdering =
    ordering[ZonedDateTime]("zdt", Seq(
      ZonedDateTime.of(LocalDateTime.of(2024, 10, 27, 3, 30), helsinki).withEarlierOffsetAtOverlap(),
      ZonedDateTime.of(LocalDateTime.of(2024, 10, 27, 3, 30), helsinki).withLaterOffsetAtOverlap(),
      ZonedDateTime.of(LocalDateTime.of(2024, 10, 27, 0, 0), newYork)), expect.zonedDateTime)

  /** Values that the database or the JDBC driver may shift when they pass through the JVM time zone, as
    * java.sql.Timestamp does: a wall-clock time in the Europe/Helsinki DST gap, and the two instants of its
    * overlap. Only meaningful when the tests run in that zone, as they do once per release. */
  def testDstValues = {
    class T(tag: Tag) extends Table[(Int, LocalDateTime, Instant, ZonedDateTime)](tag, "jt_dst") {
      def id = column[Int]("id")
      def ldt = column[LocalDateTime]("ldt")
      def i = column[Instant]("i")
      def zdt = column[ZonedDateTime]("zdt")
      def * = (id, ldt, i, zdt)
    }
    val ts = TableQuery[T]
    val gap = LocalDateTime.of(2024, 3, 31, 3, 30, 0, 654000000)
    val overlap = LocalDateTime.of(2024, 10, 27, 3, 30)
    val rows = Seq(
      (1, gap, Instant.parse("2024-10-27T00:30:00Z"), ZonedDateTime.of(overlap, helsinki).withEarlierOffsetAtOverlap()),
      (2, overlap, Instant.parse("2024-10-27T01:30:00Z"), ZonedDateTime.of(overlap, helsinki).withLaterOffsetAtOverlap())
    )
    val expected = rows.map { case (id, l, i, z) =>
      (id, expect.localDateTime(l), expect.instant(i), expect.zonedDateTime(z))
    }

    seq(
      ts.schema.create,
      ts += rows(0),
      ts += rows(1),
      ts.sortBy(_.id).result.map(_ shouldBe expected),
      ts.filter(_.ldt === gap.bind).map(_.id).result.map(_ shouldBe Seq(1)),
      ts.filter(_.i === rows(1)._3.bind).map(_.id).result.map(_ shouldBe Seq(2)),
      ts.filter(_.zdt === rows(1)._4.bind).map(_.id).result.map(_ shouldBe Seq(2)),
      ts.schema.drop
    )
  }

  /** Columns created outside Slick with the database's own types are read by the java.time mappings (#1984 for
    * MySQL DATETIME) */
  def testColumnsCreatedOutsideSlick = {
    def q(id: String) = tdb.profile.quoteIdentifier(id)
    val table = q("jt_external")
    val ldt = LocalDateTime.of(2024, 6, 15, 12, 34, 56)
    val odt = OffsetDateTime.of(ldt, plus0530)

    class T(tag: Tag) extends Table[(Int, LocalDateTime, Instant, OffsetDateTime, ZonedDateTime)](tag, "jt_external") {
      def id = column[Int]("id")
      def ldt = column[LocalDateTime]("ldt")
      def i = column[Instant]("i")
      def odt = column[OffsetDateTime]("odt")
      def zdt = column[ZonedDateTime]("zdt")
      def * = (id, ldt, i, odt, zdt)
    }
    val ts = TableQuery[T]
    def columns(dt: String, tz: String) =
      s"${q("id")} INT, ${q("ldt")} $dt, ${q("i")} $tz, ${q("odt")} $tz, ${q("zdt")} $tz"

    val plainLiteral = "'2024-06-15 12:34:56'"
    /* For each database: its native types, literals for the timestamp without and with an offset, and whether the
     * zone column keeps the region */
    val native: Option[(String, String, String, Boolean)] = tdb.profile match {
      case _: MySQLProfile =>
        // MySQL's DATETIME without a precision stores whole seconds (#1984)
        Some((columns("DATETIME", "TIMESTAMP NULL"), plainLiteral, "'2024-06-15 12:34:56+05:30'", false))
      case _: PostgresProfile =>
        Some((columns("timestamp", "timestamptz"), plainLiteral, "'2024-06-15 12:34:56+05:30'", false))
      case _: SQLServerProfile =>
        Some((columns("datetime2", "datetimeoffset"), plainLiteral, "'2024-06-15 12:34:56 +05:30'", false))
      case _: OracleProfile =>
        Some((columns("TIMESTAMP", "TIMESTAMP WITH TIME ZONE"), "TIMESTAMP '2024-06-15 12:34:56'",
          "TIMESTAMP '2024-06-15 12:34:56 +05:30'", true))
      case _: H2Profile =>
        Some((columns("TIMESTAMP", "TIMESTAMP WITH TIME ZONE"), plainLiteral,
          "TIMESTAMP WITH TIME ZONE '2024-06-15 12:34:56+05:30'", false))
      case _: HsqldbProfile =>
        Some((columns("TIMESTAMP", "TIMESTAMP WITH TIME ZONE"), "TIMESTAMP '2024-06-15 12:34:56'",
          "TIMESTAMP '2024-06-15 12:34:56+05:30'", false))
      case _ =>
        None
    }

    native match {
      case Some((cols, ldtLiteral, tsLiteral, keepsRegion)) =>
        val zdt = if (keepsRegion) odt.toZonedDateTime else expect.zonedDateTime(odt.toZonedDateTime)

        seq(
          sqlu"#${s"create table $table ($cols)"}",
          sqlu"#${s"insert into $table values (1, $ldtLiteral, $tsLiteral, $tsLiteral, $tsLiteral)"}",
          ts.result.map(_ shouldBe Seq((1, ldt, odt.toInstant, expect.offsetDateTime(odt), zdt))),
          ts.filter(_.ldt === ldt).map(_.id).result.map(_ shouldBe Seq(1)),
          ts.filter(_.i === odt.toInstant.bind).map(_.id).result.map(_ shouldBe Seq(1)),
          sqlu"#${s"drop table $table"}"
        )
      case None =>
        DBIO.successful(())
    }
  }
}
