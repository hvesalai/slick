package slick.test.jdbc

import java.sql.{Date, Timestamp}
import java.time.*

import com.typesafe.config.{Config, ConfigFactory}
import org.junit.Assert.*
import org.junit.Test

import slick.SlickException
import slick.jdbc.*

/** The DDL and literals of the java.time mappings, without a database. With `javaTimeStorage = text` they must be
  * exactly those of Slick 3 (upstream/main before the native mappings). */
class JavaTimeMappingTest {
  import JavaTimeMappingTest.*

  val ld = LocalDate.of(2024, 6, 15)
  val ldt = LocalDateTime.of(2024, 6, 15, 12, 34, 56, 123456789)

  /** The column types, java.sql.Types codes and literals of the seven java.time types in a profile */
  def describe(p: JdbcProfile): Expected = {
    import p.api.*

    class T(tag: Tag) extends Table[(LocalDate, LocalTime, LocalDateTime, Instant, OffsetTime, OffsetDateTime,
      ZonedDateTime, Option[Instant])](tag, "t") {
      def ld = column[LocalDate]("ld")
      def lt = column[LocalTime]("lt")
      def ldt = column[LocalDateTime]("ldt")
      def i = column[Instant]("i")
      def ot = column[OffsetTime]("ot")
      def odt = column[OffsetDateTime]("odt")
      def zdt = column[ZonedDateTime]("zdt")
      def oi = column[Option[Instant]]("oi")
      def * = (ld, lt, ldt, i, ot, odt, zdt, oi)
    }
    val ct = p.columnTypes
    val types: Seq[(p.DriverJdbcType[Any], Any)] = Seq(
      ct.localDateType -> ld,
      ct.localTimeType -> LocalTime.of(12, 34, 56, 123456789),
      ct.localDateTimeType -> ldt,
      ct.instantType -> Instant.parse("2024-06-15T12:34:56.123456789Z"),
      ct.offsetTimeType -> OffsetTime.of(12, 34, 56, 123456789, ZoneOffset.ofHoursMinutes(5, 30)),
      ct.offsetDateTimeType -> OffsetDateTime.of(2024, 6, 15, 12, 34, 56, 123456789, ZoneOffset.ofHoursMinutes(5, 30)),
      ct.zonedDateType -> ZonedDateTime.of(2024, 6, 15, 12, 34, 56, 123456789, ZoneId.of("Europe/Helsinki"))
    ).map { case (t, v) => (t.asInstanceOf[p.DriverJdbcType[Any]], v) }

    Expected(
      TableQuery[T].schema.createStatements.mkString("\n"),
      types.map(_._1.sqlType),
      types.map { case (t, v) => if (t.hasLiteralForm) t.valueToSQLLiteral(v) else "<no literal>" })
  }

  def check(mode: String, expected: Map[String, Expected], profiles: Seq[(String, JdbcProfile)]): Unit =
    for ((name, p) <- profiles) {
      val e = expected(name)
      val a = describe(p)

      assertEquals(s"$mode DDL of $name", e.ddl, a.ddl)
      assertEquals(s"$mode java.sql.Types of $name", e.sqlTypes, a.sqlTypes)
      assertEquals(s"$mode literals of $name", e.literals, a.literals)
    }

  @Test def testNativeMappings(): Unit =
    check("native", native, Seq(
      "H2Profile" -> H2Profile,
      "PostgresProfile" -> PostgresProfile,
      "MySQLProfile" -> MySQLProfile,
      "SQLServerProfile" -> SQLServerProfile,
      "OracleProfile" -> OracleProfile,
      "DB2Profile" -> DB2Profile,
      "DerbyProfile" -> DerbyProfile,
      "HsqldbProfile" -> HsqldbProfile,
      "SQLiteProfile" -> SQLiteProfile))

  @Test def testTextMappingsAreThoseOfSlick3(): Unit =
    check("text", text, Seq(
      "H2Profile" -> new H2Profile { override protected def loadProfileConfig = textStorage },
      "PostgresProfile" -> new PostgresProfile { override protected def loadProfileConfig = textStorage },
      "MySQLProfile" -> new MySQLProfile { override protected def loadProfileConfig = textStorage },
      "SQLServerProfile" -> new SQLServerProfile { override protected def loadProfileConfig = textStorage },
      "OracleProfile" -> new OracleProfile with JdbcActionComponent.OneRowPerStatementOnly {
        override protected def loadProfileConfig = textStorage
      },
      "DB2Profile" -> new DB2Profile { override protected def loadProfileConfig = textStorage },
      "DerbyProfile" -> new DerbyProfile { override protected def loadProfileConfig = textStorage },
      "HsqldbProfile" -> new HsqldbProfile { override protected def loadProfileConfig = textStorage },
      "SQLiteProfile" -> new SQLiteProfile { override protected def loadProfileConfig = textStorage }))

  @Test def testUnknownStorageFails(): Unit = {
    val e = assertThrows(classOf[SlickException], () => {
      new H2Profile { override protected def loadProfileConfig = ConfigFactory.parseString("javaTimeStorage = utc") }
      ()
    })

    assertTrue(e.getMessage, e.getMessage.contains("javaTimeStorage") && e.getMessage.contains("'utc'") &&
      e.getMessage.contains("'native'") && e.getMessage.contains("'text'"))
  }

  /** java.sql.Timestamp and Time keep the #1412 MySQL precision in either mode */
  @Test def testTextStorageKeepsMySQLTimestampPrecision(): Unit = {
    val p = new MySQLProfile { override protected def loadProfileConfig = textStorage }

    assertEquals("TIMESTAMP(6)", p.columnTypes.timestampJdbcType.sqlTypeName(None))
  }

  val text: Map[String, Expected] = Map(
    "H2Profile" -> Expected(
      "create table \"t\" (\"ld\" DATE NOT NULL,\"lt\" VARCHAR NOT NULL,\"ldt\" TIMESTAMP NOT NULL,\"i\" TIMESTAMP(9) WITH TIME ZONE NOT NULL,\"ot\" VARCHAR NOT NULL,\"odt\" VARCHAR NOT NULL,\"zdt\" VARCHAR NOT NULL,\"oi\" TIMESTAMP(9) WITH TIME ZONE)",
      Seq(91, 12, 93, 93, 12, 12, 12),
      Seq(
        "'2024-06-15'",
        "'12:34:56.123456789'",
        "'2024-06-15 12:34:56.123456789'",
        "'2024-06-15T12:34:56.123456789Z'",
        "'12:34:56.123456789+05:30'",
        "'2024-06-15T12:34:56.123456789+05:30'",
        "'2024-06-15T12:34:56.123456789+03:00[Europe/Helsinki]'")),
    "PostgresProfile" -> Expected(
      "create table \"t\" (\"ld\" DATE NOT NULL,\"lt\" TIME NOT NULL,\"ldt\" TIMESTAMP NOT NULL,\"i\" TIMESTAMP NOT NULL,\"ot\" TIMETZ NOT NULL,\"odt\" VARCHAR NOT NULL,\"zdt\" VARCHAR NOT NULL,\"oi\" TIMESTAMP)",
      Seq(91, 1111, 1111, 1111, 1111, 12, 12),
      Seq(
        "<no literal>",
        "<no literal>",
        "<no literal>",
        "<no literal>",
        "<no literal>",
        "'2024-06-15T12:34:56.123456789+05:30'",
        "'2024-06-15T12:34:56.123456789+03:00[Europe/Helsinki]'")),
    "MySQLProfile" -> Expected(
      "create table `t` (`ld` DATE NOT NULL,`lt` TEXT NOT NULL,`ldt` TEXT NOT NULL,`i` TEXT NOT NULL,`ot` TEXT NOT NULL,`odt` TEXT NOT NULL,`zdt` TEXT NOT NULL,`oi` TEXT)",
      Seq(91, 12, 12, 12, 12, 12, 12),
      Seq(
        "'2024-06-15'",
        "'12:34:56.123456789'",
        "'2024-06-15T12:34:56.123456789'",
        "'2024-06-15T12:34:56.123456789Z'",
        "'12:34:56.123456789+05:30'",
        "'2024-06-15T12:34:56.123456789+05:30'",
        "'2024-06-15T12:34:56.123456789+03:00[Europe/Helsinki]'")),
    "SQLServerProfile" -> Expected(
      "create table \"t\" (\"ld\" DATE NOT NULL,\"lt\" TIME(6) NOT NULL,\"ldt\" DATETIME2(6) NOT NULL,\"i\" DATETIMEOFFSET(6) NOT NULL,\"ot\" VARCHAR(MAX) NOT NULL,\"odt\" DATETIMEOFFSET(6) NOT NULL,\"zdt\" VARCHAR(MAX) NOT NULL,\"oi\" DATETIMEOFFSET(6))",
      Seq(91, 12, 93, 93, 12, 12, 12),
      Seq(
        "'2024-06-15'",
        "(convert(time(6), '12:34:56.123456789'))",
        "'2024-06-15 12:34:56.123456789'",
        "(convert(datetimeoffset(6), '2024-06-15 12:34:56.123456 '))",
        "'12:34:56.123456789+05:30'",
        "(convert(datetimeoffset(6), '2024-06-15 12:34:56.123456 +05:30'))",
        "'2024-06-15T12:34:56.123456789+03:00[Europe/Helsinki]'")),
    "OracleProfile" -> Expected(
      "create table \"t\" (\"ld\" DATE NOT NULL,\"lt\" VARCHAR2(254) NOT NULL,\"ldt\" TIMESTAMP NOT NULL,\"i\" TIMESTAMP(9) NOT NULL,\"ot\" TIMESTAMP(6) WITH TIME ZONE NOT NULL,\"odt\" VARCHAR2(254) NOT NULL,\"zdt\" VARCHAR2(254) NOT NULL,\"oi\" TIMESTAMP(9))",
      Seq(91, 12, 93, 93, 12, 12, 12),
      Seq(
        "TO_DATE('2024-06-15', 'SYYYY-MM-DD')",
        "'12:34:56.123456789'",
        "TO_TIMESTAMP('2024-06-15 12:34:56.123456789', 'YYYY-MM-DD HH24:MI:SS.FF9')",
        "TO_TIMESTAMP_TZ('2024-06-15 12:34:56.123456789 +00', 'YYYY-MM-DD HH24:MI:SS.FF9 TZH')",
        "TO_TIMESTAMP_TZ('1970-01-01 12:34:56.123456789 +0530', 'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM')",
        "'2024-06-15T12:34:56.123456789+05:30'",
        "'2024-06-15T12:34:56.123456789+03:00[Europe/Helsinki]'")),
    "DB2Profile" -> Expected(
      "create table \"t\" (\"ld\" DATE NOT NULL,\"lt\" VARCHAR(254) NOT NULL,\"ldt\" TIMESTAMP NOT NULL,\"i\" VARCHAR(254) NOT NULL,\"ot\" VARCHAR(254) NOT NULL,\"odt\" VARCHAR(254) NOT NULL,\"zdt\" VARCHAR(254) NOT NULL,\"oi\" VARCHAR(254))",
      Seq(91, 12, 93, 12, 12, 12, 12),
      Seq(
        "'2024-06-15'",
        "'12:34:56.123456789'",
        "'2024-06-15 12:34:56.123456789'",
        "'2024-06-15T12:34:56.123456789Z'",
        "'12:34:56.123456789+05:30'",
        "'2024-06-15T12:34:56.123456789+05:30'",
        "'2024-06-15T12:34:56.123456789+03:00[Europe/Helsinki]'")),
    "DerbyProfile" -> Expected(
      "create table \"t\" (\"ld\" DATE NOT NULL,\"lt\" VARCHAR(254) NOT NULL,\"ldt\" TIMESTAMP NOT NULL,\"i\" VARCHAR(254) NOT NULL,\"ot\" VARCHAR(254) NOT NULL,\"odt\" VARCHAR(254) NOT NULL,\"zdt\" VARCHAR(254) NOT NULL,\"oi\" VARCHAR(254))",
      Seq(91, 12, 93, 12, 12, 12, 12),
      Seq(
        "'2024-06-15'",
        "'12:34:56.123456789'",
        "'2024-06-15 12:34:56.123456789'",
        "'2024-06-15T12:34:56.123456789Z'",
        "'12:34:56.123456789+05:30'",
        "'2024-06-15T12:34:56.123456789+05:30'",
        "'2024-06-15T12:34:56.123456789+03:00[Europe/Helsinki]'")),
    "HsqldbProfile" -> Expected(
      "create table \"t\" (\"ld\" DATE NOT NULL,\"lt\" TIME(3) NOT NULL,\"ldt\" TIMESTAMP NOT NULL,\"i\" TIMESTAMP(9) WITH TIME ZONE NOT NULL,\"ot\" TIME(9) WITH TIME ZONE NOT NULL,\"odt\" TIMESTAMP(9) WITH TIME ZONE NOT NULL,\"zdt\" LONGVARCHAR NOT NULL,\"oi\" TIMESTAMP(9) WITH TIME ZONE)",
      Seq(91, 92, 93, 2014, 2013, 2014, 12),
      Seq(
        "'2024-06-15'",
        "'12:34:56.123456789'",
        "'2024-06-15 12:34:56.123456789'",
        "<no literal>",
        "<no literal>",
        "<no literal>",
        "'2024-06-15T12:34:56.123456789+03:00[Europe/Helsinki]'")),
    "SQLiteProfile" -> Expected(
      "create table \"t\" (\"ld\" DATE NOT NULL,\"lt\" VARCHAR(254) NOT NULL,\"ldt\" TIMESTAMP NOT NULL,\"i\" TIMESTAMP NOT NULL,\"ot\" VARCHAR(254) NOT NULL,\"odt\" VARCHAR(254) NOT NULL,\"zdt\" VARCHAR(254) NOT NULL,\"oi\" TIMESTAMP)",
      Seq(91, 12, 93, 93, 12, 12, 12),
      Seq(
        Date.valueOf(ld).getTime.toString,
        "'12:34:56.123456789'",
        Timestamp.valueOf(ldt).getTime.toString,
        "1718454896123",
        "'12:34:56.123456789+05:30'",
        "'2024-06-15T12:34:56.123456789+05:30'",
        "'2024-06-15T12:34:56.123456789+03:00[Europe/Helsinki]'")))

  val native: Map[String, Expected] = Map(
    "H2Profile" -> Expected(
      "create table \"t\" (\"ld\" DATE NOT NULL,\"lt\" TIME(9) NOT NULL,\"ldt\" TIMESTAMP(9) NOT NULL,\"i\" TIMESTAMP(9) WITH TIME ZONE NOT NULL,\"ot\" TIME(9) WITH TIME ZONE NOT NULL,\"odt\" TIMESTAMP(9) WITH TIME ZONE NOT NULL,\"zdt\" TIMESTAMP(9) WITH TIME ZONE NOT NULL,\"oi\" TIMESTAMP(9) WITH TIME ZONE)",
      Seq(91, 92, 93, 2014, 2013, 2014, 2014),
      Seq(
        "'2024-06-15'",
        "'12:34:56.123456789'",
        "'2024-06-15 12:34:56.123456789'",
        "TIMESTAMP WITH TIME ZONE '2024-06-15 12:34:56.123456789+00:00'",
        "TIME WITH TIME ZONE '12:34:56.123456789+05:30'",
        "TIMESTAMP WITH TIME ZONE '2024-06-15 12:34:56.123456789+05:30'",
        "TIMESTAMP WITH TIME ZONE '2024-06-15 12:34:56.123456789+03:00'")),
    "PostgresProfile" -> Expected(
      "create table \"t\" (\"ld\" DATE NOT NULL,\"lt\" TIME(6) NOT NULL,\"ldt\" TIMESTAMP(6) NOT NULL,\"i\" TIMESTAMPTZ NOT NULL,\"ot\" TIMETZ NOT NULL,\"odt\" TIMESTAMPTZ NOT NULL,\"zdt\" TIMESTAMPTZ NOT NULL,\"oi\" TIMESTAMPTZ)",
      Seq(91, 1111, 1111, 1111, 1111, 1111, 1111),
      Seq(
        "<no literal>",
        "<no literal>",
        "<no literal>",
        "<no literal>",
        "<no literal>",
        "<no literal>",
        "<no literal>")),
    "MySQLProfile" -> Expected(
      "create table `t` (`ld` DATE NOT NULL,`lt` TIME(6) NOT NULL,`ldt` DATETIME(6) NOT NULL,`i` TIMESTAMP(6) NOT NULL,`ot` TEXT NOT NULL,`odt` TIMESTAMP(6) NOT NULL,`zdt` TIMESTAMP(6) NOT NULL,`oi` TIMESTAMP(6) NULL)",
      Seq(91, 92, 93, 93, 12, 93, 93),
      Seq(
        "'2024-06-15'",
        "'12:34:56.123456789'",
        "'2024-06-15 12:34:56.123456789'",
        "'2024-06-15 12:34:56.123456789+00:00'",
        "'07:04:56.123456789Z'",
        "'2024-06-15 12:34:56.123456789+05:30'",
        "'2024-06-15 12:34:56.123456789+03:00'")),
    "SQLServerProfile" -> Expected(
      "create table \"t\" (\"ld\" DATE NOT NULL,\"lt\" TIME(7) NOT NULL,\"ldt\" DATETIME2(7) NOT NULL,\"i\" DATETIMEOFFSET(7) NOT NULL,\"ot\" VARCHAR(MAX) NOT NULL,\"odt\" DATETIMEOFFSET(7) NOT NULL,\"zdt\" DATETIMEOFFSET(7) NOT NULL,\"oi\" DATETIMEOFFSET(7))",
      Seq(91, 92, 93, 2014, 12, 2014, 2014),
      Seq(
        "(convert(date, '2024-06-15'))",
        "(convert(time(7), '12:34:56.1234568'))",
        "(convert(datetime2(7), '2024-06-15 12:34:56.1234568'))",
        "(convert(datetimeoffset(7), '2024-06-15 12:34:56.1234568 +00:00'))",
        "'07:04:56.123456789Z'",
        "(convert(datetimeoffset(7), '2024-06-15 12:34:56.1234568 +05:30'))",
        "(convert(datetimeoffset(7), '2024-06-15 12:34:56.1234568 +03:00'))")),
    "OracleProfile" -> Expected(
      "create table \"t\" (\"ld\" DATE NOT NULL,\"lt\" VARCHAR2(254) NOT NULL,\"ldt\" TIMESTAMP(9) NOT NULL,\"i\" TIMESTAMP(9) WITH TIME ZONE NOT NULL,\"ot\" TIMESTAMP(6) WITH TIME ZONE NOT NULL,\"odt\" TIMESTAMP(9) WITH TIME ZONE NOT NULL,\"zdt\" TIMESTAMP(9) WITH TIME ZONE NOT NULL,\"oi\" TIMESTAMP(9) WITH TIME ZONE)",
      Seq(91, 12, 93, 2014, 12, 2014, 2014),
      Seq(
        "TO_DATE('2024-06-15', 'SYYYY-MM-DD')",
        "'12:34:56.123456789'",
        "TO_TIMESTAMP('2024-06-15 12:34:56.123456789', 'YYYY-MM-DD HH24:MI:SS.FF9')",
        "TO_TIMESTAMP_TZ('2024-06-15 12:34:56.123456789 +00:00', 'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM')",
        "TO_TIMESTAMP_TZ('1970-01-01 12:34:56.123456789 +0530', 'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM')",
        "TO_TIMESTAMP_TZ('2024-06-15 12:34:56.123456789 +05:30', 'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM')",
        "(TO_TIMESTAMP_TZ('2024-06-15 09:34:56.123456789 +00:00', 'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM') AT TIME ZONE 'Europe/Helsinki')")),
    "DB2Profile" -> Expected(
      "create table \"t\" (\"ld\" DATE NOT NULL,\"lt\" VARCHAR(254) NOT NULL,\"ldt\" TIMESTAMP(9) NOT NULL,\"i\" TIMESTAMP(9) NOT NULL,\"ot\" VARCHAR(254) NOT NULL,\"odt\" TIMESTAMP(9) NOT NULL,\"zdt\" TIMESTAMP(9) NOT NULL,\"oi\" TIMESTAMP(9))",
      Seq(91, 12, 93, 93, 12, 93, 93),
      Seq(
        "'2024-06-15'",
        "'12:34:56.123456789'",
        "'2024-06-15 12:34:56.123456789'",
        "'2024-06-15 12:34:56.123456789'",
        "'07:04:56.123456789Z'",
        "'2024-06-15 07:04:56.123456789'",
        "'2024-06-15 09:34:56.123456789'")),
    "DerbyProfile" -> Expected(
      "create table \"t\" (\"ld\" DATE NOT NULL,\"lt\" VARCHAR(254) NOT NULL,\"ldt\" TIMESTAMP NOT NULL,\"i\" VARCHAR(254) NOT NULL,\"ot\" VARCHAR(254) NOT NULL,\"odt\" VARCHAR(254) NOT NULL,\"zdt\" VARCHAR(254) NOT NULL,\"oi\" VARCHAR(254))",
      Seq(91, 12, 93, 12, 12, 12, 12),
      Seq(
        "'2024-06-15'",
        "'12:34:56.123456789'",
        "'2024-06-15 12:34:56.123456789'",
        "'2024-06-15T12:34:56.123456789Z'",
        "'07:04:56.123456789Z'",
        "'2024-06-15T07:04:56.123456789Z'",
        "'2024-06-15T09:34:56.123456789Z'")),
    "HsqldbProfile" -> Expected(
      "create table \"t\" (\"ld\" DATE NOT NULL,\"lt\" TIME(9) NOT NULL,\"ldt\" TIMESTAMP(9) NOT NULL,\"i\" TIMESTAMP(9) WITH TIME ZONE NOT NULL,\"ot\" TIME(9) WITH TIME ZONE NOT NULL,\"odt\" TIMESTAMP(9) WITH TIME ZONE NOT NULL,\"zdt\" TIMESTAMP(9) WITH TIME ZONE NOT NULL,\"oi\" TIMESTAMP(9) WITH TIME ZONE)",
      Seq(91, 92, 93, 2014, 2013, 2014, 2014),
      Seq(
        "DATE '2024-06-15'",
        "TIME '12:34:56.123456789'",
        "TIMESTAMP '2024-06-15 12:34:56.123456789'",
        "TIMESTAMP '2024-06-15 12:34:56.123456789+00:00'",
        "TIME '12:34:56.123456789+05:30'",
        "TIMESTAMP '2024-06-15 12:34:56.123456789+05:30'",
        "TIMESTAMP '2024-06-15 12:34:56.123456789+03:00'")),
    "SQLiteProfile" -> Expected(
      "create table \"t\" (\"ld\" DATE NOT NULL,\"lt\" VARCHAR(254) NOT NULL,\"ldt\" TIMESTAMP NOT NULL,\"i\" TIMESTAMP NOT NULL,\"ot\" VARCHAR(254) NOT NULL,\"odt\" VARCHAR(254) NOT NULL,\"zdt\" VARCHAR(254) NOT NULL,\"oi\" TIMESTAMP)",
      Seq(91, 12, 93, 93, 12, 12, 12),
      Seq(
        Date.valueOf(ld).getTime.toString,
        "'12:34:56.123456789'",
        Timestamp.valueOf(ldt).getTime.toString,
        "1718454896123",
        "'07:04:56.123456789Z'",
        "'2024-06-15T07:04:56.123456789Z'",
        "'2024-06-15T09:34:56.123456789Z'")))
}

object JavaTimeMappingTest {
  case class Expected(ddl: String, sqlTypes: Seq[Int], literals: Seq[String])

  val textStorage: Config = ConfigFactory.parseString("javaTimeStorage = text")
}
