Date and Time types
===================

Slick maps the [java.time](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/package-summary.html)
types `LocalDate`, `LocalTime`, `LocalDateTime`, `Instant`, `OffsetTime`, `OffsetDateTime` and `ZonedDateTime` to the
database's own date and time types wherever the database has one. The databases differ a lot in what their types
keep (an offset, a zone, how many fractional digits), so the same Scala type behaves somewhat differently on
different databases. This page lists what each profile does.

Values are bound and read without going through the JVM's default time zone: a `LocalDateTime` in a daylight saving
time (DST) gap of the JVM's zone, or two different `Instant`s that have the same wall-clock time in the hour the
clocks are turned back, are stored and read back unchanged. The exceptions are listed per database below.

## Mappings

N = the database's own type. U = the database's own type, but it keeps only the instant: the value is normalized to
UTC (see below). T = text, where the database has no fitting type.

| | PostgreSQL | MySQL | SQL Server | Oracle | DB2 | Derby | H2 | HSQLDB | SQLite |
|---|---|---|---|---|---|---|---|---|---|
| `LocalDate` | DATE N | DATE N | DATE N | DATE N | DATE N | DATE N | DATE N | DATE N | milliseconds |
| `LocalTime` | TIME(6) N | TIME(6) N | TIME(7) N | T | T | T | TIME(9) N | TIME(9) N | T |
| `LocalDateTime` | TIMESTAMP(6) N | DATETIME(6) N | DATETIME2(7) N | TIMESTAMP(9) N | TIMESTAMP(9) N | TIMESTAMP N | TIMESTAMP(9) N | TIMESTAMP(9) N | milliseconds |
| `Instant` | TIMESTAMPTZ N | TIMESTAMP(6) N | DATETIMEOFFSET(7) N | TIMESTAMP(9) WITH TIME ZONE N | TIMESTAMP(9) (UTC) | T | TIMESTAMP(9) WITH TIME ZONE N | TIMESTAMP(9) WITH TIME ZONE N | milliseconds |
| `OffsetTime` | TIMETZ N | T | T | TIMESTAMP(6) WITH TIME ZONE (on 1970-01-01) | T | T | TIME(9) WITH TIME ZONE N | TIME(9) WITH TIME ZONE N | T |
| `OffsetDateTime` | TIMESTAMPTZ U | TIMESTAMP(6) U | DATETIMEOFFSET(7) N | TIMESTAMP(9) WITH TIME ZONE N | TIMESTAMP(9) U | T | TIMESTAMP(9) WITH TIME ZONE N | TIMESTAMP(9) WITH TIME ZONE N | T |
| `ZonedDateTime` | TIMESTAMPTZ U | TIMESTAMP(6) U | DATETIMEOFFSET(7) offset | TIMESTAMP(9) WITH TIME ZONE N | TIMESTAMP(9) U | T | TIMESTAMP(9) WITH TIME ZONE offset | TIMESTAMP(9) WITH TIME ZONE offset | T |

MySQL's precision (6) is the profile setting `slick.jdbc.MySQLProfile.defaultFractionalSecondsPrecision`. SQLite
has no date and time types: `LocalDate`, `LocalDateTime` and `Instant` are stored as milliseconds since the epoch, as
the sqlite-jdbc driver stores `java.sql.Timestamp` (see the SQLite section of @ref:[Databases](database.md)). You can
change the type of a single column with `O.SqlType`, as long as the binding still fits it.

### Offsets and zones

- **Normalized to UTC (U).** PostgreSQL's `timestamptz` and MySQL's `TIMESTAMP` keep an instant, not the offset it
  was written with, and DB2 has no time zone type. There an `OffsetDateTime` or `ZonedDateTime` keeps its instant
  and is read back in UTC: `2024-06-15T12:00+02:00` is stored as 10:00 UTC and read back as
  `2024-06-15T10:00Z` (an `OffsetDateTime` with offset `+00:00`, or a `ZonedDateTime` with the zone
  `ZoneOffset.UTC`). The same applies to the text mappings of the offset types.
- **Offset only.** SQL Server's `datetimeoffset`, and the `WITH TIME ZONE` types of H2 and HSQLDB, keep the offset,
  but not a zone region: a `ZonedDateTime` in `Europe/Helsinki` is read back with the zone `+03:00`.
- **Region.** Only Oracle's `TIMESTAMP WITH TIME ZONE` keeps the zone region of a `ZonedDateTime`, including which of
  the two offsets a time in the hour the clocks are turned back had.
- Values with an offset are compared and sorted as instants: `12:00+05:30` is earlier than `09:00Z`.

### Text

Where there is no fitting type, the value is stored as text (`VARCHAR`, or `TEXT` on MySQL) in a form that sorts in
time order, so that `<`, `>`, `sortBy`, `min` and `max` work on it:

- `LocalTime`: `HH:mm:ss` with only as many fractional digits as needed, e.g. `09:30:00` or `09:30:00.5`.
- `OffsetTime`, `OffsetDateTime`, `ZonedDateTime`, and `Instant` on Derby: normalized to UTC as above, with a fixed
  width, e.g. `07:04:56.123456789Z` and `2024-06-15T07:04:56.123456789Z`.

Comparing an `OffsetTime` stored as text compares the UTC times of the day. That differs from `OffsetTime.compareTo`
when the conversion to UTC crosses midnight (`01:00+02:00` is `23:00Z`).

### Fractional seconds

The databases keep different numbers of fractional digits (see the table), and they treat the rest differently.
PostgreSQL, MySQL, SQL Server and Oracle round (for example `.123456789` is stored as `.123457` in PostgreSQL and
MySQL, and as `.1234568` in SQL Server); DB2 and HSQLDB truncate; H2, HSQLDB, Oracle and DB2 keep all nine digits of
the types above. Slick does not unify this, so a value read back can differ from the one written in its last digits.
Compare stored values with values of the column's precision. Near midnight, rounding a `LocalTime` can give 24:00,
which `LocalTime` cannot hold (MySQL; PostgreSQL reads it as `LocalTime.MAX`).

## Database notes

### MySQL

`TIMESTAMP` holds instants from 1970-01-01 00:00:01 UTC to 2038-01-19 03:14:07 UTC. MySQL converts a `TIMESTAMP`
from the session time zone to UTC when it stores it, and back when it reads it, and the MySQL driver converts between
instants and that wall-clock time in its `connectionTimeZone` (by default the JVM's zone). These two zones must be
the same, or `Instant`, `OffsetDateTime` and `ZonedDateTime` values are stored as the wrong instants. Set them in the
JDBC URL, preferably to a zone without DST:

```
jdbc:mysql://localhost/mydb?connectionTimeZone=+00:00&forceConnectionTimeZoneToSession=true
```

In a zone with DST, the two instants of the hour the clocks are turned back have the same wall-clock time, and MySQL
stores both as the earlier one. `DATETIME` (`LocalDateTime`), `DATE` and `TIME` are not converted at all.

### PostgreSQL

`timestamptz` keeps the instant in UTC. `-infinity` and `infinity` are read as `Instant.MIN`/`MAX`,
`OffsetDateTime.MIN`/`MAX`, `LocalDateTime.MIN`/`MAX` and `LocalDate.MIN`/`MAX`, and those values are written as
them. The java.time types have no literal form on PostgreSQL: they are always bound as parameters.

### Oracle

Oracle has no time type, so a `LocalTime` is stored as text, and an `OffsetTime` as a `TIMESTAMP WITH TIME ZONE` on
1970-01-01. A `TIMESTAMP WITH TIME ZONE` column (`Instant`, `OffsetDateTime`, `ZonedDateTime`) cannot be a primary
key, and an index on it is a function-based index. A `ZonedDateTime`'s zone must be one that Oracle's time zone file
knows.

### SQL Server

`LocalTime` values are bound as text, because the driver sends `LocalTime` as a `datetime` (1/300 s precision)
unless `sendTimeAsDatetime=false` is set.

### DB2 and Derby

Their drivers support no java.time types, and they convert `TIMESTAMP` values through the JVM's time zone. Slick binds
and reads them with a UTC `Calendar`, which avoids that, with these exceptions on **Derby**: Derby ignores the
`Calendar` in a batch (e.g. `++=`) and converts literals in the JVM's zone, so a `LocalDateTime` in a DST gap of the
JVM's zone is moved by an hour there. For the same reason Derby stores `Instant`, `OffsetDateTime` and
`ZonedDateTime` as text. Running a Derby application in UTC avoids these issues. Updates through a mutable result set
(`.mutate`) are bound as text on both.

### SQLite

`LocalDate`, `LocalDateTime` and `Instant` are mapped through `java.sql.Date` and `java.sql.Timestamp`, as in Slick 3,
so `LocalDate` and `LocalDateTime` values are converted in the JVM's time zone (a `LocalDateTime` in its DST gap is
moved by an hour), and they keep milliseconds.

## Storing zoned values

There are three ways to store a value whose zone matters:

1. The database's own type, normalized to UTC where the database does not keep the offset or zone (the default
   mappings above). Values compare and sort as instants.
2. As 1, plus the zone in a separate column, so that the original `ZonedDateTime` can be rebuilt:

    ```scala
    case class Event(id: Long, at: ZonedDateTime)

    class Events(tag: Tag) extends Table[Event](tag, "events") {
      def id = column[Long]("id", O.PrimaryKey)
      def at = column[Instant]("at")
      def zone = column[String]("zone")
      def * = (id, at, zone).<>(
        { case (id, at, zone) => Event(id, at.atZone(ZoneId.of(zone))) },
        (e: Event) => Some((e.id, e.at.toInstant, e.at.getZone.getId))
      )
    }
    ```

3. Text that keeps everything, such as `ZonedDateTime.toString`, with your own mapped type (see below). Text written
   with different offsets does not sort in time order.

## Tables created by Slick 3: `javaTimeStorage`

Slick 3 stored several of these types as text (for example `Instant` and `LocalDateTime` on MySQL, and
`OffsetDateTime` and `ZonedDateTime` on most databases), and bound some through `java.sql.Timestamp` in the JVM's
time zone. To keep using tables created by Slick 3 without migrating them, set

```
slick.jdbc.MySQLProfile.javaTimeStorage = text
```

(for your profile) in `application.conf`. `text` uses exactly the mappings of Slick 3 for all seven types (column
types, binding, reading and literals); the default is `native`. Any other value fails. A custom profile can also
override `javaTimeStorage`. The @ref:[migration guide](migrating-to-slick4.md#java-time-column-types) has the
statements that convert the columns to the native types.

## Custom mappings

You can replace a built-in mapping with your own column type, see @ref:[Schemas](schemas.md). Give the implicit the
same name as the built-in one (e.g. `instantColumnType`, `localDateTimeColumnType`, `offsetDateTimeColumnType`,
`zonedDateTimeColumnType`), so that it shadows the built-in one:

```scala
import slick.jdbc.MySQLProfile.api._

implicit val instantColumnType: BaseColumnType[Instant] =
  MappedColumnType.base[Instant, Long](_.toEpochMilli, Instant.ofEpochMilli)
```

On Scala 2, an implicit with a different name is ambiguous with, or loses to, the built-in one, which has a more
specific type. On Scala 3 a local implicit with any name wins.
