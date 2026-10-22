package com.typesafe.slick.testkit.tests

import java.time._
import java.time.temporal.{ChronoField, ChronoUnit, Temporal}

import slick.jdbc._

/** What a profile's native java.time mappings keep of a value: the offset or zone, and how many fractional
  * digits (and whether the database rounds or truncates the rest). Values read back from the database are
  * compared with `expected(written)`. */
class JavaTimeExpectations(profile: JdbcProfile) {

  /** TIME WITH TIME ZONE (or Oracle's TIMESTAMP WITH TIME ZONE) keeps the offset; the text mapping normalizes
    * to UTC */
  val offsetTimeKeepsOffset: Boolean = profile match {
    case _: H2Profile | _: HsqldbProfile | _: PostgresProfile | _: OracleProfile => true
    case _                                                                        => false
  }

  /** TIMESTAMP WITH TIME ZONE / DATETIMEOFFSET keeps the offset; the others normalize to UTC */
  val offsetDateTimeKeepsOffset: Boolean = profile match {
    case _: H2Profile | _: HsqldbProfile | _: SQLServerProfile | _: OracleProfile => true
    case _                                                                         => false
  }

  /** Only Oracle keeps the zone region of a ZonedDateTime */
  val zonedDateTimeKeepsRegion: Boolean = profile.isInstanceOf[OracleProfile]

  /** Fractional-second digits kept, and whether the database rounds (true) or truncates (false) */
  case class Precision(digits: Int, rounds: Boolean) {
    def apply[T <: Temporal](value: T): T = {
      val unit = math.pow(10, 9 - digits).toLong
      val shifted = if (rounds) value.plus(unit / 2, ChronoUnit.NANOS).asInstanceOf[T] else value

      shifted.`with`(ChronoField.NANO_OF_SECOND, shifted.get(ChronoField.NANO_OF_SECOND) / unit * unit).asInstanceOf[T]
    }
  }

  private val nanos = Precision(9, rounds = false)

  val localTimePrecision: Precision = profile match {
    case _: PostgresProfile | _: MySQLProfile => Precision(6, rounds = true)
    case _: SQLServerProfile                  => Precision(7, rounds = true)
    case _                                    => nanos
  }
  val localDateTimePrecision: Precision = profile match {
    case _: PostgresProfile | _: MySQLProfile => Precision(6, rounds = true)
    case _: SQLServerProfile                  => Precision(7, rounds = true)
    // sqlite-jdbc stores java.sql.Timestamp as milliseconds since the epoch
    case _: SQLiteProfile                     => Precision(3, rounds = false)
    case _                                    => nanos
  }
  val instantPrecision: Precision = localDateTimePrecision
  /** OffsetDateTime and ZonedDateTime (stored as text on SQLite) */
  val offsetDateTimePrecision: Precision = profile match {
    case _: SQLiteProfile => nanos
    case _                => localDateTimePrecision
  }
  val offsetTimePrecision: Precision = profile match {
    case _: PostgresProfile => Precision(6, rounds = true)
    // Oracle stores an OffsetTime in a TIMESTAMP(6) WITH TIME ZONE
    case _: OracleProfile   => Precision(6, rounds = true)
    case _                  => nanos
  }

  def localTime(v: LocalTime): LocalTime = localTimePrecision(v)
  def localDateTime(v: LocalDateTime): LocalDateTime = profile match {
    // SQLite keeps the mapping of Slick 3 through java.sql.Timestamp, which moves a time in a DST gap of the JVM
    // time zone
    case _: SQLiteProfile => localDateTimePrecision(java.sql.Timestamp.valueOf(v).toLocalDateTime)
    case _                => localDateTimePrecision(v)
  }
  def instant(v: Instant): Instant = instantPrecision(v.atOffset(ZoneOffset.UTC)).toInstant

  def offsetTime(v: OffsetTime): OffsetTime =
    offsetTimePrecision(if (offsetTimeKeepsOffset) v else v.withOffsetSameInstant(ZoneOffset.UTC))

  def offsetDateTime(v: OffsetDateTime): OffsetDateTime =
    offsetDateTimePrecision(if (offsetDateTimeKeepsOffset) v else v.withOffsetSameInstant(ZoneOffset.UTC))

  def zonedDateTime(v: ZonedDateTime): ZonedDateTime =
    offsetDateTimePrecision(
      if (zonedDateTimeKeepsRegion) v
      else if (offsetDateTimeKeepsOffset) v.toOffsetDateTime.toZonedDateTime
      else v.withZoneSameInstant(ZoneOffset.UTC))
}
