package slick.jdbc

import java.sql.{Array as _, *}
import java.time.*
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField
import java.util.{Calendar, TimeZone, UUID}

import scala.reflect.ClassTag

import slick.SlickException
import slick.ast.*
import slick.relational.{RelationalProfile, RelationalTypesComponent}

trait JdbcTypesComponent extends RelationalTypesComponent { self: JdbcProfile =>

  /** How this profile stores the java.time types: `native` (the default) uses the database's own date and
    * time types where it has them, `text` the mappings of Slick 3, which store several types as text, for
    * databases with tables created by Slick 3 that cannot be migrated yet. It is read from the profile
    * configuration key `javaTimeStorage`, e.g. `slick.jdbc.MySQLProfile.javaTimeStorage = text`. */
  protected def javaTimeStorage: String

  /** Whether [[javaTimeStorage]] is `text`, i.e. the java.time types use the mappings of Slick 3 */
  protected final def useTextJavaTimeStorage: Boolean =
    this.javaTimeStorage == JdbcTypesComponent.JavaTimeStorageText

  abstract class MappedJdbcType[T, U](implicit val tmd: JdbcType[U], val classTag: ClassTag[T]) extends JdbcType[T] {
    def map(t: T): U
    def comap(u: U): T

    def newSqlType: Option[Int] = None
    def newSqlTypeName(size: Option[FieldSymbol]): Option[String] = None
    def newValueToSQLLiteral(value: T): Option[String] = None
    def newHasLiteralForm: Option[Boolean] = None

    def sqlType = newSqlType.getOrElse(tmd.sqlType)
    def sqlTypeName(sym: Option[FieldSymbol]) = newSqlTypeName(sym).getOrElse(tmd.sqlTypeName(sym))
    def setValue(v: T, p: PreparedStatement, idx: Int) = tmd.setValue(map(v), p, idx)
    def setNull(p: PreparedStatement, idx: Int): Unit = tmd.setNull(p, idx)
    def getValue(r: ResultSet, idx: Int) = {
      val v = tmd.getValue(r, idx)
      if((v.asInstanceOf[AnyRef] eq null) || tmd.wasNull(r, idx)) null.asInstanceOf[T]
      else comap(v)
    }
    def wasNull(r: ResultSet, idx: Int) = tmd.wasNull(r, idx)
    def updateValue(v: T, r: ResultSet, idx: Int) = tmd.updateValue(map(v), r, idx)
    def valueToSQLLiteral(value: T) = newValueToSQLLiteral(value).getOrElse(tmd.valueToSQLLiteral(map(value)))
    def hasLiteralForm = newHasLiteralForm.getOrElse(tmd.hasLiteralForm)
    override def hasLiteralFormFor(value: T) = newHasLiteralForm.getOrElse(tmd.hasLiteralFormFor(map(value)))
    override def scalaType: ScalaBaseType[T] = ScalaBaseType[T]
    override def toString = s"MappedJdbcType[${classTag.runtimeClass.getName} -> $tmd]"
    override def hashCode = tmd.hashCode() + classTag.hashCode()
    override def equals(o: Any) = o match {
      case o: MappedJdbcType[?, ?] => tmd == o.tmd && classTag == o.classTag
      case _                       => false
    }
  }

  object MappedJdbcType extends MappedColumnTypeFactory {
    def base[T : ClassTag, U : BaseColumnType](tmap: T => U, tcomap: U => T): BaseColumnType[T] = {
      assertNonNullType(implicitly[BaseColumnType[U]])
      new MappedJdbcType[T, U] with BaseTypedType[T] {
        def map(t: T) = tmap(t)
        def comap(u: U) = tcomap(u)
      }
    }
  }

  object JdbcType {
    def unapply(t: Type) = Some((jdbcTypeFor(t), t.isInstanceOf[OptionType]))
  }

  def jdbcTypeFor(t: Type): JdbcType[Any] = ((t.structural match {
    case tmd: JdbcType[?]          => tmd
    case ScalaBaseType.booleanType => columnTypes.booleanJdbcType
    case ScalaBaseType.bigDecimalType => columnTypes.bigDecimalJdbcType
    case ScalaBaseType.byteType => columnTypes.byteJdbcType
    case ScalaBaseType.charType => columnTypes.charJdbcType
    case ScalaBaseType.doubleType     => columnTypes.doubleJdbcType
    case ScalaBaseType.floatType      => columnTypes.floatJdbcType
    case ScalaBaseType.intType        => columnTypes.intJdbcType
    case ScalaBaseType.longType       => columnTypes.longJdbcType
    case ScalaBaseType.nullType       => columnTypes.nullJdbcType
    case ScalaBaseType.shortType      => columnTypes.shortJdbcType
    case ScalaBaseType.stringType     => columnTypes.stringJdbcType
    case t: OptionType                => jdbcTypeFor(t.elementType)
    case t: ErasedScalaBaseType[?, ?] => jdbcTypeFor(t.erasure)
    case t                            => throw new SlickException("JdbcProfile has no JdbcType for type "+t)
  }): JdbcType[?]).asInstanceOf[JdbcType[Any]]

  def defaultSqlTypeName(tmd: JdbcType[?], sym: Option[FieldSymbol]): String = tmd.sqlType match {
    case java.sql.Types.VARCHAR =>
      val size = sym.flatMap(_.findColumnOption[RelationalProfile.ColumnOption.Length])
      size.fold("VARCHAR(254)")(l => if(l.varying) s"VARCHAR(${l.length})" else s"CHAR(${l.length})")
    case java.sql.Types.DECIMAL => "DECIMAL(21,2)"
    case java.sql.Types.TIME_WITH_TIMEZONE => "TIME WITH TIME ZONE"
    case java.sql.Types.TIMESTAMP_WITH_TIMEZONE => "TIMESTAMP WITH TIME ZONE"
    case t => JdbcTypesComponent.typeNames.getOrElse(t,
      throw new SlickException("No SQL type name found in java.sql.Types for code "+t))
  }

  abstract class DriverJdbcType[T](implicit val classTag: ClassTag[T]) extends JdbcType[T] {
    override def scalaType: ScalaBaseType[T] = ScalaBaseType[T]
    def sqlTypeName(sym: Option[FieldSymbol]): String = self.defaultSqlTypeName(this, sym)
    def valueToSQLLiteral(value: T) =
      if(hasLiteralForm) value.toString
      else throw new SlickException(sqlTypeName(None) + " does not have a literal representation")
    def hasLiteralForm = true
    def wasNull(r: ResultSet, idx: Int) = r.wasNull()
    def setNull(p: PreparedStatement, idx: Int): Unit = p.setNull(idx, sqlType)
  }

  class JdbcTypes {
    val booleanJdbcType = new BooleanJdbcType
    val blobJdbcType = new BlobJdbcType
    val byteJdbcType = new ByteJdbcType
    val byteArrayJdbcType = new ByteArrayJdbcType
    val charJdbcType = new CharJdbcType
    val clobJdbcType = new ClobJdbcType
    val dateJdbcType = new DateJdbcType
    val offsetDateTimeType: OffsetDateTimeJdbcType =
      if (useTextJavaTimeStorage) new LegacyOffsetDateTimeJdbcType else new OffsetDateTimeJdbcType
    val zonedDateType: ZonedDateTimeJdbcType =
      if (useTextJavaTimeStorage) new LegacyZonedDateTimeJdbcType else new ZonedDateTimeJdbcType
    val localTimeType: LocalTimeJdbcType =
      if (useTextJavaTimeStorage) new TextLocalTimeJdbcType else new LocalTimeJdbcType
    val localDateType: LocalDateJdbcType =
      if (useTextJavaTimeStorage) new LegacyLocalDateJdbcType else new LocalDateJdbcType
    val localDateTimeType: LocalDateTimeJdbcType =
      if (useTextJavaTimeStorage) new LegacyLocalDateTimeJdbcType else new LocalDateTimeJdbcType
    val offsetTimeType: OffsetTimeJdbcType =
      if (useTextJavaTimeStorage) new LegacyOffsetTimeJdbcType else new OffsetTimeJdbcType
    val instantType: InstantJdbcType =
      if (useTextJavaTimeStorage) new LegacyInstantJdbcType else new InstantJdbcType
    val doubleJdbcType = new DoubleJdbcType
    val floatJdbcType = new FloatJdbcType
    val intJdbcType = new IntJdbcType
    val longJdbcType = new LongJdbcType
    val shortJdbcType = new ShortJdbcType
    val stringJdbcType = new StringJdbcType
    val timeJdbcType = new TimeJdbcType
    val timestampJdbcType = new TimestampJdbcType
    val uuidJdbcType = new UUIDJdbcType
    val bigDecimalJdbcType = new BigDecimalJdbcType
    val nullJdbcType = new NullJdbcType

    class BooleanJdbcType extends DriverJdbcType[Boolean] {
      def sqlType = java.sql.Types.BOOLEAN
      def setValue(v: Boolean, p: PreparedStatement, idx: Int) = p.setBoolean(idx, v)
      def getValue(r: ResultSet, idx: Int) = r.getBoolean(idx)
      def updateValue(v: Boolean, r: ResultSet, idx: Int) = r.updateBoolean(idx, v)
    }

    class BlobJdbcType extends DriverJdbcType[Blob] {
      def sqlType = java.sql.Types.BLOB
      def setValue(v: Blob, p: PreparedStatement, idx: Int) = p.setBlob(idx, v)
      def getValue(r: ResultSet, idx: Int) = r.getBlob(idx)
      def updateValue(v: Blob, r: ResultSet, idx: Int) = r.updateBlob(idx, v)
      override def hasLiteralForm = false
    }

    class ByteJdbcType extends DriverJdbcType[Byte] with NumericTypedType {
      def sqlType = java.sql.Types.TINYINT
      def setValue(v: Byte, p: PreparedStatement, idx: Int) = p.setByte(idx, v)
      def getValue(r: ResultSet, idx: Int) = r.getByte(idx)
      def updateValue(v: Byte, r: ResultSet, idx: Int) = r.updateByte(idx, v)
    }

    class ByteArrayJdbcType extends DriverJdbcType[Array[Byte]] {
      def sqlType = java.sql.Types.BLOB
      def setValue(v: Array[Byte], p: PreparedStatement, idx: Int) = p.setBytes(idx, v)
      def getValue(r: ResultSet, idx: Int) = r.getBytes(idx)
      def updateValue(v: Array[Byte], r: ResultSet, idx: Int) = r.updateBytes(idx, v)
      override def hasLiteralForm = false
    }

    class ClobJdbcType extends DriverJdbcType[Clob] {
      def sqlType = java.sql.Types.CLOB
      def setValue(v: Clob, p: PreparedStatement, idx: Int) = p.setClob(idx, v)
      def getValue(r: ResultSet, idx: Int) = r.getClob(idx)
      def updateValue(v: Clob, r: ResultSet, idx: Int) = r.updateClob(idx, v)
      override def hasLiteralForm = false
    }

    class CharJdbcType extends DriverJdbcType[Char] {
      def sqlType = java.sql.Types.CHAR
      override def sqlTypeName(sym: Option[FieldSymbol]) = "CHAR(1)"
      def setValue(v: Char, p: PreparedStatement, idx: Int) = stringJdbcType.setValue(String.valueOf(v), p, idx)
      def getValue(r: ResultSet, idx: Int) = {
        val s = stringJdbcType.getValue(r, idx)
        if(s == null || s.isEmpty) ' ' else s.charAt(0)
      }
      def updateValue(v: Char, r: ResultSet, idx: Int) = stringJdbcType.updateValue(String.valueOf(v), r, idx)
      override def valueToSQLLiteral(v: Char) = stringJdbcType.valueToSQLLiteral(String.valueOf(v))
    }

    /**
      * Use [[ZonedDateTimeJdbcType]] or [[OffsetDateTimeJdbcType]] or [[LocalTimeJdbcType]]
      * or `java.time.OffsetTime` or `java.time.LocalDateTime` or `java.time.LocalDate`
      * or `java.time.Instant` instead.
      */
    class DateJdbcType extends DriverJdbcType[Date] {
      def sqlType = java.sql.Types.DATE
      def setValue(v: Date, p: PreparedStatement, idx: Int) = p.setDate(idx, v)
      def getValue(r: ResultSet, idx: Int) = r.getDate(idx)
      def updateValue(v: Date, r: ResultSet, idx: Int) = r.updateDate(idx, v)
      override def valueToSQLLiteral(value: Date) = s"{d '${value.toString}'}"
    }

    /* The java.time types. The classes named after the types (e.g. InstantJdbcType) bind and read the
     * values with the JDBC 4.2 methods setObject and getObject. The profiles override them where the
     * database or its driver needs something else. The Text* classes store a value as text, the
     * StringBound* and UtcCalendar* classes bind and read a native DATE or TIMESTAMP column for drivers
     * without java.time support, the UtcTimestamp* classes store an instant as the date and time in UTC
     * for databases without a time zone type, and the Legacy* classes are the mappings of Slick 3, which
     * a profile uses with `javaTimeStorage = text`. Only the Legacy* classes depend on the JVM time zone. */

    class LocalDateJdbcType extends DriverJdbcType[LocalDate] {
      override def sqlType: Int = java.sql.Types.DATE
      override def setValue(v: LocalDate, p: PreparedStatement, idx: Int): Unit = p.setObject(idx, v)
      override def getValue(r: ResultSet, idx: Int): LocalDate = r.getObject(idx, classOf[LocalDate])
      override def updateValue(v: LocalDate, r: ResultSet, idx: Int): Unit = r.updateObject(idx, v)
      override def valueToSQLLiteral(value: LocalDate) = s"'${value.toString}'"
    }

    class LocalTimeJdbcType extends DriverJdbcType[LocalTime] {
      override def sqlType: Int = java.sql.Types.TIME
      override def setValue(v: LocalTime, p: PreparedStatement, idx: Int): Unit = p.setObject(idx, v)
      override def getValue(r: ResultSet, idx: Int): LocalTime = r.getObject(idx, classOf[LocalTime])
      override def updateValue(v: LocalTime, r: ResultSet, idx: Int): Unit = r.updateObject(idx, v)
      override def valueToSQLLiteral(value: LocalTime) = s"'${JavaTimeFormats.localTime.format(value)}'"
    }

    class LocalDateTimeJdbcType extends DriverJdbcType[LocalDateTime] {
      override def sqlType: Int = java.sql.Types.TIMESTAMP
      override def setValue(v: LocalDateTime, p: PreparedStatement, idx: Int): Unit = p.setObject(idx, v)
      override def getValue(r: ResultSet, idx: Int): LocalDateTime = r.getObject(idx, classOf[LocalDateTime])
      override def updateValue(v: LocalDateTime, r: ResultSet, idx: Int): Unit = r.updateObject(idx, v)
      override def valueToSQLLiteral(value: LocalDateTime) = s"'${JavaTimeFormats.localDateTime.format(value)}'"
    }

    /** An [[Instant]] in a TIMESTAMP WITH TIME ZONE column. It is bound as an [[OffsetDateTime]] in UTC,
      * because JDBC 4.2 has no mapping for Instant. */
    class InstantJdbcType extends DriverJdbcType[Instant] {
      override def sqlType: Int = java.sql.Types.TIMESTAMP_WITH_TIMEZONE
      override def setValue(v: Instant, p: PreparedStatement, idx: Int): Unit = p.setObject(idx, v.atOffset(ZoneOffset.UTC))
      override def getValue(r: ResultSet, idx: Int): Instant = r.getObject(idx, classOf[OffsetDateTime]) match {
        case null  => null
        case value => value.toInstant
      }
      override def updateValue(v: Instant, r: ResultSet, idx: Int): Unit = r.updateObject(idx, v.atOffset(ZoneOffset.UTC))
      override def valueToSQLLiteral(value: Instant) =
        s"TIMESTAMP WITH TIME ZONE '${JavaTimeFormats.offsetDateTime.format(value.atOffset(ZoneOffset.UTC))}'"
    }

    class OffsetTimeJdbcType extends DriverJdbcType[OffsetTime] {
      override def sqlType: Int = java.sql.Types.TIME_WITH_TIMEZONE
      override def setValue(v: OffsetTime, p: PreparedStatement, idx: Int): Unit = p.setObject(idx, v)
      override def getValue(r: ResultSet, idx: Int): OffsetTime = r.getObject(idx, classOf[OffsetTime])
      override def updateValue(v: OffsetTime, r: ResultSet, idx: Int): Unit = r.updateObject(idx, v)
      override def valueToSQLLiteral(value: OffsetTime) =
        s"TIME WITH TIME ZONE '${JavaTimeFormats.offsetTime.format(value)}'"
    }

    class OffsetDateTimeJdbcType extends DriverJdbcType[OffsetDateTime] {
      override def sqlType: Int = java.sql.Types.TIMESTAMP_WITH_TIMEZONE
      override def setValue(v: OffsetDateTime, p: PreparedStatement, idx: Int): Unit = p.setObject(idx, v)
      override def getValue(r: ResultSet, idx: Int): OffsetDateTime = r.getObject(idx, classOf[OffsetDateTime])
      override def updateValue(v: OffsetDateTime, r: ResultSet, idx: Int): Unit = r.updateObject(idx, v)
      override def valueToSQLLiteral(value: OffsetDateTime) =
        s"TIMESTAMP WITH TIME ZONE '${JavaTimeFormats.offsetDateTime.format(value)}'"
    }

    /** A [[ZonedDateTime]] in a TIMESTAMP WITH TIME ZONE column. It is bound as an [[OffsetDateTime]], because
      * JDBC 4.2 has no mapping for ZonedDateTime, so the zone region is lost: the value is read back with its
      * offset as the zone. */
    class ZonedDateTimeJdbcType extends DriverJdbcType[ZonedDateTime] {
      override def sqlType: Int = java.sql.Types.TIMESTAMP_WITH_TIMEZONE
      override def setValue(v: ZonedDateTime, p: PreparedStatement, idx: Int): Unit = p.setObject(idx, v.toOffsetDateTime)
      override def getValue(r: ResultSet, idx: Int): ZonedDateTime = r.getObject(idx, classOf[OffsetDateTime]) match {
        case null  => null
        case value => value.toZonedDateTime
      }
      override def updateValue(v: ZonedDateTime, r: ResultSet, idx: Int): Unit = r.updateObject(idx, v.toOffsetDateTime)
      override def valueToSQLLiteral(value: ZonedDateTime) =
        s"TIMESTAMP WITH TIME ZONE '${JavaTimeFormats.offsetDateTime.format(value)}'"
    }

    /** A [[LocalTime]] stored as text, for databases without a TIME type that keeps fractional seconds. The
      * format, `HH:mm:ss` with only as many fractional digits as needed, is canonical and sorts in time
      * order. It is also the LocalTime mapping of Slick 3. */
    class TextLocalTimeJdbcType extends LocalTimeJdbcType {
      override def sqlType: Int = java.sql.Types.VARCHAR
      override def setValue(v: LocalTime, p: PreparedStatement, idx: Int): Unit =
        p.setString(idx, JavaTimeFormats.localTime.format(v))
      override def getValue(r: ResultSet, idx: Int): LocalTime = r.getString(idx) match {
        case null => null
        case iso8601String => LocalTime.parse(iso8601String)
      }
      override def updateValue(v: LocalTime, r: ResultSet, idx: Int): Unit =
        r.updateString(idx, JavaTimeFormats.localTime.format(v))
      override def valueToSQLLiteral(value: LocalTime) = s"'${JavaTimeFormats.localTime.format(value)}'"
    }

    /** An [[OffsetTime]] stored as text, for databases without a time type that keeps an offset. The value is
      * normalized to UTC (12:00+02:00 is stored as 10:00Z and read back as 10:00Z), and written with a fixed
      * width, so that the text sorts in time order. Values written by Slick 3, with their own offsets, are read
      * and normalized too. */
    class TextOffsetTimeJdbcType extends OffsetTimeJdbcType {
      override def sqlType: Int = java.sql.Types.VARCHAR
      protected def serialize(v: OffsetTime): String =
        JavaTimeFormats.utcTime.format(v.withOffsetSameInstant(ZoneOffset.UTC))
      override def setValue(v: OffsetTime, p: PreparedStatement, idx: Int): Unit = p.setString(idx, serialize(v))
      override def getValue(r: ResultSet, idx: Int): OffsetTime = r.getString(idx) match {
        case null => null
        case text => OffsetTime.parse(text).withOffsetSameInstant(ZoneOffset.UTC)
      }
      override def updateValue(v: OffsetTime, r: ResultSet, idx: Int): Unit = r.updateString(idx, serialize(v))
      override def valueToSQLLiteral(value: OffsetTime) = s"'${serialize(value)}'"
    }

    /** An [[Instant]] stored as text in UTC, with a fixed width, so that the text sorts in time order. Values
      * written by Slick 3 (`Instant.toString`) are read too. */
    class TextInstantJdbcType extends InstantJdbcType {
      override def sqlType: Int = java.sql.Types.VARCHAR
      protected def serialize(v: Instant): String = JavaTimeFormats.utcDateTime.format(v)
      override def setValue(v: Instant, p: PreparedStatement, idx: Int): Unit = p.setString(idx, serialize(v))
      override def getValue(r: ResultSet, idx: Int): Instant = r.getString(idx) match {
        case null => null
        case text => Instant.parse(text)
      }
      override def updateValue(v: Instant, r: ResultSet, idx: Int): Unit = r.updateString(idx, serialize(v))
      override def valueToSQLLiteral(value: Instant) = s"'${serialize(value)}'"
    }

    /** An [[OffsetDateTime]] stored as text, normalized to UTC like [[TextOffsetTimeJdbcType]]. */
    class TextOffsetDateTimeJdbcType extends OffsetDateTimeJdbcType {
      override def sqlType: Int = java.sql.Types.VARCHAR
      protected def serialize(v: OffsetDateTime): String = JavaTimeFormats.utcDateTime.format(v.toInstant)
      override def setValue(v: OffsetDateTime, p: PreparedStatement, idx: Int): Unit = p.setString(idx, serialize(v))
      override def getValue(r: ResultSet, idx: Int): OffsetDateTime = r.getString(idx) match {
        case null => null
        case text => OffsetDateTime.parse(text).withOffsetSameInstant(ZoneOffset.UTC)
      }
      override def updateValue(v: OffsetDateTime, r: ResultSet, idx: Int): Unit = r.updateString(idx, serialize(v))
      override def valueToSQLLiteral(value: OffsetDateTime) = s"'${serialize(value)}'"
    }

    /** A [[ZonedDateTime]] stored as text, normalized to UTC like [[TextOffsetTimeJdbcType]]: the zone region
      * is lost, and the value is read back with the zone [[ZoneOffset.UTC]]. */
    class TextZonedDateTimeJdbcType extends ZonedDateTimeJdbcType {
      override def sqlType: Int = java.sql.Types.VARCHAR
      protected def serialize(v: ZonedDateTime): String = JavaTimeFormats.utcDateTime.format(v.toInstant)
      override def setValue(v: ZonedDateTime, p: PreparedStatement, idx: Int): Unit = p.setString(idx, serialize(v))
      override def getValue(r: ResultSet, idx: Int): ZonedDateTime = r.getString(idx) match {
        case null => null
        case text => ZonedDateTime.parse(text).withZoneSameInstant(ZoneOffset.UTC)
      }
      override def updateValue(v: ZonedDateTime, r: ResultSet, idx: Int): Unit = r.updateString(idx, serialize(v))
      override def valueToSQLLiteral(value: ZonedDateTime) = s"'${serialize(value)}'"
    }

    /** A [[LocalDate]] in a DATE column, bound and read as a `yyyy-MM-dd` string, for drivers that support
      * no java.time types. */
    class StringBoundLocalDateJdbcType extends LocalDateJdbcType {
      override def setValue(v: LocalDate, p: PreparedStatement, idx: Int): Unit = p.setString(idx, v.toString)
      override def getValue(r: ResultSet, idx: Int): LocalDate = r.getString(idx) match {
        case null => null
        case text => LocalDate.parse(text.substring(0, 10))
      }
      override def updateValue(v: LocalDate, r: ResultSet, idx: Int): Unit = r.updateString(idx, v.toString)
    }

    /** A [[LocalDateTime]] in a TIMESTAMP column, bound and read as a java.sql.Timestamp of the same date and time
      * in UTC, with a UTC Calendar, for drivers that support no java.time types. The driver then converts the
      * date and time in UTC, which has no DST gaps, rather than in the JVM time zone, in which a time in a DST gap
      * would move by an hour. ResultSet.updateTimestamp takes no Calendar, so an update of a mutable result set
      * binds a string, which the driver may convert in the JVM time zone. */
    class UtcCalendarLocalDateTimeJdbcType extends LocalDateTimeJdbcType {
      private[this] def utcCalendar = Calendar.getInstance(TimeZone.getTimeZone(ZoneOffset.UTC))
      override def setValue(v: LocalDateTime, p: PreparedStatement, idx: Int): Unit =
        p.setTimestamp(idx, Timestamp.from(v.toInstant(ZoneOffset.UTC)), utcCalendar)
      override def getValue(r: ResultSet, idx: Int): LocalDateTime = r.getTimestamp(idx, utcCalendar) match {
        case null  => null
        case value => LocalDateTime.ofInstant(value.toInstant, ZoneOffset.UTC)
      }
      override def updateValue(v: LocalDateTime, r: ResultSet, idx: Int): Unit =
        r.updateString(idx, JavaTimeFormats.localDateTime.format(v))
    }

    /** An [[Instant]] in a TIMESTAMP column (without time zone) that holds the date and time in UTC, for
      * databases without a time zone type. `utc` binds and reads the UTC date and time. */
    class UtcTimestampInstantJdbcType(utc: LocalDateTimeJdbcType) extends InstantJdbcType {
      override def sqlType: Int = utc.sqlType
      override def sqlTypeName(sym: Option[FieldSymbol]): String = utc.sqlTypeName(sym)
      override def setValue(v: Instant, p: PreparedStatement, idx: Int): Unit =
        utc.setValue(LocalDateTime.ofInstant(v, ZoneOffset.UTC), p, idx)
      override def getValue(r: ResultSet, idx: Int): Instant = utc.getValue(r, idx) match {
        case null  => null
        case value => value.toInstant(ZoneOffset.UTC)
      }
      override def updateValue(v: Instant, r: ResultSet, idx: Int): Unit =
        utc.updateValue(LocalDateTime.ofInstant(v, ZoneOffset.UTC), r, idx)
      override def hasLiteralForm: Boolean = utc.hasLiteralForm
      override def valueToSQLLiteral(value: Instant) = utc.valueToSQLLiteral(LocalDateTime.ofInstant(value, ZoneOffset.UTC))
    }

    /** An [[OffsetDateTime]] in a TIMESTAMP column (without time zone) that holds the date and time in UTC, like
      * [[UtcTimestampInstantJdbcType]]. The value is read back in UTC. */
    class UtcTimestampOffsetDateTimeJdbcType(utc: LocalDateTimeJdbcType) extends OffsetDateTimeJdbcType {
      private val instant = new UtcTimestampInstantJdbcType(utc)
      override def sqlType: Int = utc.sqlType
      override def sqlTypeName(sym: Option[FieldSymbol]): String = utc.sqlTypeName(sym)
      override def setValue(v: OffsetDateTime, p: PreparedStatement, idx: Int): Unit = instant.setValue(v.toInstant, p, idx)
      override def getValue(r: ResultSet, idx: Int): OffsetDateTime = instant.getValue(r, idx) match {
        case null  => null
        case value => value.atOffset(ZoneOffset.UTC)
      }
      override def updateValue(v: OffsetDateTime, r: ResultSet, idx: Int): Unit = instant.updateValue(v.toInstant, r, idx)
      override def hasLiteralForm: Boolean = utc.hasLiteralForm
      override def valueToSQLLiteral(value: OffsetDateTime) = instant.valueToSQLLiteral(value.toInstant)
    }

    /** A [[ZonedDateTime]] in a TIMESTAMP column (without time zone) that holds the date and time in UTC, like
      * [[UtcTimestampInstantJdbcType]]. The value is read back with the zone [[ZoneOffset.UTC]]. */
    class UtcTimestampZonedDateTimeJdbcType(utc: LocalDateTimeJdbcType) extends ZonedDateTimeJdbcType {
      private val instant = new UtcTimestampInstantJdbcType(utc)
      override def sqlType: Int = utc.sqlType
      override def sqlTypeName(sym: Option[FieldSymbol]): String = utc.sqlTypeName(sym)
      override def setValue(v: ZonedDateTime, p: PreparedStatement, idx: Int): Unit = instant.setValue(v.toInstant, p, idx)
      override def getValue(r: ResultSet, idx: Int): ZonedDateTime = instant.getValue(r, idx) match {
        case null  => null
        case value => value.atZone(ZoneOffset.UTC)
      }
      override def updateValue(v: ZonedDateTime, r: ResultSet, idx: Int): Unit = instant.updateValue(v.toInstant, r, idx)
      override def hasLiteralForm: Boolean = utc.hasLiteralForm
      override def valueToSQLLiteral(value: ZonedDateTime) = instant.valueToSQLLiteral(value.toInstant)
    }

    /* The java.time mappings of Slick 3, used when `useTextJavaTimeStorage` is set */

    class LegacyLocalDateJdbcType extends LocalDateJdbcType {
      override def setValue(v: LocalDate, p: PreparedStatement, idx: Int) : Unit = {
        p.setDate(idx, Date.valueOf(v))
      }
      override def getValue(r: ResultSet, idx: Int) : LocalDate = {
        r.getDate(idx) match {
          case null => null
          case date => date.toLocalDate
        }
      }
      override def updateValue(v: LocalDate, r: ResultSet, idx: Int) : Unit = {
        r.updateDate(idx, Date.valueOf(v))
      }
      override def valueToSQLLiteral(value: LocalDate) = {
        s"'${value.toString}'"
      }
    }

    class LegacyLocalDateTimeJdbcType extends LocalDateTimeJdbcType {
      override def setValue(v: LocalDateTime, p: PreparedStatement, idx: Int) : Unit = {
        p.setTimestamp(idx, Timestamp.valueOf(v))
      }
      override def getValue(r: ResultSet, idx: Int) : LocalDateTime = {
        r.getTimestamp(idx) match {
          case null => null
          case timestamp => timestamp.toLocalDateTime
        }
      }
      override def updateValue(v: LocalDateTime, r: ResultSet, idx: Int) : Unit = {
        r.updateTimestamp(idx, Timestamp.valueOf(v))
      }
      override def valueToSQLLiteral(value: LocalDateTime) = {
        s"'${Timestamp.valueOf(value)}'"
      }
    }

    class LegacyInstantJdbcType extends InstantJdbcType {
      override def sqlType : Int = {
        java.sql.Types.TIMESTAMP
      }
      override def setValue(v: Instant, p: PreparedStatement, idx: Int) : Unit = {
        p.setTimestamp(idx, Timestamp.from(v))
      }
      override def getValue(r: ResultSet, idx: Int) : Instant = {
        r.getTimestamp(idx) match {
          case null => null
          case timestamp => timestamp.toInstant
        }
      }
      override def updateValue(v: Instant, r: ResultSet, idx: Int) : Unit = {
        r.updateTimestamp(idx, Timestamp.from(v))
      }
      override def valueToSQLLiteral(value: Instant) = {
        s"'${Timestamp.from(value)}'"
      }
    }

    class LegacyOffsetTimeJdbcType extends OffsetTimeJdbcType {
      override def sqlType : Int = java.sql.Types.VARCHAR
      override def setValue(v: OffsetTime, p: PreparedStatement, idx: Int) : Unit = {
        p.setString(idx, v.toString)
      }
      override def getValue(r: ResultSet, idx: Int) : OffsetTime = {
        r.getString(idx) match {
          case null => null
          case iso8601String => OffsetTime.parse(iso8601String)
        }
      }
      override def updateValue(v: OffsetTime, r: ResultSet, idx: Int) : Unit = {
        r.updateString(idx, v.toString)
      }
      override def valueToSQLLiteral(value: OffsetTime) = {
        s"'${value.toString}'"
      }
    }

    class LegacyOffsetDateTimeJdbcType extends OffsetDateTimeJdbcType {
      override def sqlType : Int = java.sql.Types.VARCHAR
      override def setValue(v: OffsetDateTime, p: PreparedStatement, idx: Int): Unit = {
        p.setString(idx, v.toString)
      }
      override def getValue(r: ResultSet, idx: Int): OffsetDateTime = {
        r.getString(idx) match {
          case null => null
          case iso8601String : String => OffsetDateTime.parse(iso8601String)
        }
      }
      override def updateValue(v: OffsetDateTime, r: ResultSet, idx: Int): Unit = {
        r.updateString(idx, v.toString)
      }
      override def valueToSQLLiteral(value: OffsetDateTime) = {
        s"'${value.toString}'"
      }
    }

    class LegacyZonedDateTimeJdbcType extends ZonedDateTimeJdbcType {
      override def sqlType : Int = java.sql.Types.VARCHAR
      override def setValue(v: ZonedDateTime, p: PreparedStatement, idx: Int) : Unit = {
        p.setString(idx, v.toString)
      }
      override def getValue(r: ResultSet, idx: Int) : ZonedDateTime = {
        r.getString(idx) match {
          case null => null
          case iso8601String => ZonedDateTime.parse(iso8601String)
        }
      }
      override def updateValue(v: ZonedDateTime, r: ResultSet, idx: Int) = {
        r.updateString(idx, v.toString)
      }
      override def valueToSQLLiteral(value: ZonedDateTime) = {
        s"'${value.toString}'"
      }
    }

    class DoubleJdbcType extends DriverJdbcType[Double] with NumericTypedType {
      def sqlType = java.sql.Types.DOUBLE
      def setValue(v: Double, p: PreparedStatement, idx: Int) = p.setDouble(idx, v)
      def getValue(r: ResultSet, idx: Int) = r.getDouble(idx)
      def updateValue(v: Double, r: ResultSet, idx: Int) = r.updateDouble(idx, v)
      // NaN and the infinities have no numeric literal form. Queries send them as bind variables, so this cast
      // is only used where a literal is required (e.g. column defaults) and works only on some databases
      override def valueToSQLLiteral(value: Double) =
        if (java.lang.Double.isFinite(value)) value.toString else s"cast('$value' as ${sqlTypeName(None)})"
      override def hasLiteralFormFor(value: Double) = hasLiteralForm && java.lang.Double.isFinite(value)
    }

    class FloatJdbcType extends DriverJdbcType[Float] with NumericTypedType {
      // see http://docs.oracle.com/javase/1.5.0/docs/guide/jdbc/getstart/mapping.html#1055162
      def sqlType = java.sql.Types.REAL
      def setValue(v: Float, p: PreparedStatement, idx: Int) = p.setFloat(idx, v)
      def getValue(r: ResultSet, idx: Int) = r.getFloat(idx)
      def updateValue(v: Float, r: ResultSet, idx: Int) = r.updateFloat(idx, v)
      // NaN and the infinities have no numeric literal form. Queries send them as bind variables, so this cast
      // is only used where a literal is required (e.g. column defaults) and works only on some databases
      override def valueToSQLLiteral(value: Float) =
        if (java.lang.Float.isFinite(value)) value.toString else s"cast('$value' as ${sqlTypeName(None)})"
      override def hasLiteralFormFor(value: Float) = hasLiteralForm && java.lang.Float.isFinite(value)
    }

    class IntJdbcType extends DriverJdbcType[Int] with NumericTypedType {
      def sqlType = java.sql.Types.INTEGER
      def setValue(v: Int, p: PreparedStatement, idx: Int) = p.setInt(idx, v)
      def getValue(r: ResultSet, idx: Int) = r.getInt(idx)
      def updateValue(v: Int, r: ResultSet, idx: Int) = r.updateInt(idx, v)
    }

    class LongJdbcType extends DriverJdbcType[Long] with NumericTypedType {
      def sqlType = java.sql.Types.BIGINT
      def setValue(v: Long, p: PreparedStatement, idx: Int) = p.setLong(idx, v)
      def getValue(r: ResultSet, idx: Int) = r.getLong(idx)
      def updateValue(v: Long, r: ResultSet, idx: Int) = r.updateLong(idx, v)
    }

    class ShortJdbcType extends DriverJdbcType[Short] with NumericTypedType {
      def sqlType = java.sql.Types.SMALLINT
      def setValue(v: Short, p: PreparedStatement, idx: Int) = p.setShort(idx, v)
      def getValue(r: ResultSet, idx: Int) = r.getShort(idx)
      def updateValue(v: Short, r: ResultSet, idx: Int) = r.updateShort(idx, v)
    }

    class StringJdbcType extends DriverJdbcType[String] {
      def sqlType = java.sql.Types.VARCHAR
      def setValue(v: String, p: PreparedStatement, idx: Int) = p.setString(idx, v)
      def getValue(r: ResultSet, idx: Int) = r.getString(idx)
      def updateValue(v: String, r: ResultSet, idx: Int) = r.updateString(idx, v)
      override def valueToSQLLiteral(value: String) = if(value eq null) "NULL" else {
        val sb = new StringBuilder
        sb append '\''
        for(c <- value) c match {
          case '\'' => sb append "''"
          case _ => sb append c
        }
        sb append '\''
        sb.toString
      }
    }

    class TimeJdbcType extends DriverJdbcType[Time] {
      def sqlType = java.sql.Types.TIME
      def setValue(v: Time, p: PreparedStatement, idx: Int) = p.setTime(idx, v)
      def getValue(r: ResultSet, idx: Int) = r.getTime(idx)
      def updateValue(v: Time, r: ResultSet, idx: Int) = r.updateTime(idx, v)
      override def valueToSQLLiteral(value: Time) = "{t '"+value.toString+"'}"
    }

    class TimestampJdbcType extends DriverJdbcType[Timestamp] {
      def sqlType = java.sql.Types.TIMESTAMP
      def setValue(v: Timestamp, p: PreparedStatement, idx: Int) = p.setTimestamp(idx, v)
      def getValue(r: ResultSet, idx: Int) = r.getTimestamp(idx)
      def updateValue(v: Timestamp, r: ResultSet, idx: Int) = r.updateTimestamp(idx, v)
      override def valueToSQLLiteral(value: Timestamp) = "{ts '"+value.toString+"'}"
    }

    class UUIDJdbcType extends DriverJdbcType[UUID] {
      def sqlType = java.sql.Types.OTHER
      def setValue(v: UUID, p: PreparedStatement, idx: Int) = p.setBytes(idx, toBytes(v))
      def getValue(r: ResultSet, idx: Int) = fromBytes(r.getBytes(idx))
      def updateValue(v: UUID, r: ResultSet, idx: Int) = r.updateBytes(idx, toBytes(v))
      override def hasLiteralForm = false
      def toBytes(uuid: UUID) = if(uuid eq null) null else {
        val msb = uuid.getMostSignificantBits
        val lsb = uuid.getLeastSignificantBits
        val buff = new Array[Byte](16)
        for (i <- 0 until 8) {
          buff(i) = ((msb >> (8 * (7 - i))) & 255).toByte
          buff(8 + i) = ((lsb >> (8 * (7 - i))) & 255).toByte
        }
        buff
      }
      def fromBytes(data: Array[Byte]) = if(data eq null) null else {
        var msb = 0L
        var lsb = 0L
        for (i <- 0 until 8) {
          msb = (msb << 8) | (data(i) & 0xff)
        }
        for (i <- 8 until 16) {
          lsb = (lsb << 8) | (data(i) & 0xff)
        }
        new UUID(msb, lsb)
      }
    }

    class BigDecimalJdbcType extends DriverJdbcType[BigDecimal] with NumericTypedType {
      def sqlType = java.sql.Types.DECIMAL
      def setValue(v: BigDecimal, p: PreparedStatement, idx: Int) = p.setBigDecimal(idx, v.bigDecimal)
      def getValue(r: ResultSet, idx: Int) = {
        val v = r.getBigDecimal(idx)
        if(v eq null) null else BigDecimal(v)
      }
      def updateValue(v: BigDecimal, r: ResultSet, idx: Int) = r.updateBigDecimal(idx, v.bigDecimal)
    }

    private[this] implicit def nullCt: ClassTag[Null] = ClassTag.Null
    class NullJdbcType extends DriverJdbcType[Null] {
      def sqlType = java.sql.Types.NULL
      def setValue(v: Null, p: PreparedStatement, idx: Int) = p.setString(idx, null)
      override def setNull(p: PreparedStatement, idx: Int) = p.setString(idx, null)
      def getValue(r: ResultSet, idx: Int): Null = null
      def updateValue(v: Null, r: ResultSet, idx: Int) = r.updateNull(idx)
      override def valueToSQLLiteral(value: Null) = "NULL"
    }
  }

  trait JdbcImplicitColumnTypes extends RelationalImplicitColumnTypes {
    implicit def booleanColumnType:        DriverJdbcType[Boolean] = columnTypes.booleanJdbcType
    implicit def blobColumnType:           DriverJdbcType[Blob] = columnTypes.blobJdbcType
    implicit def byteColumnType:           DriverJdbcType[Byte] with NumericTypedType = columnTypes.byteJdbcType
    implicit def byteArrayColumnType:      DriverJdbcType[Array[Byte]] = columnTypes.byteArrayJdbcType
    implicit def charColumnType:           DriverJdbcType[Char] = columnTypes.charJdbcType
    implicit def clobColumnType:           DriverJdbcType[Clob] = columnTypes.clobJdbcType
    implicit def dateColumnType:           DriverJdbcType[Date] = columnTypes.dateJdbcType
    implicit def doubleColumnType:         DriverJdbcType[Double] with NumericTypedType = columnTypes.doubleJdbcType
    implicit def floatColumnType:          DriverJdbcType[Float] with NumericTypedType = columnTypes.floatJdbcType
    implicit def intColumnType:            DriverJdbcType[Int] with NumericTypedType = columnTypes.intJdbcType
    implicit def longColumnType:           DriverJdbcType[Long] with NumericTypedType = columnTypes.longJdbcType
    implicit def shortColumnType:          DriverJdbcType[Short] with NumericTypedType = columnTypes.shortJdbcType
    implicit def stringColumnType:         DriverJdbcType[String] = columnTypes.stringJdbcType
    implicit def timeColumnType:           DriverJdbcType[Time] = columnTypes.timeJdbcType
    implicit def timestampColumnType:      DriverJdbcType[Timestamp] = columnTypes.timestampJdbcType
    implicit def uuidColumnType:           DriverJdbcType[UUID] = columnTypes.uuidJdbcType
    implicit def bigDecimalColumnType:     DriverJdbcType[BigDecimal] with NumericTypedType = columnTypes.bigDecimalJdbcType
    implicit def offsetDateTimeColumnType: DriverJdbcType[OffsetDateTime] = columnTypes.offsetDateTimeType
    implicit def zonedDateTimeColumnType:  DriverJdbcType[ZonedDateTime] = columnTypes.zonedDateType
    implicit def localTimeColumnType:      DriverJdbcType[LocalTime] = columnTypes.localTimeType
    implicit def localDateColumnType:      DriverJdbcType[LocalDate] = columnTypes.localDateType
    implicit def localDateTimeColumnType:  DriverJdbcType[LocalDateTime] = columnTypes.localDateTimeType
    implicit def offsetTimeColumnType:     DriverJdbcType[OffsetTime] = columnTypes.offsetTimeType
    implicit def instantColumnType:        DriverJdbcType[Instant] = columnTypes.instantType
  }
}

/** Text formats of the java.time values that Slick binds or stores as strings */
private[jdbc] object JavaTimeFormats {
  private val time9 = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS")

  /** `HH:mm:ss` with only as many fractional digits as needed. It is canonical and sorts in time order. */
  val localTime: DateTimeFormatter =
    new DateTimeFormatterBuilder()
      .appendPattern("HH:mm:ss")
      .optionalStart()
      .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
      .optionalEnd()
      .toFormatter()

  /** `yyyy-MM-dd HH:mm:ss.nnnnnnnnn` */
  val localDateTime: DateTimeFormatter =
    new DateTimeFormatterBuilder()
      .append(DateTimeFormatter.ISO_LOCAL_DATE)
      .appendLiteral(' ')
      .append(time9)
      .toFormatter()

  /** Reads a date and a time separated by a space or `T`, with or without seconds and fractional digits */
  val localDateTimeParser: DateTimeFormatter =
    new DateTimeFormatterBuilder()
      .append(DateTimeFormatter.ISO_LOCAL_DATE)
      .optionalStart().appendLiteral(' ').optionalEnd()
      .optionalStart().appendLiteral('T').optionalEnd()
      .append(DateTimeFormatter.ISO_LOCAL_TIME)
      .toFormatter()

  /** `HH:mm:ss.nnnnnnnnn+hh:mm` */
  val offsetTime: DateTimeFormatter =
    new DateTimeFormatterBuilder()
      .append(time9)
      .appendOffset("+HH:MM:ss", "+00:00")
      .toFormatter()

  /** `yyyy-MM-dd HH:mm:ss.nnnnnnnnn+hh:mm` */
  val offsetDateTime: DateTimeFormatter =
    new DateTimeFormatterBuilder()
      .append(localDateTime)
      .appendOffset("+HH:MM:ss", "+00:00")
      .toFormatter()

  /** `HH:mm:ss.nnnnnnnnnZ` for a time in UTC: fixed width, so that the text sorts in time order */
  val utcTime: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS'Z'")

  /** `yyyy-MM-ddTHH:mm:ss.nnnnnnnnnZ` for an instant, in UTC: fixed width (for years 0 to 9999), so that the
    * text sorts in time order */
  val utcDateTime: DateTimeFormatter =
    new DateTimeFormatterBuilder()
      .append(DateTimeFormatter.ISO_LOCAL_DATE)
      .appendLiteral('T')
      .append(utcTime)
      .toFormatter()
      .withZone(ZoneOffset.UTC)
}

object JdbcTypesComponent {
  /** The values of the profile configuration key `javaTimeStorage` */
  private[slick] val JavaTimeStorageNative = "native"
  private[slick] val JavaTimeStorageText = "text"

  private[slick] lazy val typeNames = Map() ++
    (for(f <- classOf[java.sql.Types].getFields)
      yield f.get(null).asInstanceOf[Int] -> f.getName)
}
