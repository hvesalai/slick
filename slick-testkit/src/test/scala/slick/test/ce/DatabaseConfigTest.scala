package slick.test.ce

import cats.effect.IO
import com.typesafe.config.ConfigFactory
import munit.CatsEffectSuite

import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile

/** Tests for [[DatabaseConfig.forConfig]] with an effect type parameter.
  *
  * Covers the fix for issue #3: `DatabaseConfig.forConfig[P, F]` must return a
  * `Resource[F, LoadedDatabaseConfig[P, F]]` with a live database instead of
  * calling the old `createDatabase` stub that threw `SlickException`.
  */
class DatabaseConfigTest extends CatsEffectSuite {

  private val h2Config = ConfigFactory.parseString(
    """
      |mydb {
      |  profile = "slick.jdbc.H2Profile$"
      |  db {
      |    connectionPool = disabled
      |    driver = "org.h2.Driver"
      |    url = "jdbc:h2:mem:dbconfigtest;DB_CLOSE_DELAY=-1"
      |  }
      |}
      |""".stripMargin
  )

  // ---------------------------------------------------------------------------
  // Profile-only forConfig (no db) — must still work
  // ---------------------------------------------------------------------------

  test("forConfig[P] returns the configured profile without opening a connection") {
    val dc = DatabaseConfig.forConfig[JdbcProfile]("mydb", h2Config)
    IO(assertEquals(dc.profileName, "slick.jdbc.H2Profile"))
  }

  // ---------------------------------------------------------------------------
  // Effect-aware forConfig[P, F] — the fix for issue #3
  // ---------------------------------------------------------------------------

  val loadedDb = ResourceFunFixture(
    DatabaseConfig.forConfig[JdbcProfile, IO]("mydb", h2Config)
  )

  loadedDb.test("forConfig[P, F] returns a LoadedDatabaseConfig with a live database") { dc =>
    IO(assertEquals(dc.profileName, "slick.jdbc.H2Profile"))
  }

  loadedDb.test("db from forConfig[P, F] can execute DBIO actions") { dc =>
    import dc.profile.api.*
    class Rows(tag: Tag) extends Table[Int](tag, "DBCONFIG_ROWS") {
      def v = column[Int]("V")
      def * = v
    }
    val rows = TableQuery[Rows]
    dc.db.run(
      DBIO.seq(
        rows.schema.create,
        rows += 1,
        rows += 2,
        rows.result.map(r => assertEquals(r.toSet, Set(1, 2))),
        rows.schema.drop
      )
    )
  }

  loadedDb.test("profile obtained from forConfig[P, F] matches the one from forConfig[P]") { dc =>
    val profileOnly = DatabaseConfig.forConfig[JdbcProfile]("mydb", h2Config)
    IO(assertEquals(dc.profile.getClass.getName, profileOnly.profile.getClass.getName))
  }
}
