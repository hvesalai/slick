package slick.basic

import java.net.URI

import scala.reflect.ClassTag
import scala.util.control.NonFatal

import cats.effect.{Async, Resource}

import slick.util.{ClassLoaderUtil, SlickLogger}
import slick.util.ConfigExtensionMethods.configExtensionMethods
import slick.SlickException

import com.typesafe.config.{Config, ConfigFactory}

/** A configuration for a Profile (without an open database connection).
  * Obtain via `DatabaseConfig.forConfig`. */
trait DatabaseConfig[P <: BasicProfile] {
  /** The configured Profile. */
  val profile: P

  /** The raw configuration. */
  def config: Config

  /** The name of the Profile class or object (without a trailing "$"). */
  def profileName: String

  /** Whether the `profileName` represents an object instead of a class. */
  def profileIsObject: Boolean
}

/** A [[DatabaseConfig]] bundled with an open [[BasicBackend#BasicDatabaseDef]] for effect type `F`.
  *
  * Obtain via `DatabaseConfig.forConfig` with an explicit `F` type parameter. */
trait LoadedDatabaseConfig[P <: BasicProfile, F[_]] extends DatabaseConfig[P] {
  /** The open database for this configuration. Managed by the enclosing `Resource`. */
  def db: profile.backend.Database[F]
}

object DatabaseConfig {
  private[this] lazy val logger = SlickLogger[DatabaseConfig[?]]

  /** Resolve the profile name from config, throwing if missing. */
  private def resolveProfileName(basePath: String, config: Config): String =
    config.getStringOpt(basePath + "profile").getOrElse {
      val nOld = config.getStringOpt(basePath + "driver").map {
        case "slick.driver.DerbyDriver$"   => "slick.jdbc.DerbyProfile$"
        case "slick.driver.H2Driver$"      => "slick.jdbc.H2Profile$"
        case "slick.driver.HsqldbDriver$"  => "slick.jdbc.HsqldbProfile$"
        case "slick.driver.MySQLDriver$"   => "slick.jdbc.MySQLProfile$"
        case "slick.driver.PostgresDriver$" => "slick.jdbc.PostgresProfile$"
        case "slick.driver.SQLiteDriver$"  => "slick.jdbc.SQLiteProfile$"
        case "slick.memory.MemoryDriver$"  => "slick.memory.MemoryProfile$"
        case n => n
      }
      if (nOld.isDefined)
        logger.warn(s"Use `${basePath}profile` instead of `${basePath}driver`. The latter is deprecated since Slick 3.2 and will be removed.")
      nOld.getOrElse(config.getString(basePath + "profile")) // trigger the correct error
    }

  /** Instantiate the profile object/class identified by `n`. */
  private def instantiateProfile[P <: BasicProfile : ClassTag](n: String, classLoader: ClassLoader): P = {
    val untypedP = try {
      if (n.endsWith("$")) classLoader.loadClass(n).getField("MODULE$").get(null)
      else classLoader.loadClass(n).getConstructor().newInstance()
    } catch { case NonFatal(ex) =>
      throw new SlickException(s"""Error getting instance of profile "$n"""", ex)
    }
    val pClass = implicitly[ClassTag[P]].runtimeClass
    if (!pClass.isInstance(untypedP))
      throw new SlickException(s"Configured profile $n does not conform to requested profile ${pClass.getName}")
    untypedP.asInstanceOf[P]
  }

  /** Load a Profile configuration (profile only, no open database) through
    * [[https://github.com/typesafehub/config Typesafe Config]].
    *
    * To also open a database connection, use the `forConfig` overload that takes an explicit
    * effect type `F` (returning `Resource[F, LoadedDatabaseConfig[P, F]]`).
    *
    * The following config parameters are available:
    * <ul>
    *   <li>`profile` (String, required): The fully qualified name of a class or object which
    *   implements the specified profile. If the name ends with `$` it is assumed to be an object
    *   name, otherwise a class name.</li>
    *   <li>`db` (Config, optional): The configuration of a database for the profile's backend.
    *   For profiles extending `JdbcProfile` (which always use `JdbcBackend`), see
    *   `JdbcBackend.DatabaseFactory.forConfig` for parameters that should be defined inside of
    *   `db`.</li>
    * </ul>
    *
    * Note: Slick 3.2 also supports the old `driver` parameter as an alternative to `profile`.
    * Old profile names (e.g. ``slick.driver.DerbyDriver$`` for ``slick.jdbc.DerbyProfile$``) are
    * recognized and translated to the new names. This feature is deprecated and will be removed
    * in a future release.
    *
    * @param path The path in the configuration file for the database configuration (e.g. `foo.bar`
    *             would find a profile name at config key `foo.bar.profile`) or an empty string
    *             for the top level of the `Config` object.
    * @param config The `Config` object to read from. This defaults to the global app config
    *               (e.g. in `application.conf` at the root of the class path) if not specified.
    * @param classLoader The ClassLoader to use to load any custom classes from. The default is to
    *                    try the context ClassLoader first and fall back to Slick's ClassLoader.
    */
  def forConfig[P <: BasicProfile : ClassTag](
    path: String,
    config: Config = ConfigFactory.load(),
    classLoader: ClassLoader = ClassLoaderUtil.defaultClassLoader
  ): DatabaseConfig[P] = {
    val basePath = if (path.isEmpty) "" else path + "."
    val n = resolveProfileName(basePath, config)
    val p = instantiateProfile[P](n, classLoader)
    val root = config
    new DatabaseConfig[P] {
      val profile: P = p
      lazy val config: Config = if (path.isEmpty) root else root.getConfig(path)
      def profileName = if (profileIsObject) n.substring(0, n.length - 1) else n
      def profileIsObject = n.endsWith("$")
    }
  }

  /** Load a Profile configuration and open a database connection, returning a
    * `Resource[F, LoadedDatabaseConfig[P, F]]` that manages the connection pool lifecycle.
    *
    * The `db` field on the returned [[LoadedDatabaseConfig]] is the open database instance for
    * the configured profile, ready to use with `db.run(...)` and `db.stream(...)`.
    *
    * @param path The path in the configuration file for the database configuration (e.g. `"mydb"`),
    *             or an empty string for the top level of the `Config` object.
    * @param config The `Config` object to read from. Defaults to `ConfigFactory.load()`.
    * @param classLoader ClassLoader for loading the profile class. Defaults to the context
    *                    classloader with a fallback to Slick's own classloader.
    */
  def forConfig[P <: BasicProfile : ClassTag, F[_] : Async](
    path: String,
    config: Config,
    classLoader: ClassLoader
  ): Resource[F, LoadedDatabaseConfig[P, F]] = {
    val basePath = if (path.isEmpty) "" else path + "."
    val root = config
    val n = resolveProfileName(basePath, root)
    val p = instantiateProfile[P](n, classLoader)
    // The db sub-path is `<path>.db` (or just `db` at the top level).
    val dbPath = (if (path.isEmpty) "" else path + ".") + "db"
    p.backend.createDatabase[F](root, dbPath, classLoader).map { database =>
      new LoadedDatabaseConfig[P, F] {
        val profile: P = p
        // The db field is typed as profile.backend.Database[F].
        // We know `database` was produced by `p.backend.createDatabase[F]`, so this cast is safe.
        val db: profile.backend.Database[F] = database.asInstanceOf[profile.backend.Database[F]]
        lazy val config: Config = if (path.isEmpty) root else root.getConfig(path)
        def profileName = if (profileIsObject) n.substring(0, n.length - 1) else n
        def profileIsObject = n.endsWith("$")
      }
    }
  }

  /** Overload of `forConfig[P, F](path, config, classLoader)` (the effect-type variant)
    * using the default class loader and `ConfigFactory.load()` as the config source. */
  def forConfig[P <: BasicProfile : ClassTag, F[_] : Async](
    path: String
  ): Resource[F, LoadedDatabaseConfig[P, F]] =
    forConfig[P, F](path, ConfigFactory.load(), ClassLoaderUtil.defaultClassLoader)

  /** Overload of `forConfig[P, F](path, config, classLoader)` (the effect-type variant)
    * using the default class loader. */
  def forConfig[P <: BasicProfile : ClassTag, F[_] : Async](
    path: String,
    config: Config
  ): Resource[F, LoadedDatabaseConfig[P, F]] =
    forConfig[P, F](path, config, ClassLoaderUtil.defaultClassLoader)

  /** Load a profile and database configuration from the specified URI. If only a fragment name
    * is given, it is resolved as a path in the global app config (e.g. in `application.conf` at
    * the root of the class path), otherwise as a path in the configuration located at the URI
    * without the fragment, which must be a valid URL. Without a fragment, the whole config object
    * is used. */
  def forURI[P <: BasicProfile : ClassTag](uri: URI, classLoader: ClassLoader = ClassLoaderUtil.defaultClassLoader): DatabaseConfig[P] = {
    val (base, path) = {
      val f = uri.getRawFragment
      val s = uri.toString
      if (s.isEmpty) (null, "")
      else if (f eq null) (s, "")
      else if (s.startsWith("#")) (null, uri.getFragment)
      else (s.substring(0, s.length - f.length - 1), uri.getFragment)
    }
    val root =
      if (base eq null) ConfigFactory.load(classLoader)
      else ConfigFactory.parseURL(new URI(base).toURL).resolve()
    forConfig[P](path, root, classLoader)
  }
}
