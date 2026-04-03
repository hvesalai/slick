package slick.basic

import java.io.Closeable

import cats.effect.{Async, Outcome, Ref, Resource}
import cats.effect.std.Semaphore
import cats.syntax.all.*
import cats.effect.syntax.all.*
import fs2.Stream

import slick.SlickException
import slick.dbio.*
import slick.util.*
import slick.compat.collection.*

import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import ClassLoaderUtil.defaultClassLoader

/** Backend for the basic database and session handling features.
  * Concrete backends like `JdbcBackend` extend this type and provide concrete
  * types for `Database`, `DatabaseFactory` and `Session`. */
trait BasicBackend { self =>
  protected lazy val actionLogger = new SlickLogger(LoggerFactory.getLogger(classOf[BasicBackend].getName+".action"))
  protected lazy val streamLogger = new SlickLogger(LoggerFactory.getLogger(classOf[BasicBackend].getName+".stream"))

  /** Non-parameterized marker trait for any database instance, regardless of effect type.
    * Use this type when you need to refer to "any database" without knowing the effect type. */
  trait AnyDatabaseDef extends Closeable {
    /** Create a new session. The session needs to be closed explicitly by calling its close() method. */
    def createSession(): BasicSessionDef
  }

  /** The type of database objects used by this backend, parameterized by effect type. */
  type Database[F[_]] >: Null <: BasicDatabaseDef[F]
  /** The type of the database factory used by this backend. */
  type DatabaseFactory >: Null
  /** The type of session objects used by this backend. */
  type Session >: Null <: BasicSessionDef
  /** The type of the context used for running SynchronousDatabaseActions */
  type Context >: Null <: BasicActionContext
  /** The type of the context used for streaming SynchronousDatabaseActions */
  type StreamingContext >: Null <: Context & BasicStreamingActionContext

  /** The database factory */
  val Database: DatabaseFactory

  /** Create a Database instance through [[https://github.com/typesafehub/config Typesafe Config]].
    * The supported config keys are backend-specific. This method is used by `DatabaseConfig`.
    *
    * Returns a `Resource[F, Database[F]]` that manages the database lifecycle.
    *
    * @param path The path in the configuration file for the database configuration, or an empty
    *             string for the top level of the `Config` object.
    * @param config The `Config` object to read from.
    * @param classLoader The ClassLoader to use for loading custom classes.
    */
  def createDatabase[F[_]: Async](config: Config, path: String, classLoader: ClassLoader = defaultClassLoader): Resource[F, Database[F]]

  // -----------------------------------------------------------------------
  // Execution state
  // -----------------------------------------------------------------------

  /** Per-`db.run` execution state carried through the interpreter via a CE3 `Ref`.
    * This replaces the old `BasicActionContext` with its `@volatile sync` workaround —
    * CE3 `Ref` provides happens-before guarantees without any explicit synchronization.
    *
    * `session` is typed as `Option[AnyRef]` rather than `Option[Session]` to avoid
    * path-dependent type issues with the abstract `Session` type member in Scala 2.13.
    * Callers cast to `Session` as needed. */
  case class ExecState(
    session:               Option[AnyRef],   // None = no JDBC connection acquired yet; cast to Session
    transactionDepth:      Int,              // nesting depth of .transactionally scopes (0 = no transaction)
    isolationLevel:        Option[Int],      // None = database default
    previousIsolationLevel: Option[Int],     // isolation level to restore after outermost transaction ends
    pinnedDepth:           Int               // nesting depth of withPinnedSession scopes (0 = not pinned)
  ) {
    def inTransaction: Boolean = transactionDepth > 0
    /** The session must be held open if there is an active transaction or pinned scope. */
    def pinned: Boolean = pinnedDepth > 0 || transactionDepth > 0
  }

  object ExecState {
    def empty: ExecState = ExecState(
      session                = None,
      transactionDepth       = 0,
      isolationLevel         = None,
      previousIsolationLevel = None,
      pinnedDepth            = 0
    )
  }

  // -----------------------------------------------------------------------
  // Database definition
  // -----------------------------------------------------------------------

  /** A database instance to which connections can be created.
    *
    * `F[_]` is the effect type (e.g. `cats.effect.IO`).
    * All `run` and `stream` calls on a given database instance use the same effect type.
    *
    * Concrete subclasses must supply:
    *   - `val asyncF: Async[F]` (the typeclass instance)
    *   - `val semaphore: Semaphore[F]` (for connection-slot back-pressure)
    */
  trait BasicDatabaseDef[F[_]] extends AnyDatabaseDef { this: Database[F] =>

    /** The `Async` instance for `F`. */
    implicit val asyncF: Async[F]

    /** CE3 Semaphore used for connection-slot back-pressure.
      * One permit = one JDBC connection. Created once at database construction time. */
    val semaphore: Semaphore[F]

    /** Create a new session. The session needs to be closed explicitly by calling its close() method. */
    def createSession(): Session

    /** Free all resources allocated by Slick for this Database. */
    override def close(): Unit

    // ------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------

    /** Run a DBIOAction and return the result in F[R]. */
    final def run[R](a: DBIOAction[R, NoStream, Nothing]): F[R] = {
      asyncF.ref(ExecState.empty).flatMap { ctx =>
        interpret[R](a, ctx)
      }
    }

    /** Stream results of a streaming DBIOAction as an FS2 Stream.
      * Back-pressure is structural — the fiber suspends when the consumer is slow. */
    final def stream[T](a: DBIOAction[?, Streaming[T], Nothing]): Stream[F, T] =
      Stream.eval(asyncF.ref(ExecState.empty)).flatMap { ctx =>
        streamInterpret[T](a, ctx)
      }

    // ------------------------------------------------------------------
    // Interpreter
    // ------------------------------------------------------------------

    /** Acquire a session lazily (only when actually needed by a SynchronousDatabaseAction),
      * run `f` with it (also providing the current ExecState snapshot), and release it unless pinned. */
    private def withSession[R](
      ctx: Ref[F, ExecState]
    )(f: (Session, ExecState) => F[R]): F[R] = {
      val F = asyncF
      ctx.get.flatMap { state =>
        state.session match {
          case Some(session) =>
            // Connection already acquired (pinned or in-transaction) — reuse it
            f(session.asInstanceOf[Session], state)

          case None =>
            // Acquire a new connection from the pool, guarded by the semaphore.
            // uncancelable ensures the acquire → open → register-finalizer sequence is
            // atomic: if the fiber is cancelled after the permit is taken but before the
            // guarantee is registered, the permit would otherwise leak permanently.
            // poll(f(...)) re-enables cancellation for the user's action so that
            // cancellation is still observed where it matters.
            F.uncancelable { poll =>
              semaphore.acquire >>
              F.blocking(createSession()).flatMap { session =>
                val setup: F[Unit] =
                  if (state.inTransaction)
                    F.blocking(setupTransaction(session, state.isolationLevel)).flatMap { prevIsolation =>
                      ctx.update(_.copy(previousIsolationLevel = prevIsolation))
                    }
                  else F.unit
                ctx.update(_.copy(session = Some(session))) >>
                setup >>
                ctx.get.flatMap { updatedState =>
                  poll(f(session, updatedState)).guarantee {
                    ctx.get.flatMap { s =>
                      if (!s.pinned)
                        F.blocking(session.close()) >>
                        ctx.update(_.copy(session = None)) >>
                        semaphore.release
                      else F.unit
                    }
                  }
                }
              }
            }
        }
      }
    }

    /** Set up a transaction on a freshly-acquired connection.
      * Returns the previous isolation level (to be restored after the transaction ends),
      * or None if the isolation level was not changed. Override in JdbcBackend. */
    protected def setupTransaction(session: Session, isolationLevel: Option[Int]): Option[Int] = None

    /** Commit the transaction on the session, restoring the given isolation level if provided.
      * Override in JdbcBackend. */
    protected def commitTransaction(session: Session, previousIsolationLevel: Option[Int]): Unit = ()

    /** Rollback the transaction on the session, restoring the given isolation level if provided.
      * Override in JdbcBackend. */
    protected def rollbackTransaction(session: Session, previousIsolationLevel: Option[Int]): Unit = ()

    /** The core recursive interpreter for `DBIOAction` values.
      *
      * - `SynchronousDatabaseAction` steps run in `F.blocking` on the CE3 blocking pool.
      * - No explicit stack-level tracking: CE3 `flatMap` is stack-safe.
      * - Execution state (session, transaction depth, pinning) is tracked in a `Ref[F, ExecState]`.
      * - Cancellation triggers rollback via `guaranteeCase`.
      */
    protected def interpret[R](
      a: DBIOAction[R, NoStream, Nothing],
      ctx: Ref[F, ExecState]
    ): F[R] = {
      val F = asyncF
      logAction(a)
      // Wrap in F.defer so that recursive calls to interpret do not consume Scala stack
      // frames. Without this, deeply nested FlatMapAction / AndThenAction structures
      // (e.g. 10,000+ levels) would cause a StackOverflowError.  CE3's defer pushes the
      // continuation onto the run-loop instead of building Scala frames.
      F.defer {
      a match {
        case SuccessAction(v) =>
          F.pure(v)

        case FailureAction(t) =>
          F.raiseError(t)

        case LiftFAction(fa) =>
          // fa is already an F[R]; type safety guaranteed by DBIO.liftF
          fa.asInstanceOf[F[R]]

        case FlatMapAction(base, f) =>
          interpret[Any](base.asInstanceOf[DBIOAction[Any, NoStream, Nothing]], ctx)
            .flatMap { v =>
              val next = f.asInstanceOf[Any => DBIOAction[R, NoStream, Nothing]](v)
              interpret[R](next, ctx)
            }

        case AndThenAction(actions) =>
          val last = actions.length - 1
          def run(pos: Int): F[Any] = {
            val fi = interpret[Any](actions(pos).asInstanceOf[DBIOAction[Any, NoStream, Nothing]], ctx)
            if (pos == last) fi
            else fi.flatMap(_ => run(pos + 1))
          }
          run(0).asInstanceOf[F[R]]

        case sa @ SequenceAction(actions) =>
          val len = actions.length
          def run(pos: Int, acc: Vector[Any]): F[Vector[Any]] = {
            if (pos == len) F.pure(acc)
            else
              interpret[Any](actions(pos).asInstanceOf[DBIOAction[Any, NoStream, Nothing]], ctx)
                .flatMap(v => run(pos + 1, acc :+ v))
          }
          run(0, Vector.empty).map { results =>
            val b = sa.cbf.asInstanceOf[Factory[Any, R]].newBuilder
            results.foreach(b += _)
            b.result()
          }

        case CleanUpAction(base, f, keepFailure) =>
          interpret[R](base.asInstanceOf[DBIOAction[R, NoStream, Nothing]], ctx).attempt.flatMap {
            case Right(v) =>
              interpret[Any](f(None).asInstanceOf[DBIOAction[Any, NoStream, Nothing]], ctx).as(v)
            case Left(err) =>
              // Run the cleanup action. Then re-raise the original error unless
              // keepFailure=false AND the cleanup action itself also failed, in which
              // case the cleanup error takes precedence over the original error.
              interpret[Any](f(Some(err)).asInstanceOf[DBIOAction[Any, NoStream, Nothing]], ctx).flatMap { _ =>
                F.raiseError[R](err)
              }.recoverWith { case cleanupErr if !keepFailure =>
                F.raiseError(cleanupErr)
              }
          }

        case FailedAction(inner) =>
          interpret[Any](inner.asInstanceOf[DBIOAction[Any, NoStream, Nothing]], ctx)
            .attempt
            .flatMap {
              case Left(t)  => F.pure(t.asInstanceOf[R])
              case Right(_) => F.raiseError(new NoSuchElementException("Action.failed did not fail"))
            }

        case AsTryAction(inner) =>
          interpret[Any](inner.asInstanceOf[DBIOAction[Any, NoStream, Nothing]], ctx)
            .attempt
            .map {
              case Right(v) => scala.util.Success(v).asInstanceOf[R]
              case Left(t)  => scala.util.Failure(t).asInstanceOf[R]
            }

        case NamedAction(inner, _) =>
          interpret[R](inner.asInstanceOf[DBIOAction[R, NoStream, Nothing]], ctx)

        case TransactionalAction(inner, isolationLevel) =>
          // uncancelable ensures the transactionDepth increment and the guaranteeCase finalizer
          // registration happen atomically. Without it a cancellation between the increment and
          // the guaranteeCase registration would leave transactionDepth in an inconsistent state.
          // The inner action runs cancelably via poll(interpret(...)).
          // Note: the finalizer (commit/rollback/session release) cannot itself be cancelled —
          // if the JDBC driver hangs in commit/rollback the fiber is stuck. This is an
          // unavoidable trade-off with synchronous JDBC drivers.
          //
          // Increment transactionDepth before running inner, so the first
          // connection acquisition calls setAutoCommit(false) and setTransactionIsolation.
          // If the connection is already open (pinned session), call setupTransaction directly.
          // Commit/rollback/release only happen when depth returns to 0 (outermost transaction).
          asyncF.uncancelable { poll =>
            ctx.get.flatMap { s =>
              val isOutermost = s.transactionDepth == 0
              ctx.update(s => s.copy(
                transactionDepth = s.transactionDepth + 1,
                isolationLevel   = if (isOutermost) isolationLevel else s.isolationLevel
              )) >>
              // If a session is already open (e.g. withPinnedSession) and this is the
              // outermost transactionally, call setupTransaction now to begin the tx
              // and set the isolation level on the already-open connection.
              (if (isOutermost) {
                s.session match {
                  case Some(sess) =>
                    asyncF.blocking(setupTransaction(sess.asInstanceOf[Session], isolationLevel)).flatMap { prevIsolation =>
                      ctx.update(_.copy(previousIsolationLevel = prevIsolation))
                    }
                  case None => asyncF.unit // no session yet; setupTransaction called in withSession
                }
              } else asyncF.unit)
            } >>
            poll(interpret[R](inner.asInstanceOf[DBIOAction[R, NoStream, Nothing]], ctx))
              .guaranteeCase { outcome =>
                // All cleanup MUST happen inside guaranteeCase so it runs even on error/cancellation.
                ctx.get.flatMap { s =>
                  val commitOrRollback: F[Unit] =
                    if (s.transactionDepth == 1) {
                      // Outermost transaction — commit or rollback
                      s.session match {
                        case Some(sess) =>
                          outcome match {
                            case Outcome.Succeeded(_) => asyncF.blocking(commitTransaction(sess.asInstanceOf[Session], s.previousIsolationLevel))
                            case _                    => asyncF.blocking(rollbackTransaction(sess.asInstanceOf[Session], s.previousIsolationLevel))
                          }
                        case None => asyncF.unit // no connection acquired; nothing to do
                      }
                    } else asyncF.unit // nested transaction — nothing to commit/rollback yet

                  val decrementDepth: F[Unit] =
                    ctx.update(s => s.copy(
                      transactionDepth       = s.transactionDepth - 1,
                      isolationLevel         = if (s.transactionDepth == 1) None else s.isolationLevel,
                      previousIsolationLevel = if (s.transactionDepth == 1) None else s.previousIsolationLevel
                    ))

                  val releaseSession: F[Unit] =
                    ctx.get.flatMap { s2 =>
                      // Release session only when outermost transaction ends (depth now 0)
                      if (!s2.pinned) {
                        s2.session match {
                          case Some(sess) =>
                            asyncF.blocking(sess.asInstanceOf[Session].close()) >>
                            ctx.update(_.copy(session = None)) >>
                            semaphore.release
                          case None => asyncF.unit
                        }
                      } else asyncF.unit
                    }

                  commitOrRollback >> decrementDepth >> releaseSession
                }
              }
          }

        case PinnedSessionAction(inner) =>
          ctx.update(s => s.copy(pinnedDepth = s.pinnedDepth + 1)) >>
          interpret[R](inner.asInstanceOf[DBIOAction[R, NoStream, Nothing]], ctx)
            .guarantee {
              ctx.update(s => s.copy(pinnedDepth = s.pinnedDepth - 1)) >>
              ctx.get.flatMap { s =>
                // Only release the connection if there are no more pins or transactions
                if (!s.pinned) {
                  s.session match {
                    case Some(sess) =>
                      asyncF.blocking(sess.asInstanceOf[Session].close()) >>
                      ctx.update(_.copy(session = None)) >>
                      semaphore.release
                    case None =>
                      asyncF.unit
                  }
                } else asyncF.unit
              }
            }

        case a: SynchronousDatabaseAction[?, ?, ?, ?, ?] =>
          withSession[R](ctx) { (session, state) =>
            F.blocking {
              a.asInstanceOf[SynchronousDatabaseAction[R, NoStream, Context, StreamingContext, Nothing]]
               .run(sessionAsContext(session, state))
            }
          }

        case a: DBIOAction[?, ?, ?] =>
          F.raiseError(new SlickException(s"Unsupported database action $a for $this"))
      }
      } // end F.defer
    }

    /** Stream the results of a streaming DBIOAction using FS2's pull model.
      * Each row fetch is wrapped in F.blocking, so no OS thread is held between rows. */
    /** Like `withSession` but for streaming: acquires/reuses a session and passes it to `f`
      * which returns a `Stream[F, T]`. The session is released (unless pinned) when the
      * stream finishes or is cancelled. */
    private def withSessionStream[T](
      ctx: Ref[F, ExecState]
    )(f: (Session, ExecState) => Stream[F, T]): Stream[F, T] = {
      val F = asyncF
      Stream.eval(ctx.get).flatMap { state =>
        state.session match {
          case Some(session) =>
            // Reuse existing pinned/transactional session
            f(session.asInstanceOf[Session], state)

          case None =>
            // Acquire a new connection from the pool, guarded by the semaphore.
            // The acquire+open is wrapped in uncancelable so that bracketCase's
            // finalizer is always registered before any cancellation can occur
            // after the semaphore permit is taken.
            Stream.bracketCase(
              asyncF.uncancelable(_ =>
                semaphore.acquire >> F.blocking(createSession())
              )
            ) { (session, _) =>
              ctx.get.flatMap { s =>
                if (!s.pinned)
                  F.blocking(session.close()) >>
                  ctx.update(_.copy(session = None)) >>
                  semaphore.release
                else F.unit
              }
            }.flatMap { session =>
              val setup: F[Unit] =
                if (state.inTransaction)
                  F.blocking(setupTransaction(session, state.isolationLevel))
                else F.unit
              Stream.eval(ctx.update(_.copy(session = Some(session))) >> setup >> ctx.get)
                .flatMap { updatedState => f(session, updatedState) }
            }
        }
      }
    }

    protected def streamInterpret[T](
      a: DBIOAction[?, Streaming[T], Nothing],
      ctx: Ref[F, ExecState]
    ): Stream[F, T] = {
      val F = asyncF
      a match {
        case sa: SynchronousDatabaseAction[?, ?, ?, ?, ?]
            if sa.supportsStreaming =>
          withSessionStream[T](ctx) { (session, state) =>
            val sda = sa.asInstanceOf[SynchronousDatabaseAction[?, Streaming[T], Context, StreamingContext, Nothing]]
            streamFromSDA[T](sda, session, state)
          }

        case sa: SynchronousDatabaseAction[?, ?, ?, ?, ?] =>
          // FusedAndThenAction (supportsStreaming = false): unfuse and recurse so that prefix
          // actions run through the interpreter and the final streaming action is streamed.
          streamInterpret[T](
            sa.nonFusedEquivalentAction.asInstanceOf[DBIOAction[?, Streaming[T], Nothing]], ctx)

        case AndThenAction(actions) =>
          // Run all prefix actions through the interpreter, then stream the last one.
          val prefix = actions.init.asInstanceOf[IndexedSeq[DBIOAction[Any, NoStream, Nothing]]]
          val last   = actions.last.asInstanceOf[DBIOAction[?, Streaming[T], Nothing]]
          if (prefix.isEmpty) streamInterpret[T](last, ctx)
          else
            Stream.eval(prefix.foldLeft(F.unit)((acc, act) =>
              acc >> interpret[Any](act, ctx).void
            )) >> streamInterpret[T](last, ctx)

        case PinnedSessionAction(inner) =>
          Stream.eval(ctx.update(s => s.copy(pinnedDepth = s.pinnedDepth + 1))) >>
          (streamInterpret[T](inner.asInstanceOf[DBIOAction[?, Streaming[T], Nothing]], ctx)
            .onFinalize {
              ctx.update(s => s.copy(pinnedDepth = s.pinnedDepth - 1)) >>
              ctx.get.flatMap { s =>
                if (!s.pinned) {
                  s.session match {
                    case Some(sess) =>
                      F.blocking(sess.asInstanceOf[Session].close()) >>
                      ctx.update(_.copy(session = None)) >>
                      semaphore.release
                    case None =>
                      F.unit
                  }
                } else F.unit
              }
            })

        case TransactionalAction(inner, isolationLevel) =>
          // Same as the interpret-side TransactionalAction, but for streaming:
          // set up the transaction, stream inner, commit/rollback on finalization.
          Stream.eval(ctx.get.flatMap { s =>
            val isOutermost = s.transactionDepth == 0
            ctx.update(s => s.copy(
              transactionDepth = s.transactionDepth + 1,
              isolationLevel   = if (isOutermost) isolationLevel else s.isolationLevel
            )) >>
            (if (isOutermost) {
              s.session match {
                case Some(sess) =>
                  F.blocking(setupTransaction(sess.asInstanceOf[Session], isolationLevel)).flatMap { prevIsolation =>
                    ctx.update(_.copy(previousIsolationLevel = prevIsolation))
                  }
                case None => F.unit
              }
            } else F.unit)
          }) >>
          streamInterpret[T](inner.asInstanceOf[DBIOAction[?, Streaming[T], Nothing]], ctx)
            .onFinalizeCase { exitCase =>
              ctx.get.flatMap { s =>
                val commitOrRollback: F[Unit] =
                  if (s.transactionDepth == 1) {
                    s.session match {
                      case Some(sess) =>
                        exitCase match {
                          case Resource.ExitCase.Succeeded  => F.blocking(commitTransaction(sess.asInstanceOf[Session], s.previousIsolationLevel))
                          case _                            => F.blocking(rollbackTransaction(sess.asInstanceOf[Session], s.previousIsolationLevel))
                        }
                      case None => F.unit
                    }
                  } else F.unit

                val decrementDepth: F[Unit] =
                  ctx.update(s => s.copy(
                    transactionDepth       = s.transactionDepth - 1,
                    isolationLevel         = if (s.transactionDepth == 1) None else s.isolationLevel,
                    previousIsolationLevel = if (s.transactionDepth == 1) None else s.previousIsolationLevel
                  ))

                val releaseSession: F[Unit] =
                  ctx.get.flatMap { s2 =>
                    if (!s2.pinned) {
                      s2.session match {
                        case Some(sess) =>
                          F.blocking(sess.asInstanceOf[Session].close()) >>
                          ctx.update(_.copy(session = None)) >>
                          semaphore.release
                        case None => F.unit
                      }
                    } else F.unit
                  }

                commitOrRollback >> decrementDepth >> releaseSession
              }
            }

        case FlatMapAction(base, f) =>
          // Run the (non-streaming) base through the interpreter, then recurse into
          // streamInterpret for the produced streaming action.
          Stream.eval(
            interpret[Any](base.asInstanceOf[DBIOAction[Any, NoStream, Nothing]], ctx)
          ).flatMap { v =>
            val next = f.asInstanceOf[Any => DBIOAction[?, Streaming[T], Nothing]](v)
            streamInterpret[T](next, ctx)
          }

        case NamedAction(inner, _) =>
          streamInterpret[T](inner.asInstanceOf[DBIOAction[?, Streaming[T], Nothing]], ctx)

        case _ =>
          // Non-SDA streaming action — fall back to running through interpreter and collecting.
          // This path should not be reached for any first-class action type; it exists as a
          // safety net for third-party DBIOAction subclasses that are not natively streamable.
          Stream.eval(interpret[Seq[T]](
            a.asInstanceOf[DBIOAction[Seq[T], NoStream, Nothing]], ctx
          )).flatMap(seq => Stream.emits(seq))
      }
    }

    /** Build an FS2 Stream from a SynchronousDatabaseAction.
      * Must be implemented by each backend that supports streaming. */
    protected def streamFromSDA[T](
      a: SynchronousDatabaseAction[?, Streaming[T], Context, StreamingContext, Nothing],
      session: Session,
      state: ExecState
    ): Stream[F, T]

    /** Wrap a Session and ExecState as a Context for passing to SynchronousDatabaseAction.run. */
    protected def sessionAsContext(session: Session, state: ExecState): Context

    // ------------------------------------------------------------------
    // Logging
    // ------------------------------------------------------------------

    protected[this] def logAction(a: DBIOAction[?, NoStream, Nothing]): Unit = {
      if (actionLogger.isDebugEnabled && a.isLogged) {
        val logA = a.nonFusedEquivalentAction
        val aPrefix = if (a eq logA) "" else "[fused] "
        val dump = new TreePrinter(prefix = "    ", firstPrefix = aPrefix, narrow = {
          case a: DBIOAction[?, ?, ?] => a.nonFusedEquivalentAction
          case o                      => o
        }).get(logA)
        val msg = DumpInfo.highlight(dump.substring(0, dump.length - 1))
        actionLogger.debug(msg)
      }
    }
  }

  // -----------------------------------------------------------------------
  // Session definition
  // -----------------------------------------------------------------------

  /** A logical session of a `Database`. The underlying database connection is created lazily on demand. */
  trait BasicSessionDef extends Closeable {
    /** Close this Session. */
    def close(): Unit

    /** Force an actual database session to be opened. Slick sessions are lazy, so you do not
      * get a real database connection until you need it or you call force() on the session. */
    def force(): Unit
  }

  // -----------------------------------------------------------------------
  // Action context (used by SynchronousDatabaseAction.run)
  // -----------------------------------------------------------------------

  /** The context object passed to `SynchronousDatabaseAction` instances by the execution engine.
    * The heavy concurrency state lives in [[ExecState]] / `Ref`; this is a thin wrapper that
    * gives SDAs access to the session and statement parameters.
    * Pin/unpin are preserved because SynchronousDatabaseAction fused forms use them directly. */
  trait BasicActionContext extends ActionContext {
    def session: Session
    /** Current transaction nesting depth (0 = no transaction, 1 = one level, etc.) */
    def transactionDepth: Int
    /** Whether the session is currently pinned (fromExecState.pinned). */
    def statePinned: Boolean
    /** isPinned reflects both ExecState.pinned and the ActionContext stickiness counter. */
    override def isPinned: Boolean = statePinned || super.isPinned
  }

  /** A special BasicActionContext for streaming SynchronousDatabaseActions. */
  trait BasicStreamingActionContext extends BasicActionContext with StreamingActionContext
}
