package slick

import cats.effect.std.Dispatcher
import fs2.interop.reactivestreams.*
import org.reactivestreams.Publisher

import slick.basic.BasicBackend
import slick.dbio.{DBIOAction, Streaming}

package object reactivestreams {

  /** A Reactive Streams `Publisher` that streams the results of a Slick query.
    *
    * This is a type alias for [[org.reactivestreams.Publisher]] provided for
    * drop-in compatibility with Slick 3 code that referenced `DatabasePublisher[T]`.
    *
    * Obtain one via the `streamAsPublisher` extension method on a Slick `Database`:
    *
    * {{{
    * import slick.reactivestreams.*
    * import cats.effect.IO
    * import cats.effect.std.Dispatcher
    *
    * Dispatcher.parallel[IO].use { implicit dispatcher =>
    *   val publisher: DatabasePublisher[User] =
    *     db.streamAsPublisher(users.result)
    *   // hand publisher to a Reactive Streams consumer ...
    *   IO.unit
    * }
    * }}}
    */
  type DatabasePublisher[T] = Publisher[T]

  /** Provides the `streamAsPublisher` extension method on any Slick database.
    *
    * The conversion is done via `fs2-reactive-streams` (`fs2.interop.reactivestreams`).
    * A [[cats.effect.Dispatcher]] is required to bridge the CE3 world into the Reactive
    * Streams callback interface.  Obtain one from your `IOApp` context:
    *
    * {{{
    * Dispatcher.parallel[IO].use { implicit dispatcher => ... }
    * }}}
    */
  implicit class DatabasePublisherOps[F[_]](val db: BasicBackend#BasicDatabaseDef[F]) {
    /** Convert a streaming [[slick.dbio.DBIOAction]] to a Reactive Streams [[Publisher]].
      *
      * The returned [[Publisher]] is a *unicast* publisher: each subscriber triggers its own
      * independent database query execution.
      *
      * @param a The streaming action to execute.
      * @tparam T The element type of the stream.
      * @return A [[DatabasePublisher]] backed by the FS2 stream produced by `db.stream(a)`.
      */
    def streamAsPublisher[T](
      a: DBIOAction[?, Streaming[T], Nothing]
    )(implicit dispatcher: Dispatcher[F]): DatabasePublisher[T] = {
      implicit val asyncF: cats.effect.Async[F] = db.asyncF
      val resource = db.stream(a).toUnicastPublisher
      dispatcher.unsafeRunSync(asyncF.map(resource.allocated)(_._1))
    }
  }
}
