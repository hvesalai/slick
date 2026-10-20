package com.typesafe.slick.testkit.tests

import scala.util.Try

import com.typesafe.slick.testkit.util.{AsyncTest, JdbcTestDB}

import slick.SlickException

class UpdateTest extends AsyncTest[JdbcTestDB] {
  import tdb.profile.api.*

  case class Item(id: Int, name: String, qty: Int)

  class Items(tname: String)(tag: Tag) extends Table[Item](tag, tname) {
    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
    def name = column[String]("name")
    def qty = column[Int]("qty")
    def * = (id, name, qty).mapTo[Item]
  }

  /** The column list of the `SET` clause of an UPDATE statement, in lower case */
  private def setClause(sql: String): String = {
    val lower = sql.toLowerCase
    val start = lower.indexOf(" set ") + 5
    val end = lower.indexOf(" where ", start)

    if (end < 0) lower.substring(start) else lower.substring(start, end)
  }

  private def placeholders(sql: String) = sql.count(_ == '?')

  def testSoftUpdate = {
    val items = TableQuery(new Items("soft_update")(_))
    val byId = Compiled((id: Rep[Int]) => items.filter(_.id === id))
    val rows = Seq(Item(0, "a", 1), Item(0, "b", 2), Item(0, "c", 3), Item(0, "d", 4), Item(0, "e", 5))

    seq(
      items.schema.create,
      items ++= rows,
      // A whole mapped row: the AutoInc id stays, the other columns are updated
      items.filter(_.id === 1.bind).softUpdate(Item(101, "A", 10)).map(_ shouldBe 1),
      // An explicit projection that lists the AutoInc column
      items.filter(_.id === 2.bind).map(t => (t.id, t.name, t.qty)).softUpdate((102, "B", 20)).map(_ shouldBe 1),
      // An Option-lifted AutoInc column
      items.filter(_.id === 3.bind).map(t => (t.name, t.id.?)).softUpdate(("C", Some(103))).map(_ shouldBe 1),
      // A compiled query with a parameter after the SET values
      byId(4).softUpdate(Item(104, "D", 40)).map(_ shouldBe 1),
      // A projection without the AutoInc column is updated as with `update`
      items.filter(_.id === 5.bind).map(_.name).softUpdate("E").map(_ shouldBe 1),
      items.sortBy(_.id).result.map(_ shouldBe Seq(
        Item(1, "A", 10), Item(2, "B", 20), Item(3, "C", 3), Item(4, "D", 40), Item(5, "E", 5)))
    )
  }

  def testSoftUpdateOnlyAutoIncColumns = {
    val items = TableQuery(new Items("soft_update_only_autoinc")(_))
    val ids = items.filter(_.name === "a".bind).map(_.id)
    // The failure names the problem and is raised when the statement is built, before anything runs
    def failsToBuild(f: => Any) =
      Try(f).failed.get should { case e: SlickException => e.getMessage.contains("AutoInc"); case _ => false }

    failsToBuild(ids.softUpdateStatement)
    failsToBuild(ids.softUpdate(1))
    failsToBuild(items.map(t => (t.id, t.id.?)).softUpdate((1, Some(1))))
    failsToBuild(Compiled(ids).softUpdate(1))

    // `update` still sets the AutoInc column
    setClause(ids.updateStatement) should (_.contains("id"))
    DBIO.successful(())
  }

  def testSoftUpdateStatements = {
    val items = TableQuery(new Items("soft_update_statements")(_))
    val q = items.filter(_.id === 1.bind)

    // `update` sets every selected column, `softUpdate` skips the AutoInc one
    setClause(q.updateStatement) should (_.contains("id"))
    setClause(q.softUpdateStatement) should (s => !s.contains("id") && s.contains("name") && s.contains("qty"))
    placeholders(q.updateStatement) shouldBe 4
    placeholders(q.softUpdateStatement) shouldBe 3
    q.softUpdate(Item(1, "a", 1)).statements.toList shouldBe List(q.softUpdateStatement)
    q.update(Item(1, "a", 1)).statements.toList shouldBe List(q.updateStatement)

    val explicit = q.map(t => (t.qty, t.id))
    setClause(explicit.softUpdateStatement) should (s => !s.contains("id") && s.contains("qty"))
    Compiled(q).softUpdateStatement shouldBe q.softUpdateStatement

    // Without an AutoInc column both statements are the same
    val names = q.map(t => (t.name, t.qty))
    names.softUpdateStatement shouldBe names.updateStatement
    DBIO.successful(())
  }
}
