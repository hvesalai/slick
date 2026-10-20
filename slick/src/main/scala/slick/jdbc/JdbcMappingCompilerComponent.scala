package slick.jdbc

import java.sql.{PreparedStatement, ResultSet}

import slick.SlickException
import slick.ast.*
import slick.ast.Util.*
import slick.compiler.{CodeGen, CompilerState, InsertCompiler}
import slick.relational.*
import slick.util.{ConstArray, SQLBuilder}

/** JDBC profile component which contains the mapping compiler and insert compiler */
trait JdbcMappingCompilerComponent { self: JdbcProfile =>

  /** The `MappingCompiler` for this profile. */
  val mappingCompiler: MappingCompiler = new MappingCompiler

  /** Create a (possibly specialized) `ResultConverter` for the given `JdbcType`. */
  def createBaseResultConverter[T](ti: JdbcType[T],
                                   name: String,
                                   idx: Int): ResultConverter[ResultSet, PreparedStatement, ResultSet, T] =
    SpecializedJdbcResultConverter.base(ti, name, idx)

  /** Create a (possibly specialized) `ResultConverter` for `Option` values of the given `JdbcType`. */
  def createOptionResultConverter[T](ti: JdbcType[T], idx: Int): ResultConverter[ResultSet, PreparedStatement, ResultSet, Option[T]] =
    new OptionResultConverter(ti, idx)

  /** A ResultConverterCompiler that builds JDBC-based converters. Instances of
    * this class use mutable state internally. They are meant to be used for a
    * single conversion only and must not be shared or reused. */
  class MappingCompiler extends ResultConverterCompiler[ResultSet, PreparedStatement, ResultSet] {
    def createColumnConverter(n: Node,
                              idx: Int,
                              column: Option[FieldSymbol]): ResultConverter[ResultSet, PreparedStatement, ResultSet, ?] = {
      val JdbcType(ti, option) = n.nodeType.structural
      if(option) createOptionResultConverter(ti, idx)
      else createBaseResultConverter(ti, column.fold("<computed>")(_.name), idx)
    }

    override def createGetOrElseResultConverter[T](rc: ResultConverter[ResultSet, PreparedStatement, ResultSet, Option[T]],
                                                   default: () => T) = rc match {
      case rc: OptionResultConverter[T] => rc.getOrElse(default)
      case _                            => super.createGetOrElseResultConverter[T](rc, default)
    }

    override def createIsDefinedResultConverter[T](rc: ResultConverter[ResultSet, PreparedStatement, ResultSet, Option[T]]) =
      rc match {
        case rc: OptionResultConverter[?] => rc.isDefined
        case _                            => super.createIsDefinedResultConverter(rc)
      }

    override def createTypeMappingResultConverter(rc: ResultConverter[ResultSet, PreparedStatement, ResultSet, Any],
                                                  mapper: MappedScalaType.Mapper) = {
      val tm = TypeMappingResultConverter(rc, mapper.toBase, mapper.toMapped)
      mapper.fastPath match {
        case Some(f) => f(tm).asInstanceOf[ResultConverter[ResultSet, PreparedStatement, ResultSet, Any]]
        case None => tm
      }
    }
  }

  /** Code generator phase for queries on JdbcProfile. */
  class JdbcCodeGen(f: QueryBuilder => SQLBuilder.Result) extends CodeGen {
    override def compileServerSideAndMapping(serverSide: Node,
                                             mapping: Option[Node],
                                             state: CompilerState): (CompiledStatement, Option[CompiledMapping]) = {
      val (tree, tpe) = treeAndType(serverSide)
      val sbr = f(self.createQueryBuilder(tree, state))
      (CompiledStatement(sbr.sql, sbr, tpe).infer(), mapping.map(mappingCompiler.compileMapping))
    }
  }

  /** Code generator phase for updates on JdbcProfile that only set the selected columns which the `mode` includes,
    * e.g. `InsertCompiler.NonAutoInc` for `softUpdate`. The other columns are left out of the `SET` clause,
    * and the mapping writes no parameters for them, like the `InsertColumn`s of a standard insert. */
  class JdbcUpdateCodeGen(mode: InsertCompiler.Mode) extends JdbcCodeGen(_.buildUpdate()) {
    override def compileServerSideAndMapping(serverSide: Node,
                                             mapping: Option[Node],
                                             state: CompilerState): (CompiledStatement, Option[CompiledMapping]) = {
      val (tree, tpe) = treeAndType(serverSide)

      tree match {
        case c @ Comprehension(sym, _: TableNode, Pure(ProductNode(columns), ts), _, _, _, _, _, _, _, _) =>
          // The 1-based positions of the selected columns that are not set; `buildUpdate` rejects other selections
          val excluded = columns.iterator.zipWithIndex.collect {
            case (Select(Ref(s), fs: FieldSymbol), idx) if s == sym && !mode(fs) => (idx + 1, fs)
          }.toMap
          val nothingToSet = excluded.size == columns.length

          if (excluded.isEmpty) super.compileServerSideAndMapping(serverSide, mapping, state)
          else if (nothingToSet) throw noColumnsToSet
          else {
            val kept =
              ProductNode(ConstArray.from(columns.toSeq.zipWithIndex.collect {
                case (n, idx) if !excluded.contains(idx + 1) => n
              })).infer()
            val tree2 = c.copy(select = Pure(kept, ts).infer()).infer()
            val sbr = self.createQueryBuilder(tree2, state).buildUpdate()

            (CompiledStatement(sbr.sql, sbr, tpe).infer(),
              mapping.map(m => mappingCompiler.compileMapping(skipColumns(m, columns.length, excluded))))
          }
        case Comprehension(sym, _: TableNode, Pure(Select(Ref(s), fs: FieldSymbol), _), _, _, _, _, _, _, _, _)
          if s == sym && !mode(fs) =>
          throw noColumnsToSet
        case _ =>
          super.compileServerSideAndMapping(serverSide, mapping, state)
      }
    }

    private def noColumnsToSet =
      new SlickException("All columns selected for this update are AutoInc columns, which are not set, so there are " +
        "no columns to update -- use update to set them")

    /** Replace the excluded columns in a mapping by `InsertColumn`s without paths, which write nothing, and renumber
      * the remaining columns to match the shortened `SET` clause. */
    private def skipColumns(mapping: Node, width: Int, excluded: Map[Int, FieldSymbol]): Node = {
      val newIndex = (1 to width).filterNot(excluded.contains).zipWithIndex.map { case (idx, i) => (idx, i + 1) }.toMap

      mapping.replace({
        case cast @ Library.SilentCast(Select(_, ElementSymbol(idx))) if excluded.contains(idx) =>
          InsertColumn(ConstArray.empty, excluded(idx), cast.nodeType)
        case sel @ Select(ref, ElementSymbol(idx)) =>
          excluded.get(idx) match {
            case Some(fs) => InsertColumn(ConstArray.empty, fs, sel.nodeType)
            case None     => Select(ref, ElementSymbol(newIndex(idx))) :@ sel.nodeType
          }
      }, keepType = true)
    }
  }

  /** Code generator phase for inserts on JdbcProfile. */
  class JdbcInsertCodeGen(f: Insert => InsertBuilder) extends CodeGen {
    override def compileServerSideAndMapping(serverSide: Node,
                                             mapping: Option[Node],
                                             state: CompilerState): (CompiledStatement, Option[CompiledMapping]) = {
      val ib = f(serverSide.asInstanceOf[Insert])
      val ibr = ib.buildInsert
      (CompiledStatement(ibr.sql, ibr, serverSide.nodeType).infer(),
        mapping.map(n => mappingCompiler.compileMapping(ib.transformMapping(n))))
    }
  }
}
