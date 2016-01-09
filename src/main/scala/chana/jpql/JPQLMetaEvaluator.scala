package chana.jpql

import chana.avro.Projection
import chana.jpql.nodes._
import chana.schema.SchemaBoard
import org.apache.avro.Schema
import scala.collection.immutable

object JPQLMetaEvaluator {
  final case class QualedAttribute(qual: String, paths: List[String])
}
final class JPQLMetaEvaluator(jpqlKey: String, schemaBoard: SchemaBoard) extends JPQLEvaluator {
  import JPQLMetaEvaluator.QualedAttribute

  def id = throw new UnsupportedOperationException("Do not call id method in " + this.getClass.getSimpleName)
  protected var asToEntity = Map[String, String]()
  protected var asToJoin = Map[String, List[String]]()
  override protected def addAsToEntity(as: String, entity: String) = asToEntity += (as -> entity)
  override protected def addAsToJoin(as: String, joinPath: List[String]) = asToJoin += (as -> joinPath)

  override protected def forceGather = true

  private var entityAliasToProjectionNode = Map[String, Projection.Node]()

  /**
   * Entrance
   */
  def collectMeta(stmt: Statement, record: Any): JPQLMeta = {
    stmt match {
      case stmt @ SelectStatement(select, from, where, groupby, having, orderby) =>
        val entity = fromClause(from, record) // collect asToEntity and asToJoin

        entityAliasToProjectionNode = asToEntity.foldLeft(Map[String, Projection.Node]()) {
          case (acc, (as, entity)) =>
            schemaBoard.schemaOf(entity) match {
              case Some(schema) => acc + (as -> Projection.FieldNode(entity, None, schema))
              case None         => acc
            }
        }

        selectClause(select, record, toGather = true)

        // skip wherecluse which is not necessary to be considered.

        // visit groupby, having and orderby to collect necessary dataset
        groupby foreach { x => groupbyClause(x, record) }
        having foreach { x => havingClause(x, record) }
        orderby foreach { x => orderbyClause(x, record) }

        val projectionSchemas = entityAliasToProjectionNode map {
          case (as, projectionNode) => Projection.visitProjectionNode(jpqlKey, projectionNode, null).endRecord
        }

        val ids = stmt.collectSpecifiedIds.collect {
          case (as, id) if asToEntity.get(as) == Some(entity) => id
        }

        JPQLSelect(stmt, entity, asToEntity, asToJoin, ids, projectionSchemas.toList)

      case stmt @ UpdateStatement(update, set, where) =>
        // visit updateClause is enough for meta
        val entity = updateClause(update, record)
        val ids = stmt.collectSpecifiedIds.collect {
          case (as, id) if asToEntity.get(as) == Some(entity) => id
        }

        JPQLUpdate(stmt, entity, asToEntity, asToJoin, ids)

      case stmt @ DeleteStatement(delete, attributes, where) =>
        // visit deleteClause is enough for meta
        val entity = deleteClause(delete, record)
        val ids = stmt.collectSpecifiedIds.collect {
          case (as, id) if asToEntity.get(as) == Some(entity) => id
        }

        JPQLDelete(stmt, entity, asToEntity, asToJoin, ids)

      case stmt @ InsertStatement(insert, attributes, values, where) =>
        // visit insertClause is enough for meta
        val entity = insertClause(insert, record)
        val ids = stmt.collectSpecifiedIds.collect {
          case (as, id) if asToEntity.get(as) == Some(entity) => id
        }

        JPQLInsert(stmt, entity, asToEntity, asToJoin, ids)
    }
  }

  def normalizeAttribute(attr: QualedAttribute): QualedAttribute = {
    asToJoin.get(attr.qual) match {
      case Some(paths) => QualedAttribute(paths.head, paths.tail ::: attr.paths)
      case None =>
        // check if qual is an alias, if true, turn it to normalized one to be collected
        asToItem.get(attr.qual) match {
          case Some(QualedAttribute(qual0, paths0)) => QualedAttribute(qual0, paths0 ::: attr.paths)
          case Some(x)                              => attr // TODO Something wrong?
          case None                                 => attr
        }
    }
  }

  /**
   * TODO collect aliased value
   */
  private def collectLeastProjectionNodes(_attr: QualedAttribute) {
    if (enterGather) {
      val attr = normalizeAttribute(_attr)

      // Do not collect top level id, which is specified outside
      if (attr.paths.headOption != JPQLEvaluator.SOME_ID) {
        entityAliasToProjectionNode.get(attr.qual) match {
          case Some(projectionNode) =>
            var currSchema = projectionNode.schema
            var currNode: Projection.Node = projectionNode

            var paths = attr.paths

            while (paths.nonEmpty) {
              val path = paths.head
              paths = paths.tail

              // check if currNode has been targeted
              if (!currNode.isClosed) {
                val pathNode = currNode.findChild(path) match {
                  case Some(child) => child
                  case None =>
                    currSchema.getType match {
                      case Schema.Type.RECORD =>
                        val field = currSchema.getField(path)
                        currSchema = field.schema.getType match {
                          case Schema.Type.UNION  => chana.avro.getNonNullOfUnion(field.schema)
                          case Schema.Type.RECORD => field.schema
                          case Schema.Type.ARRAY  => field.schema // TODO should be ArrayField ?
                          case Schema.Type.MAP    => field.schema // TODO should be MapKeyField/MapValueField ?
                          case _                  => field.schema
                        }

                        val child = Projection.FieldNode(path, Some(currNode), currSchema)
                        currNode.addChild(child)
                        child

                      case Schema.Type.UNION =>
                        val field = currSchema.getField(path)
                        currSchema = chana.avro.getNonNullOfUnion(field.schema)

                        val child = Projection.FieldNode(path, Some(currNode), currSchema)
                        currNode.addChild(child)
                        child

                      case Schema.Type.ARRAY => throw JPQLRuntimeException(currSchema, "is not a record when fetch its attribute: " + path) // TODO
                      case Schema.Type.MAP   => throw JPQLRuntimeException(currSchema, "is not a record when fetch its attribute: " + path) // TODO
                      case _                 => throw JPQLRuntimeException(currSchema, "is not a record when fetch its attribute: " + path)
                    }
                }
                currNode = pathNode
              }
            } // end while
            currNode.close()

          case _ => throw JPQLRuntimeException(attr.qual, "is not an AS alias of entity")
        }
      }
    }
  }

  // ----- overrided methods

  override def pathExprOrVarAccess(expr: PathExprOrVarAccess, record: Any): Any = {
    expr match {
      case PathExprOrVarAccess(Left(_qual), attrs) =>
        val qual = qualIdentVar(_qual, record)
        val paths = attrs map { x => attribute(x, record) }
        val attr = QualedAttribute(qual, paths)
        collectLeastProjectionNodes(attr)
        attr
      case PathExprOrVarAccess(Right(func), attrs) =>
        funcsReturningAny(func, record)
      // For MapValue, the field should have been collected during MapValue
    }
  }

  override def pathExpr(expr: PathExpr, record: Any): Any = {
    val qual = qualIdentVar(expr.qual, record)
    val paths = expr.attributes map { x => attribute(x, record) }
    val attr = QualedAttribute(qual, paths)
    collectLeastProjectionNodes(attr)
    attr
  }

  override def orderbyItem(item: OrderbyItem, record: Any): Any = {
    val orderingItem = item.expr match {
      case Left(x)  => simpleArithExpr(x, record)
      case Right(x) => scalarExpr(x, record)
    }
  }

  override def condExpr(expr: CondExpr, record: Any): Boolean = {
    expr.orTerms.foldLeft(condTerm(expr.term, record)) { (acc, orTerm) =>
      condTerm(orTerm, record)
    }
    true
  }

  override def condTerm(term: CondTerm, record: Any): Boolean = {
    term.andFactors.foldLeft(condFactor(term.factor, record)) { (acc, andFactor) =>
      condFactor(andFactor, record)
    }
    true
  }

  override def condFactor(factor: CondFactor, record: Any): Boolean = {
    val res = factor.expr match {
      case Left(x)  => condPrimary(x, record)
      case Right(x) => existsExpr(x, record)
    }
    true
  }

  override def condPrimary(primary: CondPrimary, record: Any): Boolean = {
    primary match {
      case x: CondExpr       => condExpr(x, record)
      case x: SimpleCondExpr => simpleCondExpr(x, record)
    }
  }

  override def simpleCondExpr(expr: SimpleCondExpr, record: Any): Boolean = {
    val base = expr.expr match {
      case Left(x)  => arithExpr(x, record)
      case Right(x) => nonArithScalarExpr(x, record)
    }
    expr.rem match {
      case x: ComparisonExpr => comparsionExprRightOperand(x.operand, record)
      case CondExprNotableWithNot(not, expr) =>
        expr match {
          case x: BetweenExpr          => betweenExpr(x, record)
          case x: LikeExpr             => likeExpr(x, record)
          case x: InExpr               => inExpr(x, record)
          case x: CollectionMemberExpr => collectionMemberExpr(x, record)
        }
      case IsExprWithNot(not, expr) =>
    }
    true
  }

  override def funcsReturningString(expr: FuncsReturningString, record: Any): String = {
    expr match {
      case Concat(expr, exprs: List[ScalarExpr]) =>
        scalarExpr(expr, record)
        exprs foreach { x => scalarExpr(x, record) }

      case Substring(expr, startExpr, lengthExpr: Option[ScalarExpr]) =>
        scalarExpr(expr, record)
        scalarExpr(startExpr, record)
        lengthExpr foreach { x => scalarExpr(x, record) }

      case Trim(trimSpec: Option[TrimSpec], trimChar: Option[TrimChar], from) =>
        stringPrimary(from, record)
        trimChar match {
          case Some(_: LiteralString) =>
          case Some(x: InputParam)    => inputParam(x, record)
          case None                   => ""
        }
        trimSpec match {
          case Some(BOTH) | None =>
          case Some(LEADING)     =>
          case Some(TRAILING)    =>
        }

      case Upper(expr) =>
        scalarExpr(expr, record)

      case Lower(expr) =>
        scalarExpr(expr, record)

      case MapKey(expr) =>
        val qual = varAccessOrTypeConstant(expr, record)
        val attr = QualedAttribute(qual, Nil)
        collectLeastProjectionNodes(attr)
    }
    ""
  }

  // SELECT e from Employee e join e.contactInfo c where KEY(c) = 'Email' and VALUE(c) = 'joe@gmail.com'
  override def funcsReturningAny(expr: FuncsReturningAny, record: Any): Any = {
    expr match {
      case MapValue(expr) =>
        val qual = varAccessOrTypeConstant(expr, record)
        val attr = QualedAttribute(qual, Nil)
        collectLeastProjectionNodes(attr)
      case JPQLJsonValue(jsonNode) =>
      // do nothing?
    }
  }

}

