package chana.jpql

import chana.avro.Projection
import chana.jpql.nodes._
import chana.schema.SchemaBoard
import org.apache.avro.Schema
import scala.collection.immutable

final class JPQLMetaEvaluator(jpqlKey: String, schemaBoard: SchemaBoard) extends JPQLEvaluator {

  def id = throw new UnsupportedOperationException("Do not call id method in " + this.getClass.getSimpleName)
  protected var asToEntity = Map[String, String]()
  protected var asToJoin = Map[String, List[String]]()
  override protected def addAsToEntity(as: String, entity: String) = asToEntity += (as -> entity)
  override protected def addAsToJoin(as: String, joinPath: List[String]) = asToJoin += (as -> joinPath)

  override protected def forceGather = true

  private var asToProjectionNode = Map[String, Projection.Node]()

  /**
   * Entrance
   */
  def collectMeta(stmt: Statement, record: Any): JPQLMeta = {
    stmt match {
      case stmt @ SelectStatement(select, from, where, groupby, having, orderby) =>
        val entity = fromClause(from, record) // collect asToEntity and asToJoin

        asToProjectionNode = asToEntity.foldLeft(Map[String, Projection.Node]()) {
          case (acc, (as, entity)) =>
            schemaBoard.schemaOf(entity) match {
              case Some(schema) => acc + (as -> Projection.FieldNode(schema.getName.toLowerCase, None, schema))
              case None         => acc
            }
        }

        selectClause(select, record)

        // skip wherecluse which is not necessary to be considered
        // visit groupby, having and orderby to collect necessary dataset
        groupby foreach { x => groupbyClause(x, record) }
        having foreach { x => havingClause(x, record) }
        orderby foreach { x => orderbyClause(x, record) }

        val projectionSchemas = asToProjectionNode map {
          case (as, projectionNode) => Projection.visitProjectionNode(jpqlKey, projectionNode, null).endRecord
        }

        val ids = stmt.collectSpecifiedIds.filter {
          case (as, id) => asToEntity.get(as).fold(false)(_ == entity)
        } map (_._2)

        JPQLSelect(stmt, entity, asToEntity, asToJoin, ids, projectionSchemas.toList)

      case stmt @ UpdateStatement(update, set, where) =>
        // visit updateClause is enough for meta
        val entity = updateClause(update, record)
        val ids = stmt.collectSpecifiedIds.filter {
          case (as, id) => asToEntity.get(as).fold(false)(_ == entity)
        } map (_._2)

        JPQLUpdate(stmt, entity, asToEntity, asToJoin, ids)

      case stmt @ DeleteStatement(delete, attributes, where) =>
        // visit deleteClause is enough for meta
        val entity = deleteClause(delete, record)
        val ids = stmt.collectSpecifiedIds.filter {
          case (as, id) => asToEntity.get(as).fold(false)(_ == entity)
        } map (_._2)

        JPQLDelete(stmt, entity, asToEntity, asToJoin, ids)

      case stmt @ InsertStatement(insert, attributes, values, where) =>
        // visit insertClause is enough for meta
        val entity = insertClause(insert, record)
        val ids = stmt.collectSpecifiedIds.filter {
          case (as, id) => asToEntity.get(as).fold(false)(_ == entity)
        } map (_._2)

        JPQLInsert(stmt, entity, asToEntity, asToJoin, ids)
    }
  }

  /**
   * TODO collect aliased value
   */
  private def collectLeastProjectionNodes(qual: String, attrPaths: List[String]) {
    if (isToGather) {
      val (qual1, attrPaths1) = asToJoin.get(qual) match {
        case Some(paths) => (paths.head, paths.tail ::: attrPaths)
        case None        => (qual, attrPaths)
      }

      if (attrPaths1.headOption != JPQLEvaluator.SOME_ID) {
        asToProjectionNode.get(qual1) match {
          case Some(projectionNode) =>
            var currSchema = projectionNode.schema
            var currNode: Projection.Node = projectionNode

            var paths = attrPaths1

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
                          case Schema.Type.RECORD => field.schema
                          case Schema.Type.UNION  => chana.avro.getNonNullOfUnion(field.schema)
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

          case _ => throw JPQLRuntimeException(qual1, "is not an AS alias of entity")
        }
      }
    }
  }

  // ----- overrided methods

  override def pathExprOrVarAccess(expr: PathExprOrVarAccess, record: Any): Any = {
    expr match {
      case PathExprOrVarAccess(Left(qual), attrs) =>
        val qual0 = qualIdentVar(qual, record)
        val paths = attrs map { x => attribute(x, record) }
        collectLeastProjectionNodes(qual0, paths)
      case PathExprOrVarAccess(Right(func), attrs) =>
        funcsReturningAny(func, record)
      // For MapValue, the field should have been collected during MapValue
    }
  }

  override def pathExpr(expr: PathExpr, record: Any): Any = {
    val qual0 = qualIdentVar(expr.qual, record)
    val paths = expr.attributes map { x => attribute(x, record) }
    collectLeastProjectionNodes(qual0, paths)
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
        collectLeastProjectionNodes(qual, List())
    }
    ""
  }

  // SELECT e from Employee e join e.contactInfo c where KEY(c) = 'Email' and VALUE(c) = 'joe@gmail.com'
  override def funcsReturningAny(expr: FuncsReturningAny, record: Any): Any = {
    expr match {
      case MapValue(expr) =>
        val qual = varAccessOrTypeConstant(expr, record)
        collectLeastProjectionNodes(qual, List())
      case JPQLJsonValue(jsonNode) =>
      // do nothing?
    }
  }

}

