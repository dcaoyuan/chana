package chana.jpql

import chana.avro.Projection
import chana.jpql.nodes._
import chana.schema.SchemaBoard
import org.apache.avro.Schema
import scala.collection.immutable

final class JPQLMetaEvaluator(jpqlKey: String, schemaBoard: SchemaBoard) extends JPQLEvaluator {

  protected var asToEntity = Map[String, String]()
  protected var asToJoin = Map[String, List[String]]()
  override protected def addAsToEntity(as: String, entity: String) = asToEntity += (as -> entity)
  override protected def addAsToJoin(as: String, joinPath: List[String]) = asToJoin += (as -> joinPath)

  private var asToProjectionNode = Map[String, Projection.Node]()

  /**
   * Entrance
   */
  def collectMeta(stmt: Statement, record: Any): JPQLMeta = {
    stmt match {
      case stmt @ SelectStatement(select, from, where, groupby, having, orderby) =>
        fromClause(from, record) // collect asToEntity and asToJoin

        asToProjectionNode = asToEntity.foldLeft(Map[String, Projection.Node]()) {
          case (acc, (as, entityName)) =>
            schemaBoard.schemaOf(entityName) match {
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

        JPQLSelect(stmt, asToEntity, asToJoin, projectionSchemas.toList)

      case stmt @ UpdateStatement(update, set, where) =>
        // visit updateClause is enough for meta
        updateClause(update, record)
        JPQLUpdate(stmt, asToEntity, asToJoin)

      case stmt @ DeleteStatement(delete, attributes, where) =>
        // visit deleteClause is enough for meta
        deleteClause(delete, record)
        JPQLDelete(stmt, asToEntity, asToJoin)

      case stmt @ InsertStatement(insert, attributes, values, where) =>
        // visit insertClause is enough for meta
        insertClause(insert, record)
        JPQLInsert(stmt, asToEntity, asToJoin)
    }
  }

  private def collectLeastProjectionNodes(qual: String, attrPaths: List[String]) {
    if (isToGather) {
      val (qual1, attrPaths1) = asToJoin.get(qual) match {
        case Some(paths) => (paths.head, paths.tail ::: attrPaths)
        case None        => (qual, attrPaths)
      }

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
          }
          currNode.close()

        case _ => throw JPQLRuntimeException(qual1, "is not an AS alias of entity")
      }
    }
  }

  override def pathExprOrVarAccess(expr: PathExprOrVarAccess, record: Any): Any = {
    expr match {
      case PathExprOrVarAccess_QualIdentVar(qual, attrs) =>
        val qualx = qualIdentVar(qual, record)
        val paths = attrs map { x => attribute(x, record) }
        collectLeastProjectionNodes(qualx, paths)
      case PathExprOrVarAccess_FuncsReturingAny(expr, attrs) =>
        funcsReturningAny(expr, record)
      // For MapValue, the field should have been collected during MapValue
    }
  }

  override def pathExpr(expr: PathExpr, record: Any): Any = {
    val qual = qualIdentVar(expr.qual, record)
    val paths = expr.attributes map { x => attribute(x, record) }
    collectLeastProjectionNodes(qual, paths)
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
      case CondPrimary_CondExpr(expr)       => condExpr(expr, record)
      case CondPrimary_SimpleCondExpr(expr) => simpleCondExpr(expr, record)
    }
  }

  override def simpleCondExpr(expr: SimpleCondExpr, record: Any): Boolean = {
    val base = expr.expr match {
      case Left(x)  => arithExpr(x, record)
      case Right(x) => nonArithScalarExpr(x, record)
    }
    expr.rem match {
      case SimpleCondExprRem_ComparisonExpr(expr) => comparsionExprRightOperand(expr.operand, record)
      case SimpleCondExprRem_CondWithNotExpr(not, expr) =>
        expr match {
          case CondWithNotExpr_BetweenExpr(expr)          => betweenExpr(expr, record)
          case CondWithNotExpr_LikeExpr(expr)             => likeExpr(expr, record)
          case CondWithNotExpr_InExpr(expr)               => inExpr(expr, record)
          case CondWithNotExpr_CollectionMemberExpr(expr) => collectionMemberExpr(expr, record)
        }
      case SimpleCondExprRem_IsExpr(not, expr) =>
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
          case Some(TrimChar_String(_))         =>
          case Some(TrimChar_InputParam(param)) => inputParam(param, record)
          case None                             => ""
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

