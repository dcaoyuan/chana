package chana.jpql

import chana.avro.Projection
import chana.jpql.nodes._
import chana.schema.SchemaBoard
import org.apache.avro.Schema
import scala.collection.immutable

final case class Metadata(projectionSchema: Schema, withGroupby: Boolean)

final class JPQLMetadataEvaluator(jpqlKey: String, schemaBoard: SchemaBoard) extends JPQLEvaluator {

  private var asToProjectionNode = Map[String, (Schema, Projection.Node)]()

  def collectMetadata(root: Statement, record: Any): Iterable[Schema] = {
    root match {
      case SelectStatement(select, from, where, groupby, having, orderby) =>
        fromClause(from, record)
        asToProjectionNode = asToEntity.foldLeft(Map[String, (Schema, Projection.Node)]()) {
          case (acc, (as, entityName)) =>
            schemaBoard.schemaOf(entityName) match {
              case Some(schema) => acc + (as -> (schema, Projection.FieldNode(schema.getName.toLowerCase, None, schema)))
              case None         => acc
            }
        }

        selectClause(select, record)

        // skip wherecluse which is not necessary to be considered
        // visit groupby, having and orderby to collect necessary dataset
        groupby foreach { x => groupbyClause(x, record) }
        having foreach { x => havingClause(x, record) }
        orderby foreach { x => orderbyClause(x, record) }

        asToProjectionNode map {
          case (as, (entitySchema, projectionNode)) => Projection.visitProjectionNode(jpqlKey, projectionNode, null).endRecord
        }

      case UpdateStatement(update, set, where) => null // NOT YET
      case DeleteStatement(delete, where)      => null // NOT YET
    }
  }

  private def collectLeastProjectionNodes(qual: String, attrPaths: List[String]) {
    if (isToGather) {
      asToProjectionNode.get(qual) match {
        case Some((schema, projectionNode)) =>
          var currSchema = schema
          var currNode: Projection.Node = projectionNode

          var paths = attrPaths
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
                        case Schema.Type.UNION  => chana.avro.getFirstNoNullTypeOfUnion(field.schema)
                        case Schema.Type.ARRAY  => field.schema // TODO should be ArrayField ?
                        case Schema.Type.MAP    => field.schema // TODO should be MapKeyField/MapValueField ?
                        case _                  => field.schema
                      }

                      val child = Projection.FieldNode(path, Some(currNode), currSchema)
                      currNode.addChild(child)
                      child

                    case Schema.Type.UNION =>
                      val field = currSchema.getField(path)
                      currSchema = chana.avro.getFirstNoNullTypeOfUnion(field.schema)

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

        case _ => throw JPQLRuntimeException(qual, "is not an AS alias of entity")
      }
    }
  }

  override def pathExprOrVarAccess(expr: PathExprOrVarAccess, record: Any): Any = {
    val qual = qualIdentVar(expr.qual, record)
    val paths = expr.attributes map { x => attribute(x, record) }
    collectLeastProjectionNodes(qual, paths)
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

}

