package chana.jpql

import chana.jpql.nodes._
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.SchemaBuilder.FieldAssembler
import scala.collection.immutable

sealed abstract class MetaNode {
  def name: String
  def parent: Option[MetaNode]
  def schema: Schema

  private var _children = List[MetaNode]()
  def children = _children
  def containsChild(node: MetaNode) = _children.contains(node)
  def findChild(name: String) = _children.find(_.name == name)
  def addChild(node: MetaNode) = {
    if (isClosed) {
      throw new RuntimeException("is closed, can not add child any more")
    } else {
      _children ::= node
      this
    }
  }

  private var _isClosed: Boolean = _
  def isClosed = _isClosed
  def close() { _isClosed = true }

  def isRoot = parent.isEmpty
  def isLeaf = _children.isEmpty

  override def toString = {
    val sb = new StringBuilder(this.getClass.getSimpleName).append("(")
    sb.append(name)
    if (children.nonEmpty) {
      sb.append(", ").append(children).append(")")
    }
    sb.append(")")
    sb.toString
  }
}
final case class FieldNode(name: String, parent: Option[MetaNode], schema: Schema) extends MetaNode
final case class MapKeyNode(name: String, parent: Option[MetaNode], schema: Schema) extends MetaNode
final case class MapValueNode(name: String, parent: Option[MetaNode], schema: Schema) extends MetaNode

final class JPQLMetadataEvaluator(id: String, schema: Schema) extends JPQLEvaluator {

  private var projectionPaths = new immutable.HashSet[String]()
  private var projectionNode = FieldNode(schema.getName.toLowerCase, None, schema)

  private def projectionNamespace(namespace: String) = {
    namespace + "." + id
  }

  def collectMetaSet(root: Statement, record: Any): Schema = {
    root match {
      case SelectStatement(select, from, where, groupby, having, orderby) =>
        fromClause(from, record)

        selectClause(select, record)

        // skip wherecluse which is not necessary to be considered
        // visit groupby, having and orderby to collect necessary dataset
        groupby foreach { x => groupbyClause(x, record) }
        having foreach { x => havingClause(x, record) }
        orderby foreach { x => orderbyClause(x, record) }

        println("projection nodes: " + projectionNode)
        var fieldAssembler = SchemaBuilder.record(schema.getName).namespace(projectionNamespace(schema.getNamespace)).fields
        visitProjectionNode(id, projectionNode, fieldAssembler).endRecord

      case UpdateStatement(update, set, where) => null // NOT YET
      case DeleteStatement(delete, where)      => null // NOT YET
    }
  }

  private def collectLeastProjectionPaths(qual: String, attrPaths: List[String]) {
    if (isToCollect) {
      val EntityName = schema.getName.toLowerCase
      asToEntity.get(qual) match {
        case Some(EntityName) =>
          var currSchema = schema
          var currNode: MetaNode = projectionNode

          var key = new StringBuilder(qual)
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

                      val child = FieldNode(path, Some(currNode), currSchema)
                      currNode.addChild(child)
                      child

                    case Schema.Type.UNION =>
                      val field = currSchema.getField(path)
                      currSchema = chana.avro.getFirstNoNullTypeOfUnion(field.schema)

                      val child = FieldNode(path, Some(currNode), currSchema)
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

        case _ => throw JPQLRuntimeException(qual, "is not an AS alias of entity: " + EntityName)
      }
    }
  }

  private def visitProjectionNode(id: String, node: MetaNode, fieldAssembler: FieldAssembler[Schema]): FieldAssembler[Schema] = {
    val schema = node.schema
    if (node.isLeaf) {
      fieldAssembler.name(node.name).`type`(schema).noDefault
    } else {
      schema.getType match {
        case Schema.Type.RECORD =>
          val nextFieldAssembler = SchemaBuilder.record(schema.getName).namespace(projectionNamespace(schema.getNamespace)).fields()
          node.children.foldLeft(nextFieldAssembler) { (acc, x) => visitProjectionNode(id, x, acc) }
        case _ => throw JPQLRuntimeException(schema, "should not have children: " + node)
      }
    }
  }

  override def pathExprOrVarAccess(expr: PathExprOrVarAccess, record: Any): Any = {
    val qual = qualIdentVar(expr.qual, record)
    val paths = expr.attributes map { x => attribute(x, record) }
    collectLeastProjectionPaths(qual, paths)
  }

  override def pathExpr(expr: PathExpr, record: Any): Any = {
    val qual = qualIdentVar(expr.qual, record)
    val paths = expr.attributes map { x => attribute(x, record) }
    collectLeastProjectionPaths(qual, paths)
  }

  override def orderbyItem(item: OrderbyItem, record: Any): Any = {
    val orderingItem = item.expr match {
      case Left(x)  => simpleArithExpr(x, record)
      case Right(x) => scalarExpr(x, record)
    }
  }
}

