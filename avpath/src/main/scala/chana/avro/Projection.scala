package chana.avro

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.SchemaBuilder.FieldAssembler

object Projection {
  sealed abstract class Node {
    def name: String
    def parent: Option[Node]
    def schema: Schema

    private var _children = List[Node]()
    def children = _children
    def containsChild(node: Node) = _children.contains(node)
    def findChild(name: String) = _children.find(_.name == name)
    def addChild(node: Node) = {
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
  final case class FieldNode(name: String, parent: Option[Node], schema: Schema) extends Node
  final case class MapKeyNode(name: String, parent: Option[Node], schema: Schema) extends Node
  final case class MapValueNode(name: String, parent: Option[Node], schema: Schema) extends Node

  def visitProjectionNode(projectionName: String, node: Node, fieldAssembler: FieldAssembler[Schema]): FieldAssembler[Schema] = {
    val schema = node.schema
    if (node.isLeaf) {
      fieldAssembler.name(node.name).`type`(schema).noDefault
    } else if (node.isRoot) {
      val assembler = SchemaBuilder.record(schema.getName).namespace(projectionNamespace(projectionName, schema.getNamespace)).fields
      node.children.foldLeft(assembler) { (acc, x) => visitProjectionNode(projectionName, x, acc) }
    } else {
      schema.getType match {
        case Schema.Type.RECORD =>
          val assembler = SchemaBuilder.record(schema.getName).namespace(projectionNamespace(projectionName, schema.getNamespace)).fields()
          val recSchema = node.children.foldLeft(assembler) { (acc, x) => visitProjectionNode(projectionName, x, acc) }.endRecord
          fieldAssembler.name(node.name).`type`(recSchema).noDefault
        case _ => throw new RuntimeException(node + " should not have children, with schema: " + schema)
      }
    }
  }

  private def projectionNamespace(projectionName: String, namespace: String) = {
    namespace + "." + projectionName
  }
}
