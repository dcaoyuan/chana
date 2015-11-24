package chana.xpath.nodes

import chana.xpath.rats.XPathGrammar
import java.io.StringReader
import xtc.tree.Node

class XPathParser {

  /**
   * main entrance
   */
  def parse(jpql: String) = {
    val reader = new StringReader(jpql)
    val grammar = new XPathGrammar(reader, "<current>")
    val r = grammar.pXPath(0)
    if (r.hasValue) {
      val rootNode = r.semanticValue[Node]
      val stmt = visitRoot(rootNode)
      stmt
    } else {
      throw new Exception(r.parseError.msg + " at " + r.parseError.index)
    }
  }

  def visitRoot(rootNode: Node) = {
    //JPQL(rootNode)
  }

  // -------- general node visit method
  private def visit[T](node: Node)(body: Node => T): T = {
    body(node)
  }

  private def visitOpt[T](node: Node)(body: Node => T): Option[T] = {
    if (node eq null) None
    else Some(visit(node)(body))
  }

  private def visitList[T](nodes: xtc.util.Pair[Node])(body: Node => T): List[T] = {
    if (nodes eq null) Nil
    else {
      var rs = List[T]()
      val xs = nodes.iterator
      while (xs.hasNext) {
        rs ::= visit(xs.next)(body)
      }
      rs.reverse
    }
  }

}
