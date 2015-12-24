package chana.xpath

import chana.xpath.nodes.XPathParser
import chana.xpath.rats.XPathGrammar
import java.io.StringReader
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import xtc.tree.Node

class XPathGrammarSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {

  def parse(query: String) {
    val reader = new StringReader(query)
    val grammar = new XPathGrammar(reader, "")
    val r = grammar.pXPath(0)
    if (r.hasValue) {
      // for the signature of method: <T> T semanticValue(), we have to call
      // with at least of Any to avoid:
      //   xtc.tree.GNode$Fixed1 cannot be cast to scala.runtime.Nothing$
      val rootNode = r.semanticValue[Node]
      info("\n\n## " + query + " ##\n\n" + rootNode)
      val parser = new XPathParser()
      val stmt = parser.parse(query)
      info("\nParsed:\n" + stmt)
    }

    assert(r.hasValue, "\n\n## " + query + " ##\n\n" + r.parseError.msg + " at " + r.parseError.index)
  }

  "XPathGrammar" when {
    "parse xpath statement" should {

      "with Selecting Nodes " in {
        val queris = List(
          "bookstore",
          " /bookstore ", // leave spaces at head and tail
          "bookstore/book",
          "//book",
          "bookstore//book",
          "//@lang")

        queris foreach parse

      }

      "with Predicates" in {
        val queris = List(
          "/bookstore/book[1]",
          "/bookstore/book[last()]",
          "/bookstore/book[last()-1]",
          "bookstore/book[position()<3]",
          "//title[@lang]",
          "//title[@lang='en']",
          "/bookstore/book[price>35.00]",
          "/bookstore/book[price>35.00]/title")

        queris foreach parse
      }

      "with Selecting Unknown Nodes" in {
        val queris = List(
          "*",
          "@*",
          "node()",
          "/bookstore/*",
          "//*",
          "//title[@*]")

        queris foreach parse
      }

      "with Selecting Several Paths" in {
        val queris = List(
          "//book/title | //book/price",
          "//title | //price",
          "/bookstore/book/title | //price")

        queris foreach parse
      }

      "with Location Path Expression" in {
        val queris = List(
          "child::book",
          "attribute::lang",
          "child::*",
          "attribute::*",
          "child::text()",
          "child::node()",
          "descendant::book",
          "ancestor::book",
          "ancestor-or-self::book",
          "child::*/child::price")

        queris foreach parse
      }

      "with XPath Operators" in {
        val queris = List(
          "//book | //cd",
          "6 + 4",
          "6 - 4",
          "6 * 4",
          "6 div 4",
          "6 idiv 4",
          "price=9.80",
          "price !=9.80",
          "price<9.80",
          "price<=9.80",
          "price>9.80",
          "price>=9.80",
          "price=9.80 or price=9.70",
          "price>9.00 and price<9.90",
          "5 mod 2")

        queris foreach parse
      }

    }
  }

}
