package chana.avpath

import java.util.regex.Pattern

object Parser {

  sealed trait Syntax
  final case class PathSyntax(fromRoot: Boolean, parts: List[Syntax]) extends Syntax
  final case class SelectorSyntax(selector: String, prop: String) extends Syntax
  final case class ObjPredSyntax(arg: Syntax) extends Syntax
  final case class PosPredSyntax(arg: PosSyntax) extends Syntax
  final case class LogicalExprSyntax(op: String, args: List[Syntax]) extends Syntax
  final case class ComparionExprSyntax(op: String, args: List[Syntax]) extends Syntax
  final case class MathExprSyntax(op: String, args: List[Syntax]) extends Syntax
  final case class ConcatExprSyntax(op: String, args: List[Syntax]) extends Syntax
  final case class UnaryExprSyntax(op: String, arg: Syntax) extends Syntax
  final case class PosSyntax(idx: Syntax, fromIdx: Syntax, toIdx: Syntax) extends Syntax
  final case class LiteralSyntax[T](value: T) extends Syntax
  final case class SubstSyntax(name: String) extends Syntax
  final case class MapKeysSyntax(keys: List[Either[String, Pattern]]) extends Syntax

  class AvroPathException(column: Int, msg: String) extends Exception(msg)

  object MESSAGES {
    val UNEXP_TOKEN = "Unexpected token "
    val UNEXP_EOP = "Unexpected end of path"
  }

  sealed trait Token[+T] {
    def value: T
    def from: Int
    def to: Int
  }
  final case class IdToken(value: String, from: Int, to: Int) extends Token[String]
  final case class FloatToken(value: Float, from: Int, to: Int) extends Token[Float]
  final case class IntToken(value: Int, from: Int, to: Int) extends Token[Int]
  final case class BoolToken(value: Boolean, from: Int, to: Int) extends Token[Boolean]
  final case class StrToken(value: String, from: Int, to: Int) extends Token[String]
  final case class PunctToken(value: String, from: Int, to: Int) extends Token[String]
  final case class EOPToken(value: String, from: Int, to: Int) extends Token[String]

  private val UNDEFINED_CHAR: Char = 0
  def char(path: Array[Char], idx: Int): Char = if (idx < path.length) path(idx) else UNDEFINED_CHAR
}

/**
 * Parser is not thread safe.
 */
final class Parser {
  import Parser._

  var path = Array[Char]()
  var len = 0
  var idx = 0
  var buf: Token[_] = _

  def parse(_path: String): PathSyntax = {
    path = _path.toCharArray
    len = path.length
    idx = 0
    buf = null

    val res = parsePath()

    lex[Any]() match {
      case x: EOPToken => res
      case x           => throw new AvroPathException(x.from, MESSAGES.UNEXP_TOKEN + x)
    }
  }

  def parsePath(): PathSyntax = {
    if (!matchPath()) {
      val token = lex()
      throw new AvroPathException(token.from, MESSAGES.UNEXP_TOKEN + token)
    }

    val fromRoot = if (match_("^")) {
      lex[Any]()
      true
    } else {
      false
    }

    var parts = List[Syntax]()
    var part: Syntax = null
    while ({ part = parsePathPart(); part != null }) {
      parts ::= part
    }

    PathSyntax(fromRoot, parts.reverse)
  }

  def parsePathPart(): Syntax = {
    if (matchSelector()) {
      parseSelector()
    } else if (match_("[")) {
      parsePosPredicate()
    } else if (match_("{")) {
      parseObjectPredicate()
    } else if (match_("(")) {
      parseConcatExpr()
    } else {
      null
    }
  }

  def parseSelector(): Syntax = {
    val selector = lex[String]().value
    val prop = lookahead() match {
      case _: IdToken | _: StrToken => lex[String]().value
      case _ if match_("*")         => lex[String]().value
      case _                        => null
    }

    SelectorSyntax(selector, prop)
  }

  def parsePosPredicate(): Syntax = {
    expect("[")
    val expr = parsePosExpr()
    expect("]")

    PosPredSyntax(expr)
  }

  def parseObjectPredicate(): Syntax = {
    expect("{")
    val expr = parseLogicalORExpr()
    expect("}")

    ObjPredSyntax(expr)
  }

  def parseLogicalORExpr(): Syntax = {
    val expr = parseLogicalANDExpr()

    var operands = List[Syntax]()
    while (match_("||")) {
      lex[Any]()
      operands = parseLogicalANDExpr() :: (if (operands.isEmpty) List(expr) else operands)
    }
    if (operands.nonEmpty) LogicalExprSyntax("||", operands.reverse) else expr
  }

  def parseLogicalANDExpr(): Syntax = {
    val expr = parseEqualityExpr()

    var operands = List[Syntax]()
    while (match_("&&")) {
      lex[Any]()
      operands = parseLogicalANDExpr() :: (if (operands.isEmpty) List(expr) else operands)
    }
    if (operands.nonEmpty) LogicalExprSyntax("&&", operands.reverse) else expr
  }

  def parseEqualityExpr(): Syntax = {
    var expr = parseRelationalExpr()
    while (match_("==") || match_("!=") || match_("===") || match_("!==") ||
      match_("^=") || match_("^==") || match_("$==") || match_("$=") || match_("*==") || match_("*=")) {
      expr = ComparionExprSyntax(lex[String]().value, List(expr, parseEqualityExpr()))
    }
    expr
  }

  def parseRelationalExpr(): Syntax = {
    var expr = parseAdditiveExpr()
    while (match_("<") || match_(">") || match_("<=") || match_(">=")) {
      expr = ComparionExprSyntax(lex[String]().value, List(expr, parseRelationalExpr()))
    }
    expr
  }

  def parseAdditiveExpr(): Syntax = {
    var expr = parseMultiplicativeExpr()
    while (match_("+") || match_("-")) {
      expr = MathExprSyntax(lex[String]().value, List(expr, parseAdditiveExpr()))
    }
    expr
  }

  def parseMultiplicativeExpr(): Syntax = {
    var expr = parseUnaryExpr()
    while (match_("*") || match_("/") || match_("%")) {
      expr = MathExprSyntax(lex[String]().value, List(expr, parseMultiplicativeExpr()))
    }
    expr
  }

  def parsePosExpr(): PosSyntax = {
    if (match_(":")) {
      lex[Any]()
      PosSyntax(null, null, parseUnaryExpr())
    } else {
      val fromExpr = parseUnaryExpr()
      if (match_(":")) {
        lex[Any]()
        if (match_("]")) {
          PosSyntax(null, fromExpr, null)
        } else {
          PosSyntax(null, fromExpr, parseUnaryExpr())
        }
      } else {
        PosSyntax(fromExpr, null, null)
      }
    }
  }

  def parseUnaryExpr(): Syntax = {
    if (match_("!") || match_("-")) {
      UnaryExprSyntax(lex[String]().value, parseUnaryExpr())
    } else {
      parsePrimaryExpr()
    }
  }

  def parsePrimaryExpr(): Syntax = {
    lookahead() match {
      case _: StrToken                                    => LiteralSyntax(lex[String]().value)
      case _: FloatToken                                  => LiteralSyntax(lex[Float]().value)
      case _: IntToken                                    => LiteralSyntax(lex[Int]().value)
      case _: BoolToken                                   => LiteralSyntax(lex[Boolean]().value)
      case PunctToken("*", _, _)                          => LiteralSyntax(lex[String]().value)
      case IdToken(value, _, _) if value.charAt(0) == '$' => SubstSyntax(lex[String]().value.substring(1))
      case _ =>
        if (matchPath()) {
          parsePath()
        } else if (match_("(")) {
          parseGroupExpr()
        } else {
          val token = lex()
          throw new AvroPathException(token.from, MESSAGES.UNEXP_TOKEN + token)
        }
    }
  }

  def parseGroupExpr(): Syntax = {
    expect("(")
    val expr = parseLogicalORExpr()
    expect(")")
    expr
  }

  def parseConcatExpr(): Syntax = {
    expect("(")

    parseStringOrRegex() match {
      case Some(key) =>
        var keys = List[Either[String, Pattern]](key)
        while (match_("|")) {
          lex[Any]()
          parseStringOrRegex() match {
            case Some(more) => keys = more :: keys
            case None       => // throw sth. ?
          }
        }

        expect(")")

        MapKeysSyntax(keys.reverse)

      case None =>
        val expr = parsePath()
        var operands = List[PathSyntax]()
        while (match_("|")) {
          lex[Any]()
          operands = parsePath() :: (if (operands.isEmpty) List(expr) else operands)
        }

        expect(")")

        if (operands.nonEmpty) ConcatExprSyntax("|", operands.reverse) else expr
    }
  }

  def parseStringOrRegex(): Option[Either[String, Pattern]] = {
    var isRegex = false
    if (match_("~")) {
      lex[Any]()
      isRegex = true
    }

    if (matchString()) {
      val token = lex[String]()
      val key = token.value
      if (isRegex) {
        try {
          Some(Right(Pattern.compile(key)))
        } catch {
          case ex: Throwable => throw new AvroPathException(token.from, ex.getMessage)
        }
      } else {
        Some(Left(key))
      }
    } else {
      None
    }
  }

  def match_(value: String) = {
    lookahead() match {
      case PunctToken(`value`, _, _) => true
      case _                         => false
    }
  }

  def matchPath() = matchSelector() || match_("(") || match_("^")

  def matchSelector() = {
    lookahead() match {
      case PunctToken(".", _, _) | PunctToken("..", _, _) => true
      case _ => false
    }
  }

  def matchString() = {
    lookahead() match {
      case _: StrToken => true
      case _           => false
    }
  }

  def expect(value: String) {
    lex[Any]() match {
      case PunctToken(`value`, _, _) =>
      case x                         => throw new AvroPathException(x.from, MESSAGES.UNEXP_TOKEN + x)
    }
  }

  def lookahead(): Token[_] = {
    if (buf != null) {
      buf
    } else {
      val pos = idx
      buf = advance[Any]()
      idx = pos
      buf
    }
  }

  /**
   * @param T type of token value
   */
  def lex[T](): Token[T] = {
    if (buf != null) {
      idx = buf.to
      val token = buf
      buf = null
      token.asInstanceOf[Token[T]]
    } else {
      advance[T]()
    }
  }

  /**
   * @param T type of token value
   */
  def advance[T](): Token[T] = {
    while (idx < len && isWhiteSpace(path(idx))) {
      idx += 1
    }

    if (idx >= len) {
      EOPToken("", idx, idx).asInstanceOf[Token[T]]
    } else {
      (scanPunctuator() orElse scanIdOrBool() orElse scanString() orElse scanNumeric()).getOrElse {
        throw new AvroPathException(idx, MESSAGES.UNEXP_TOKEN + path(idx))
      }.asInstanceOf[Token[T]]
    }
  }

  def isDigit(c: Char) = {
    "0123456789".indexOf(c) >= 0
  }

  def isWhiteSpace(c: Char) = {
    c == ' '
  }

  def isIdStart(c: Char) = {
    (c == '$') || (c == '_') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
  }

  def isIdPart(c: Char) = {
    isIdStart(c) || (c >= '0' && c <= '9')
  }

  def scanIdOrBool(): Option[Token[_]] = {
    var c = char(path, idx)
    if (!isIdStart(c)) {
      return None
    }

    val start = idx
    var id = "" + c
    idx += 1
    var break = false
    while (idx < len && !break) {
      c = char(path, idx)
      if (isIdPart(c)) {
        id += c
        idx += 1
      } else {
        break = true
      }
    }

    id match {
      case "true" | "false" =>
        Some(BoolToken(id == "true", start, idx))
      case _ =>
        Some(IdToken(id, start, idx))
    }
  }

  def scanString(): Option[StrToken] = {
    if (char(path, idx) != '"') {
      return None
    }

    idx += 1
    val start = idx
    var str = ""
    var break = false
    while (idx < len && !break) {
      char(path, idx) match {
        case '"' =>
          break = true
        case c =>
          str += c
      }
      idx += 1
    }

    Some(StrToken(str, start, idx))
  }

  def scanNumeric(): Option[Token[_]] = {
    val start = idx
    var c = char(path, idx)
    var isFloat = c == '.'

    if (isFloat || isDigit(c)) {
      var num = "" + c
      var break = false
      idx += 1
      while (idx < len && !break) {
        c = char(path, idx)
        if (!isDigit(c) && c != '.') {
          break = true
        } else {
          if (c == '.') {
            if (isFloat) {
              return None
            }
            isFloat = true
          }
          num += c
          idx += 1
        }
      }

      if (isFloat)
        Some(FloatToken(num.toFloat, start, idx))
      else
        Some(IntToken(num.toInt, start, idx))
    } else {
      None
    }
  }

  def scanPunctuator(): Option[PunctToken] = {
    val start = idx
    val c1 = char(path, idx)
    val c2 = char(path, idx + 1)

    if (c1 == '.') {
      if (isDigit(c2)) {
        return None
      } else {
        idx += 1
        if (char(path, idx) == '.') {
          idx += 1
          return Some(PunctToken("..", start, idx))
        } else {
          return Some(PunctToken(".", start, idx))
        }
      }
    }

    if (c2 == '=') {
      val c3 = char(path, idx + 2)
      if (c3 == '=') {
        if ("=!^$*".indexOf(c1) >= 0) {
          idx += 3
          return Some(PunctToken("" + c1 + c2 + c3, start, idx))
        }
      } else {
        if ("=!^$*><".indexOf(c1) >= 0) {
          idx += 2
          return Some(PunctToken("" + c1 + c2, start, idx))
        }
      }
    } else if (c2 == '|' && c1 == '|' || c2 == '&' && c1 == '&') {
      idx += 2
      return Some(PunctToken("" + c1 + c2, start, idx))
    }

    if (":{}()[]^+-*/%!><|~".indexOf(c1) >= 0) {
      idx += 1
      Some(PunctToken("" + c1, start, idx))
    } else {
      None
    }
  }
}
