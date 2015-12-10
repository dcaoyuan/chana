package chana.xpath.nodes

import chana.xpath.nodes._
import chana.xpath.rats.XPathGrammar
import java.io.StringReader
import xtc.tree.Node

final class XPathParser {

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
    XPath(rootNode)
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

  // =========================================================================

  def XPath(node: Node) = {
    visit(node.getNode(0))(expr)
  }

  /**
   * Param ( COMMA Param )*
   */
  def paramList(node: Node) = {
    val param0 = visit(node.getNode(0))(param)
    val params = visitList(node.getList(1))(param)
    ParamList(param0, params)
  }

  /**
   * DOLLAR EQName TypeDeclaration?
   */
  def param(node: Node) = {
    val name = visit(node.getNode(0))(eqName)
    val typeDecl = visitOpt(node.getNode(1))(typeDeclaration)
    Param(name, typeDecl)
  }

  /**
   * EnclosedExpr
   */
  def functionBody(node: Node) = {
    val expr = visit(node.getNode(0))(enclosedExpr)
    FunctionBody(expr)
  }

  /**
   * LBrace Expr RBrace
   */
  def enclosedExpr(node: Node) = {
    val expr0 = visit(node.getNode(0))(expr)
    EnclosedExpr(expr0)
  }

  /**
   * ExprSingle ( COMMA ExprSingle )*
   */
  def expr(node: Node) = {
    val expr0 = visit(node.getNode(0))(exprSingle)
    val exprs = visitList(node.getList(1))(exprSingle)
    Expr(expr0, exprs)
  }

  def exprSingle(node: Node): ExprSingle = {
    val n = node.getNode(0)
    n.getName match {
      case "ForExpr"        => visit(n)(forExpr)
      case "LetExpr"        => visit(n)(letExpr)
      case "QuantifiedExpr" => visit(n)(quantifiedExpr)
      case "IfExpr"         => visit(n)(ifExpr)
      case "OrExpr"         => visit(n)(orExpr)
    }
  }

  /**
   * SimpleForClause RETURN ExprSingle
   */
  def forExpr(node: Node) = {
    val forClause = visit(node.getNode(0))(simpleForClause)
    val returnExpr = visit(node.getNode(1))(exprSingle)
    ForExpr(forClause, returnExpr)
  }

  /**
   * FOR SimpleForBinding ( COMMA SimpleForBinding )*
   */
  def simpleForClause(node: Node) = {
    val binding0 = visit(node.getNode(0))(simpleForBinding)
    val bindings = visitList(node.getList(1))(simpleForBinding)
    SimpleForClause(binding0, bindings)
  }

  /**
   * DOLLAR VarName IN ExprSingle
   */
  def simpleForBinding(node: Node) = {
    val name = visit(node.getNode(0))(varName)
    val expr = visit(node.getNode(1))(exprSingle)
    SimpleForBinding(name, expr)
  }

  /**
   * SimpleLetClause RETURN ExprSingle
   */
  def letExpr(node: Node) = {
    val letClause = visit(node.getNode(0))(simpleLetClause)
    val returnExpr = visit(node.getNode(1))(exprSingle)
    LetExpr(letClause, returnExpr)
  }

  /**
   * LET SimpleLetBinding ( COMMA SimpleLetBinding )*
   */
  def simpleLetClause(node: Node) = {
    val binding0 = visit(node.getNode(0))(simpleLetBinding)
    val bindings = visitList(node.getList(1))(simpleLetBinding)
    SimpleLetClause(binding0, bindings)
  }

  /**
   * DOLLAR VarName ":=" ExprSingle
   */
  def simpleLetBinding(node: Node) = {
    val name = visit(node.getNode(0))(varName)
    val boundTo = visit(node.getNode(1))(exprSingle)
    SimpleLetBinding(name, boundTo)
  }

  /**
   * (SOME / EVERY) VarInExprSingle (COMMA VarInExprSingle)* SATISFIES ExprSingle
   */
  def quantifiedExpr(node: Node) = {
    val isEvery = node.getString(0) match {
      case "some"  => false
      case "every" => true
    }
    val expr0 = visit(node.getNode(1))(varInExprSingle)
    val exprs = visitList(node.getList(2))(varInExprSingle)
    val expr1 = visit(node.getNode(3))(exprSingle)
    QuantifiedExpr(isEvery, expr0, exprs, expr1)
  }

  /**
   * DOLLAR VarName IN ExprSingle
   */
  def varInExprSingle(node: Node) = {
    val name = visit(node.getNode(0))(varName)
    val expr0 = visit(node.getNode(1))(exprSingle)
    VarInExprSingle(name, expr0)
  }

  /**
   * IF LParen Expr RParen THEN ExprSingle ELSE ExprSingle
   */
  def ifExpr(node: Node) = {
    val expr0 = visit(node.getNode(0))(expr)
    val expr1 = visit(node.getNode(1))(exprSingle)
    val expr2 = visit(node.getNode(2))(exprSingle)
    IfExpr(expr0, expr1, expr2)
  }

  /**
   * AndExpr (OR AndExpr)*
   */
  def orExpr(node: Node) = {
    val expr0 = visit(node.getNode(0))(andExpr)
    val exprs = visitList(node.getList(1))(andExpr)
    OrExpr(expr0, exprs)
  }

  /**
   * ComparisonExpr (AND ComparisonExpr)*
   */
  def andExpr(node: Node) = {
    val expr0 = visit(node.getNode(0))(comparisonExpr)
    val exprs = visitList(node.getList(1))(comparisonExpr)
    AndExpr(expr0, exprs)
  }

  /**
   * StringConcatExpr ComparisonExprPostfix ?
   */
  def comparisonExpr(node: Node) = {
    val expr0 = visit(node.getNode(0))(stringConcatExpr)
    val expr1 = visitOpt(node.getNode(1))(comparisonExprPostfix)
    ComparisonExpr(expr0, expr1)
  }

  /**
   * ( ValueComp
   * / NodeComp
   * / GeneralComp
   * ) StringConcatExpr
   */
  def comparisonExprPostfix(node: Node) = {
    val n = node.getNode(0)
    val op = n.getName match {
      case "ValueComp"   => visit(n)(valueComp)
      case "NodeComp"    => visit(n)(nodeComp)
      case "GeneralComp" => visit(n)(generalComp)

    }
    val expr0 = visit(node.getNode(1))(stringConcatExpr)
    ComparisonExprPostfix(op, expr0)
  }

  /**
   * RangeExpr (DPIPE RangeExpr)*
   */
  def stringConcatExpr(node: Node) = {
    val expr0 = visit(node.getNode(0))(rangeExpr)
    val expr1 = visitList(node.getList(1))(rangeExpr)
    StringConcatExpr(expr0, expr1)
  }

  /**
   * AdditiveExpr (TO AdditiveExpr)?
   */
  def rangeExpr(node: Node) = {
    val expr0 = visit(node.getNode(0))(additiveExpr)
    val toExpr = visitOpt(node.getNode(1))(additiveExpr)
    RangeExpr(expr0, toExpr)
  }

  /**
   * MultiplicativeExpr (MultiplicativeExprPlus / MultiplicativeExprMinus)*
   */
  def additiveExpr(node: Node) = {
    val expr0 = visit(node.getNode(0))(multiplicativeExpr)
    val exprs = visitList(node.getList(1)) { n =>
      n.getName match {
        case "MultiplicativeExprPlus"  => visit(n)(multiplicativeExprPlus)
        case "MultiplicativeExprMinus" => visit(n)(multiplicativeExprMinus)
      }
    }
    AdditiveExpr(expr0, exprs)
  }

  /**
   * PLUS MultiplicativeExpr
   */
  def multiplicativeExprPlus(node: Node) = {
    val x = visit(node.getNode(1))(multiplicativeExpr)
    MultiplicativeExpr(Plus, x.unionExpr, x.prefixedUnionExprs)
  }

  /**
   * MINUS MultiplicativeExpr
   */
  def multiplicativeExprMinus(node: Node) = {
    val x = visit(node.getNode(1))(multiplicativeExpr)
    MultiplicativeExpr(Minus, x.unionExpr, x.prefixedUnionExprs)
  }

  /**
   * UnionExpr (UnionExprMultiply / UnionExprDiv / UnionExprIdiv / UnionExprMod)*
   */
  def multiplicativeExpr(node: Node) = {
    val expr0 = visit(node.getNode(0))(unionExpr)
    val exprs = visitList(node.getList(1)) { n =>
      n.getName match {
        case "UnionExprMultiply" => visit(n)(unionExprMultiply)
        case "UnionExprDiv"      => visit(n)(unionExprDiv)
        case "UnionExprIdiv"     => visit(n)(unionExprIdiv)
        case "UnionExprMod"      => visit(n)(unionExprMod)
      }
    }
    MultiplicativeExpr(Nop, expr0, exprs)
  }

  /**
   * ASTER UnionExpr
   */
  def unionExprMultiply(node: Node) = {
    val x = visit(node.getNode(1))(unionExpr)
    UnionExpr(Aster, x.intersectExceptExpr, x.prefixedIntersectExceptExprs)
  }

  /**
   * DIV UnionExpr
   */
  def unionExprDiv(node: Node) = {
    val x = visit(node.getNode(0))(unionExpr)
    UnionExpr(Div, x.intersectExceptExpr, x.prefixedIntersectExceptExprs)
  }

  /**
   * IDIV UnionExpr
   */
  def unionExprIdiv(node: Node) = {
    val x = visit(node.getNode(0))(unionExpr)
    UnionExpr(IDiv, x.intersectExceptExpr, x.prefixedIntersectExceptExprs)
  }

  /**
   * MOD UnionExpr
   */
  def unionExprMod(node: Node) = {
    val x = visit(node.getNode(0))(unionExpr)
    UnionExpr(Mod, x.intersectExceptExpr, x.prefixedIntersectExceptExprs)
  }

  /**
   * IntersectExceptExpr (IntersectExceptExprUnion / IntersectExceptExprList)*
   */
  def unionExpr(node: Node) = {
    val expr0 = visit(node.getNode(0))(intersectExceptExpr)
    val exprs = visitList(node.getList(1)) { n =>
      n.getName match {
        case "IntersectExceptExprUnion" => visit(n)(intersectExceptExprUnion)
        case "IntersectExceptExprList"  => visit(n)(intersectExceptExprList)
      }
    }
    UnionExpr(Nop, expr0, exprs)
  }

  /**
   * UNION IntersectExceptExpr
   */
  def intersectExceptExprUnion(node: Node) = {
    val x = visit(node.getNode(0))(intersectExceptExpr)
    IntersectExceptExpr(Union, x.instanceOfExpr, x.prefixedInstanceOfExprs)
  }

  /**
   * PIPE IntersectExceptExpr
   */
  def intersectExceptExprList(node: Node) = {
    val x = visit(node.getNode(0))(intersectExceptExpr)
    IntersectExceptExpr(Pipe, x.instanceOfExpr, x.prefixedInstanceOfExprs)
  }

  /**
   * InstanceofExpr (InstanceofExprIntersect / InstanceofExprExcept)*
   */
  def intersectExceptExpr(node: Node) = {
    val expr0 = visit(node.getNode(0))(instanceofExpr)
    val exprs = visitList(node.getList(1)) { n =>
      n.getName match {
        case "InstanceofExprIntersect" => visit(n)(instanceofExprIntersect)
        case "InstanceofExprExcept"    => visit(n)(instanceofExprExcept)
      }
    }
    IntersectExceptExpr(Nop, expr0, exprs)
  }

  /**
   * INTERSECT InstanceofExpr
   */
  def instanceofExprIntersect(node: Node) = {
    val x = visit(node.getNode(0))(instanceofExpr)
    InstanceofExpr(Intersect, x.treatExpr, x.ofType)
  }

  /**
   * EXCEPT InstanceofExpr
   */
  def instanceofExprExcept(node: Node) = {
    val x = visit(node.getNode(0))(instanceofExpr)
    InstanceofExpr(Except, x.treatExpr, x.ofType)
  }

  /**
   * TreatExpr (INSTANCE OF SequenceType)?
   */
  def instanceofExpr(node: Node) = {
    val expr0 = visit(node.getNode(0))(treatExpr)
    val ofType = visitOpt(node.getNode(1))(sequenceType)
    InstanceofExpr(Nop, expr0, ofType)
  }

  /**
   * CastableExpr (TREAT AS SequenceType)?
   */
  def treatExpr(node: Node) = {
    val expr0 = visit(node.getNode(0))(castableExpr)
    val asType = visitOpt(node.getNode(1))(sequenceType)
    TreatExpr(expr0, asType)
  }

  /**
   * CastExpr (CASTABLE AS SingleType)?
   */
  def castableExpr(node: Node) = {
    val expr0 = visit(node.getNode(0))(castExpr)
    val asType = visitOpt(node.getNode(1))(singleType)
    CastableExpr(expr0, asType)
  }

  /**
   * UnaryExpr (CAST AS SingleType)?
   */
  def castExpr(node: Node) = {
    val expr0 = visit(node.getNode(0))(unaryExpr)
    val asType = visitOpt(node.getNode(1))(singleType)
    CastExpr(expr0, asType)
  }

  /**
   * (MINUS / PLUS)* ValueExpr
   */
  def unaryExpr(node: Node) = {
    val prefix = node.getString(0) match {
      case null => Nop
      case "+"  => Plus
      case "-"  => Minus
    }

    val value = visit(node.getNode(1))(valueExpr)
    UnaryExpr(prefix, value)
  }

  /**
   * SimpleMapExpr
   */
  def valueExpr(node: Node) = {
    val value = visit(node.getNode(0))(simpleMapExpr)
    ValueExpr(value)
  }

  def generalComp(node: Node) = {
    val op = node.getString(0)
    GeneralComp(op)
  }

  def valueComp(node: Node) = {
    val op = node.getString(0)
    ValueComp(op)
  }

  def nodeComp(node: Node) = {
    val op = node.getString(0)
    NodeComp(op)
  }

  /**
   * PathExpr ( void:"!" PathExpr )*
   */
  def simpleMapExpr(node: Node) = {
    val path = visit(node.getNode(0))(pathExpr)
    val exclams = visitList(node.getList(1))(pathExpr)
    SimpleMapExpr(path, exclams)
  }

  /**
   *   "//" RelativePathExpr
   * / "/"  RelativePathExpr?
   * / RelativePathExpr
   */
  def pathExpr(node: Node) = {
    node.get(0) match {
      case "//"    => PathExpr(Relative, Some(visit(node.getNode(1))(relativePathExpr)))
      case "/"     => PathExpr(Absolute, visitOpt(node.getNode(1))(relativePathExpr))
      case n: Node => PathExpr(Nop, Some(visit(n)(relativePathExpr)))
    }
  }

  /**
   * StepExpr ( StepExprAbsolute / StepExprRelative )*
   */
  def relativePathExpr(node: Node) = {
    val step = visit(node.getNode(0))(stepExpr)
    val steps = visitList(node.getList(1)) { n =>
      n.getName match {
        case "StepExprAbsolute" => visit(n)(stepExprAbsolute)
        case "StepExprRelative" => visit(n)(stepExprRelative)
      }
    }
    RelativePathExpr(StepExpr(Nop, step), steps)
  }

  /**
   * void:"/" StepExpr
   */
  def stepExprAbsolute(node: Node) = {
    val stepExpr0 = visit(node.getNode(0))(stepExpr)
    StepExpr(Absolute, stepExpr0)
  }

  /**
   * void:"//" StepExpr
   */
  def stepExprRelative(node: Node) = {
    val stepExpr0 = visit(node.getNode(0))(stepExpr)
    StepExpr(Relative, stepExpr0)
  }

  /**
   *   PostfixExpr
   * / AxisStep
   */
  def stepExpr(node: Node) = {
    val n = node.getNode(0)
    n.getName match {
      case "PostfixExpr" => Left(visit(n)(postfixExpr))
      case "AxisStep"    => Right(visit(n)(axisStep))
    }
  }

  /**
   * ( ReverseStep / ForwardStep ) PredicateList
   */
  def axisStep(node: Node): AxisStep = {
    val preds = visit(node.getNode(1))(predicateList)
    val n = node.getNode(0)
    n.getName match {
      case "ReverseStep" => ReverseAxisStep(visit(n)(reverseStep), preds)
      case "ForwardStep" => ForwardAxisStep(visit(n)(forwardStep), preds)
    }
  }

  /**
   *   ForwardAxis NodeTest
   * / AbbrevForwardStep
   */
  def forwardStep(node: Node): ForwardStep = {
    val n = node.getNode(0)
    n.getName match {
      case "ForwardAxis" =>
        val axis = visit(n)(forwardAxis)
        val nodeTst = visit(node.getNode(1))(nodeTest)
        ForwardStep_Axis(axis, nodeTst)
      case "AbbrevForwardStep" => visit(n)(abbrevForwardStep)
    }
  }

  def forwardAxis(node: Node): ForwardAxis = {
    node.getString(0) match {
      case "child"              => Child
      case "descendant"         => Descendant
      case "attribute"          => Attribute
      case "self"               => Self
      case "descendant-or-self" => DescendantOrSelf
      case "following-sibling"  => FollowingSibling
      case "following"          => Following
      case "namespace"          => Namespace
    }
  }

  /**
   * "@"? NodeTest
   */
  def abbrevForwardStep(node: Node) = {
    val withAt = node.get(0) match {
      case null => false
      case "@"  => true
    }
    val nodeTst = visit(node.getNode(1))(nodeTest)
    AbbrevForwardStep(nodeTst, withAt)
  }

  /**
   *   ReverseAxis NodeTest
   * / AbbrevReverseStep
   */
  def reverseStep(node: Node): ReverseStep = {
    node.get(0) match {
      case n: Node =>
        val axis = visit(n)(reverseAxis)
        val nodeTst = visit(node.getNode(1))(nodeTest)
        ReverseStep_Axis(axis, nodeTst)
      case _ => AbbrevReverseStep
    }
  }

  def reverseAxis(node: Node): ReverseAxis = {
    node.getString(0) match {
      case "parent"            => Parent
      case "ancestor"          => Ancestor
      case "preceding-sibling" => PrecedingSibling
      case "preceding"         => Preceding
      case "ancestor-or-self"  => AncestorOrSelf
    }
  }

  /**
   * ".."
   */
  def abbrevReverseStep(node: Node) = {
  }

  /**
   *   KindTest
   * / NameTest
   */
  def nodeTest(node: Node): NodeTest = {
    val n = node.getNode(0)
    n.getName match {
      case "KindTest" => visit(n)(kindTest)
      case "NameTest" => visit(n)(nameTest)
    }
  }

  /**
   *   EQName
   * / Wildcard
   */
  def nameTest(node: Node): NameTest = {
    val n = node.getNode(0)
    n.getName match {
      case "EQName"   => NameTest_Name(visit(n)(eqName))
      case "Wildcard" => NameTest_Wildcard(visit(n)(wildcard))
    }
  }

  /**
   *   ASTER
   * / NCName COLON ASTER
   * / ASTER COLON NCName
   * / BracedURILiteral ASTER
   */
  def wildcard(node: Node): Wildcard = {
    node.get(0) match {
      case "*" =>
        node.size match {
          case 1 => Aster
          case 2 =>
            val name = node.getString(1)
            AsterName(name)
        }

      case n: Node =>
        val uri = visit(node.getNode(0))(bracedURILiteral)
        UriAster(uri)

      case name: String => NameAster(name)
    }
  }

  /**
   * PrimaryExpr ( Predicate / ArgumentList / Lookup / ArrowPostfix )*
   */
  def postfixExpr(node: Node) = {
    val expr0 = visit(node.getNode(0))(primaryExpr)
    val ns = visitList(node.getList(1)) { n =>
      n.getName match {
        case "Predicate"    => Postfix_Predicate(visit(n)(predicate))
        case "ArgumentList" => Postfix_Arguments(visit(n)(argumentList))
        case "Lookup"       => Postfix_Lookup(visit(n)(lookup))
        case "ArrowPostfix" => Postfix_ArrowPostfix(visit(n)(arrowPostfix))
      }

    }
    PostfixExpr(expr0, ns)
  }

  /**
   *   LParen RParen
   * / LParen Argument ( COMMA Argument )* RParen
   */
  def argumentList(node: Node) = {
    val args = node.size match {
      case 0 => Nil
      case _ =>
        val arg0 = visit(node.getNode(0))(argument)
        val args = visitList(node.getList(0))(argument)
        arg0 :: args
    }
    ArgumentList(args)
  }

  /**
   * Predicate*
   */
  def predicateList(node: Node) = {
    val preds = visitList(node.getList(0))(predicate)
    PredicateList(preds)
  }

  /**
   * LBrack Expr RBrack
   */
  def predicate(node: Node) = {
    val expr0 = visit(node.getNode(0))(expr)
    Predicate(expr0)
  }

  /**
   * QUEST KeySpecifier
   */
  def lookup(node: Node) = {
    val spec = visit(node.getNode(0))(keySpecifier)
    Lookup(spec)
  }

  /**
   *   NCName
   * / IntegerLiteral
   * / ParenthesizedExpr
   * / ASTER
   */
  def keySpecifier(node: Node): KeySpecifier = {
    node.get(0) match {
      case "*"          => KeySpecifier_ASTER
      case name: String => KeySpecifier_NCName(name)
      case n: Node =>
        n.getName match {
          case "IntegerLiteral"    => KeySpecifier_IntegerLiteral(visit(n)(integerLiteral))
          case "ParenthesizedExpr" => KeySpecifier_ParenthesizedExpr(visit(n)(parenthesizedExpr))
        }
    }
  }

  /**
   * "=>" ArrowFunctionSpecifier ArgumentList
   */
  def arrowPostfix(node: Node) = {
    val spec = visit(node.getNode(0))(arrowFunctionSpecifier)
    val args = visit(node.getNode(1))(argumentList)
    ArrowPostfix(spec, args)
  }

  /**
   *   EQName
   * / VarRef
   * / ParenthesizedExpr
   */
  def arrowFunctionSpecifier(node: Node): ArrowFunctionSpecifier = {
    val n = node.getNode(0)
    n.getName match {
      case "EQName"            => ArrowFunctionSpecifier_EQName(visit(n)(eqName))
      case "VarRef"            => ArrowFunctionSpecifier_VarRef(visit(n)(varRef))
      case "ParenthesizedExpr" => ArrowFunctionSpecifier_ParenthesizedExpr(visit(n)(parenthesizedExpr))
    }
  }

  /**
   *   Literal
   * / VarRef
   * / ParenthesizedExpr
   * / ContextItemExpr
   * / FunctionCall
   * / FunctionItemExpr
   * / MapConstructor
   * / ArrayConstructor
   * / UnaryLookup
   */
  def primaryExpr(node: Node): PrimaryExpr = {
    val n = node.getNode(0)
    n.getName match {
      case "Literal"           => PrimaryExpr_Literal(visit(n)(literal))
      case "VarRef"            => PrimaryExpr_VarRef(visit(n)(varRef))
      case "ParenthesizedExpr" => PrimaryExpr_ParenthesizedExpr(visit(n)(parenthesizedExpr))
      case "ContextItemExpr"   => PrimaryExpr_ContextItemExpr
      case "FunctionCall"      => PrimaryExpr_FunctionCall(visit(n)(functionCall))
      case "FunctionItemExpr"  => PrimaryExpr_FunctionItemExpr(visit(n)(functionItemExpr))
      case "MapConstructor"    => PrimaryExpr_MapConstructor(visit(n)(mapConstructor))
      case "ArrayConstructor"  => PrimaryExpr_ArrayConstructor(visit(n)(arrayConstructor))
      case "UnaryLookup"       => PrimaryExpr_UnaryLookup(visit(n)(unaryLookup))
    }
  }

  /**
   *   NumericLiteral
   * / StringLiteral
   */
  def literal(node: Node): Literal = {
    val n = node.getNode(0)
    n.getName match {
      case "NumericLiteral" => Literal(visit(n)(numericLiteral))
      case "StringLiteral"  => Literal(n.getString(0))
    }
  }

  /**
   *   DoubleLiteral
   * / DecimalLiteral
   * / IntegerLiteral
   *
   * Note: force return type to Number to avoid being infered to double
   */
  def numericLiteral(node: Node): Any = {
    val n = node.getNode(0)
    n.getName match {
      case "DoubleLiteral"  => visit(n)(doubleLiteral)
      case "DecimalLiteral" => visit(n)(decimalLiteral)
      case "IntegerLiteral" => visit(n)(integerLiteral)
    }
  }

  def doubleLiteral(node: Node): Double = {
    val v = node.getString(0)
    java.lang.Double.parseDouble(v)
  }

  def decimalLiteral(node: Node): Double = {
    val v = node.getString(0)
    java.lang.Double.parseDouble(v)
  }

  def integerLiteral(node: Node): Int = {
    val v = node.getString(0)
    // TODO also check Long value
    java.lang.Integer.parseInt(v)
  }

  /**
   * DOLLAR VarName
   */
  def varRef(node: Node) = {
    val name = visit(node.getNode(0))(varName)
    VarRef(name)
  }

  def varName(node: Node) = {
    val name = visit(node.getNode(0))(eqName)
    VarName(name)
  }

  /**
   * LParen Expr? RParen
   */
  def parenthesizedExpr(node: Node) = {
    val exprOpt = visitOpt(node.getNode(0))(expr)
    ParenthesizedExpr(exprOpt)
  }

  /**
   * DOT
   */
  def contextItemExpr(node: Node) = {
    PrimaryExpr_ContextItemExpr
  }

  /**
   * EQName ArgumentList
   */
  def functionCall(node: Node) = {
    val name = visit(node.getNode(0))(eqName)
    val args = visit(node.getNode(1))(argumentList)
    FunctionCall(name, args)
  }

  /**
   *   ExprSingle
   * / ArgumentPlaceholder
   */
  def argument(node: Node): Argument = {
    val n = node.getNode(0)
    n.getName match {
      case "ExprSingle"          => Argument_ExprSingle(visit(n)(exprSingle))
      case "ArgumentPlaceholder" => ArgumentPlaceholder
    }
  }

  /**
   * QUEST
   */
  def argumentPlaceholder(node: Node) = {
    ArgumentPlaceholder
  }

  /**
   *   NamedFunctionRef
   * / InlineFunctionExpr
   */
  def functionItemExpr(node: Node) = {
    val n = node.getNode(0)
    n.getName match {
      case "NamedFunctionRef"   => visit(n)(namedFunctionRef)
      case "InlineFunctionExpr" => visit(n)(inlineFunctionExpr)
    }
  }

  /**
   * EQName void:"#" IntegerLiteral
   */
  def namedFunctionRef(node: Node) = {
    val name = visit(node.getNode(0))(eqName)
    val idx = node.get(1).asInstanceOf[Int]
    NamedFunctionRef(name, idx)
  }

  /**
   * FUNCTION LParen ParamList? RParen ( AS SequenceType )? FunctionBody
   */
  def inlineFunctionExpr(node: Node) = {
    val params = visitOpt(node.getNode(0))(paramList)
    val asType = visitOpt(node.getNode(1))(sequenceType)
    val body = visit(node.getNode(2))(functionBody)
    InlineFunctionExpr(params, asType, body)
  }

  /**
   *   MAP LBrace RBrace
   * / MAP LBrace MapConstructorEntry ( COMMA MapConstructorEntry )* RBrace
   */
  def mapConstructor(node: Node) = {
    val args = node.size match {
      case 0 => Nil
      case _ =>
        val entry0 = visit(node.getNode(0))(mapConstructorEntry)
        val entries = visitList(node.getList(1))(mapConstructorEntry)
        entry0 :: entries
    }
    MapConstructor(args)
  }

  /**
   * MapKeyExpr COLON MapValueExpr
   */
  def mapConstructorEntry(node: Node) = {
    val key = visit(node.getNode(0))(mapKeyExpr)
    val value = visit(node.getNode(1))(mapValueExpr)
    MapConstructorEntry(key, value)
  }

  def mapKeyExpr(node: Node) = {
    val expr0 = visit(node.getNode(0))(exprSingle)
    MapKeyExpr(expr0)
  }

  def mapValueExpr(node: Node) = {
    val expr0 = visit(node.getNode(0))(exprSingle)
    MapValueExpr(expr0)
  }

  /**
   *   SquareArrayConstructor
   * / BraceArrayConstructor
   */
  def arrayConstructor(node: Node) = {
    node.getName match {
      case "SquareArrayConstructor" => visit(node.getNode(0))(squareArrayConstructor)
      case "BraceArrayConstructor"  => visit(node.getNode(0))(braceArrayConstructor)
    }
  }

  /**
   *   LBrack RBrack
   * / LBrack ExprSingle ( COMMA ExprSingle )* RBrack
   */
  def squareArrayConstructor(node: Node) = {
    val args = node.size match {
      case 0 => Nil
      case _ =>
        val expr0 = visit(node.getNode(0))(exprSingle)
        val exprs = visitList(node.getList(1))(exprSingle)
        expr0 :: exprs
    }
    SquareArrayConstructor(args)
  }

  /**
   * ARRAY LBrace Expr? RBrace
   */
  def braceArrayConstructor(node: Node) = {
    val exprOpt = visitOpt(node.getNode(0))(expr)
    BraceArrayConstructor(exprOpt)
  }

  /**
   * QUEST KeySpecifier
   */
  def unaryLookup(node: Node) = {
    val keySpec = visit(node.getNode(1))(keySpecifier)
    UnaryLookup(keySpec)
  }

  /**
   * SimpleTypeName QUEST?
   */
  def singleType(node: Node) = {
    val name = visit(node.getNode(0))(simpleTypeName)
    val withQuest = node.getNode(1) ne null
    SingleType(name, withQuest)
  }

  /**
   * AS SequenceType
   */
  def typeDeclaration(node: Node) = {
    val asType = visit(node.getNode(0))(sequenceType)
    TypeDeclaration(asType)
  }

  /**
   *   EMPTY_SEQUENCE LParen RParen
   * / ItemType OccurrenceIndicator?
   */
  def sequenceType(node: Node): SequenceType = {
    node.size match {
      case 0 => SequenceType_Empty
      case _ =>
        val itemTpe = visit(node.getNode(0))(itemType)
        val occuInd = visitOpt(node.getNode(1))(occurrenceIndicator)
        SequenceType_ItemType(itemTpe, occuInd)
    }
  }

  /**
   *   QUEST
   * / ASTER
   * / PLUS
   */
  def occurrenceIndicator(node: Node) = {
    node.getString(0) match {
      case "?" => OccurrenceIndicator_QUEST
      case "*" => OccurrenceIndicator_ASTER
      case "+" => OccurrenceIndicator_PLUS
    }
  }

  /**
   *   KindTest
   * / ITEM LParen RParen
   * / FunctionTest
   * / MapTest
   * / ArrayTest
   * / AtomicOrUnionType
   * / ParenthesizedItemType
   */
  def itemType(node: Node): ItemType = {
    if (node.size > 0) {
      val n = node.getNode(0)
      n.getName match {
        case "KindTest"              => visit(n)(kindTest)
        case "FunctionTest"          => visit(n)(functionTest)
        case "MapTest"               => visit(n)(mapTest)
        case "ArrayTest"             => visit(n)(arrayTest)
        case "AtomicOrUnionType"     => visit(n)(atomicOrUnionType)
        case "ParenthesizedItemType" => visit(n)(parenthesizedItemType)
      }
    } else {
      ItemType_ITEM
    }
  }

  def atomicOrUnionType(node: Node) = {
    val name = visit(node.getNode(0))(eqName)
    AtomicOrUnionType(name)
  }

  /**
   *   DocumentTest
   * / ElementTest
   * / AttributeTest
   * / SchemaElementTest
   * / SchemaAttributeTest
   * / PITest
   * / CommentTest
   * / TextTest
   * / NamespaceNodeTest
   * / AnyKindTest
   */
  def kindTest(node: Node) = {
    val n = node.getNode(0)
    n.getName match {
      case "DocumentTest"        => visit(n)(documentTest)
      case "ElementTest"         => visit(n)(elementTest)
      case "AttributeTest"       => visit(n)(attributeTest)
      case "SchemaElementTest"   => visit(n)(schemaElementTest)
      case "SchemaAttributeTest" => visit(n)(schemaAttributeTest)
      case "PITest"              => visit(n)(piTest)
      case "CommentTest"         => visit(n)(commentTest)
      case "TextTest"            => visit(n)(textTest)
      case "NamespaceNodeTest"   => visit(n)(namespaceNodeTest)
      case "AnyKindTest"         => visit(n)(anyKindTest)
    }
  }

  /**
   * NODE LParen RParen
   */
  def anyKindTest(node: Node) = {
    AnyKindTest
  }

  /**
   * DOCUMENT_NODE LParen ( ElementTest / SchemaElementTest )? RParen
   */
  def documentTest(node: Node) = {
    val n = node.getNode(0)
    val test = n.getName match {
      case "ElementTest"       => visitOpt(n)(elementTest)
      case "SchemaElementTest" => visitOpt(n)(schemaElementTest)
    }
    DocumentTest(test)
  }

  /**
   * TEXT LParen RParen
   */
  def textTest(node: Node) = {
    TextTest
  }

  /**
   * COMMENT LParen RParen
   */
  def commentTest(node: Node) = {
    CommentTest
  }

  /**
   * NAMESPACE_NODE LParen RParen
   */
  def namespaceNodeTest(node: Node) = {
    NamespaceNodeTest
  }

  /**
   * PROCESSING_INSTRUCTION LParen ( NCName / StringLiteral )? RParen
   */
  def piTest(node: Node) = {
    val name = node.get(0) match {
      case null      => None
      case x: String => Some(x)
    }
    PITest(name)
  }

  /**
   *   ATTRIBUTE LParen RParen
   * / ATTRIBUTE LParen AttribNameOrWildcard ( COMMA TypeName )? RParen
   */
  def attributeTest(node: Node) = {
    node.size match {
      case 0 => AttributeTest_Empty
      case _ =>
        val name = visit(node.getNode(0))(attribNameOrWildcard)
        val tpeNameOpt = visitOpt(node.getNode(1))(typeName)
        AttributeTest_Name(name, tpeNameOpt)
    }
  }

  /**
   *   AttributeName
   * / ASTER
   */
  def attribNameOrWildcard(node: Node) = {
    node.get(0) match {
      case n: Node => Left(visit(n)(attributeName))
      case _       => Right(Aster)
    }
  }

  /**
   * SCHEMA_ATTRIBUTE LParen AttributeDeclaration RParen
   */
  def schemaAttributeTest(node: Node) = {
    val attrDecl = visit(node.getNode(0))(attributeDeclaration)
    SchemaAttributeTest(attrDecl)
  }

  def attributeDeclaration(node: Node) = {
    val name = visit(node.getNode(0))(attributeName)
    AttributeDeclaration(name)
  }

  /**
   *   ELEMENT LParen RParen
   * / ELEMENT LParen ElementNameOrWildcard ( COMMA TypeNameWithQuest )? RParen
   * / ELEMENT LParen ElementNameOrWildcard ( COMMA TypeName )? RParen
   */
  def elementTest(node: Node) = {
    node.size match {
      case 0 => ElementTest_Empty
      case _ =>
        val elementName = visit(node.getNode(0))(elementNameOrWildcard)
        val (typeNameOpt, withQuestionMark) = visit(node.getNode(1)) { x =>
          if (x eq null) (None, false)
          else {
            val tpeName = visit(x.getNode(0))(typeName)
            x.getName match {
              case "TypeNameWithQuest" => (Some(tpeName), true)
              case _                   => (Some(tpeName), false)
            }
          }
        }
        ElementTest_Name(elementName, typeNameOpt, withQuestionMark)
    }
  }

  /**
   *   ElementName
   * / ASTER
   */
  def elementNameOrWildcard(node: Node) = {
    node.get(0) match {
      case n: Node   => Left(visit(n)(elementName))
      case x: String => Right(Aster)
    }
  }

  /**
   * SCHEMA_ELEMENT LParen ElementDeclaration RParen
   */
  def schemaElementTest(node: Node) = {
    val elementDecl = visit(node.getNode(0))(elementDeclaration)
    SchemaElementTest(elementDecl)
  }

  def elementDeclaration(node: Node) = {
    val name = visit(node.getNode(0))(elementName)
    ElementDeclaration(name)
  }

  def attributeName(node: Node) = {
    val name = visit(node.getNode(0))(eqName)
    AttributeName(name)
  }

  def elementName(node: Node) = {
    val name = visit(node.getNode(0))(eqName)
    ElementName(name)
  }

  def simpleTypeName(node: Node) = {
    val name = visit(node.getNode(0))(typeName)
    SimpleTypeName(name)
  }

  def typeName(node: Node) = {
    val name = visit(node.getNode(0))(eqName)
    TypeName(name)
  }

  /**
   *   AnyFunctionTest
   * / TypedFunctionTest
   */
  def functionTest(node: Node) = {
    val n = node.getNode(0)
    n.getName match {
      case "AnyFunctionTest"   => AnyFunctionTest
      case "TypedFunctionTest" => visit(n)(typedFunctionTest)
    }
  }

  /**
   *   FUNCTION LParen RParen AS SequenceType
   * / FUNCTION LParen SequenceType ( COMMA SequenceType )* RParen AS SequenceType
   */
  def typedFunctionTest(node: Node) = {
    node.size match {
      case 1 =>
        val asType = visit(node.getNode(0))(sequenceType)
        TypedFunctionTest(Nil, asType)
      case _ =>
        val paramType0 = visit(node.getNode(0))(sequenceType)
        val paramTypes = visitList(node.getList(1))(sequenceType)
        val asType = visit(node.getNode(2))(sequenceType)
        TypedFunctionTest(paramType0 :: paramTypes, asType)
    }
  }

  /**
   *   AnyMapTest
   * / TypedMapTest
   */
  def mapTest(node: Node): MapTest = {
    val n = node.getNode(0)
    n.getName match {
      case "AnyMapTest"   => AnyMapTest
      case "TypedMapTest" => visit(n)(typedMapTest)
    }
  }

  /**
   * MAP LParen AtomicOrUnionType COMMA SequenceType RParen
   */
  def typedMapTest(node: Node) = {
    val keyType = visit(node.getNode(0))(atomicOrUnionType)
    val valueType = visit(node.getNode(1))(sequenceType)
    TypedMapTest(keyType, valueType)
  }

  /**
   *   AnyArrayTest
   * / TypedArrayTest
   */
  def arrayTest(node: Node): ArrayTest = {
    val n = node.getNode(0)
    n.getName match {
      case "AnyArrayTest"   => AnyArrayTest
      case "TypedArrayTest" => visit(n)(typedArrayTest)
    }
  }

  /**
   * ARRAY LParen SequenceType RParen
   */
  def typedArrayTest(node: Node) = {
    val tpe = visit(node.getNode(0))(sequenceType)
    TypedArrayTest(tpe)
  }

  def parenthesizedItemType(node: Node) = {
    val itemTpe = visit(node.getNode(0))(itemType)
    ParenthesizedItemType(itemTpe)
  }

  /**
   *   QName
   * / URIQualifiedName
   */
  def eqName(node: Node) = {
    val n = node.getNode(0)
    n.getName match {
      case "QName"            => visit(n)(qName)
      case "URIQualifiedName" => visit(n)(uriQualifiedName)
    }
  }

  /**
   * BracedURILiteral NCName ;
   */
  def uriQualifiedName(node: Node) = {
    val uri = visit(node.getNode(0))(bracedURILiteral)
    val local = node.getString(1)
    URIQualifiedName(uri, local)
  }

  def bracedURILiteral(node: Node) = {
    node.getString(0)
  }

  /**
   *   PrefixedName
   * / UnprefixedName
   */
  def qName(node: Node) = {
    val n = node.getNode(0)
    n.getName match {
      case "PrefixedName"   => visit(n)(prefixedName)
      case "UnprefixedName" => visit(n)(unprefixedName)
    }
  }

  def prefixedName(node: Node): EQName = {
    val prefix = node.getString(0)
    val local = node.getString(1)
    PrefixedName(prefix, local)
  }

  def unprefixedName(node: Node): EQName = {
    val name = node.getString(0)
    UnprefixedName(name)
  }

  /**
   * NCName
   */
  def prefix(node: Node) = {
    node.getString(0)
  }

  /**
   * NCName
   */
  def localPart(node: Node) = {
    node.getString(0)
  }
}
