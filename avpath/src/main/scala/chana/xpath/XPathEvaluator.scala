package chana.xpath

import chana.avro
import chana.xpath.nodes._
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord

case class XPathRuntimeException(value: Any, message: String)
  extends RuntimeException(
    value + " " + message + ". " + value + "'s type is: " + (value match {
      case null      => null
      case x: AnyRef => x.getClass.getName
      case _         => "primary type."
    }))

final case class Ctx(schema: Schema, target: Any)

class XPathEvaluator {

  def simpleEval(xpath: Expr, ctxs: List[Ctx]) = {
    expr(xpath.expr, xpath.exprs, ctxs)
  }

  // -------------------------------------------------------------------------

  def paramList(param0: Param, params: List[Param], ctxs: List[Ctx]): List[Any] = {
    param0 :: params map { x => param(x.name, x.typeDecl, ctxs) }
  }

  def param(name: EQName, typeDecl: Option[TypeDeclaration], ctxs: List[Ctx]) = {
    // name
    typeDecl map { x => typeDeclaration(x.asType, ctxs) }
  }

  def functionBody(expr: EnclosedExpr, ctxs: List[Ctx]) = {
    enclosedExpr(expr.expr, ctxs)
  }

  def enclosedExpr(_expr: Expr, ctxs: List[Ctx]): Any = {
    expr(_expr.expr, _expr.exprs, ctxs)
  }

  def expr(expr: ExprSingle, exprs: List[ExprSingle], ctxs: List[Ctx]): List[Any] = {
    expr :: exprs map { exprSingle(_, ctxs) }
  }

  def exprSingle(expr: ExprSingle, ctxs: List[Ctx]) = {
    expr match {
      case ForExpr(forClause, returnExpr)                         => forExpr(forClause, returnExpr, ctxs)
      case LetExpr(letClause, returnExpr)                         => letExpr(letClause, returnExpr, ctxs)
      case QuantifiedExpr(isEvery, varExpr, varExprs, statisExpr) => quantifiedExpr(isEvery, varExpr, varExprs, statisExpr, ctxs)
      case IfExpr(_ifExpr, thenExpr, elseExpr)                    => ifExpr(_ifExpr, thenExpr, elseExpr, ctxs)
      case OrExpr(andExpr, andExprs)                              => orExpr(andExpr, andExprs, ctxs)
    }
  }

  def forExpr(forClause: SimpleForClause, returnExpr: ExprSingle, ctxs: List[Ctx]): Any = {
    simpleForClause(forClause.binding, forClause.bindings, ctxs)
    exprSingle(returnExpr, ctxs)
  }

  def simpleForClause(binding: SimpleForBinding, bindings: List[SimpleForBinding], ctxs: List[Ctx]) = {
    binding :: bindings map { x => simpleForBinding(x.varName, x.inExpr, ctxs) }
  }

  def simpleForBinding(_varName: VarName, inExpr: ExprSingle, ctxs: List[Ctx]) = {
    varName(_varName.eqName, ctxs)
    exprSingle(inExpr, ctxs)
  }

  def letExpr(letClause: SimpleLetClause, returnExpr: ExprSingle, ctxs: List[Ctx]): Any = {
    simpleLetClause(letClause.binding, letClause.bindings, ctxs)
    exprSingle(returnExpr, ctxs)
  }

  def simpleLetClause(binding: SimpleLetBinding, bindings: List[SimpleLetBinding], ctxs: List[Ctx]) = {
    binding :: bindings map { x => simpleLetBinding(x.varName, x.boundTo, ctxs) }
  }

  def simpleLetBinding(_varName: VarName, boundTo: ExprSingle, ctxs: List[Ctx]) = {
    varName(_varName.eqName, ctxs)
    exprSingle(boundTo, ctxs)
  }

  def quantifiedExpr(isEvery: Boolean, varExpr: VarInExprSingle, varExprs: List[VarInExprSingle], statisExpr: ExprSingle, ctxs: List[Ctx]): Any = {
    varExpr :: varExprs map { x => varInExprSingle(x.varName, x.inExpr, ctxs) }
    exprSingle(statisExpr, ctxs)
  }

  def varInExprSingle(_varName: VarName, inExpr: ExprSingle, ctxs: List[Ctx]) = {
    varName(_varName.eqName, ctxs)
    exprSingle(inExpr, ctxs)
  }

  def ifExpr(ifExpr: Expr, thenExpr: ExprSingle, elseExpr: ExprSingle, ctxs: List[Ctx]): Any = {
    expr(ifExpr.expr, ifExpr.exprs, ctxs)
    exprSingle(thenExpr, ctxs)
    exprSingle(elseExpr, ctxs)
  }

  def orExpr(_andExpr: AndExpr, andExprs: List[AndExpr], ctxs: List[Ctx]) = {
    val value0 = andExpr(_andExpr.compExpr, _andExpr.compExprs, ctxs)
    val values = andExprs map { x => andExpr(x.compExpr, x.compExprs, ctxs) }
    values.foldLeft(value0) { (acc, x) => XPathFunctions.or(acc, x) }
  }

  def andExpr(compExpr: ComparisonExpr, compExprs: List[ComparisonExpr], ctxs: List[Ctx]) = {
    val value0 = comparisonExpr(compExpr.concExpr, compExpr.compExprPostfix, ctxs)
    val values = compExprs map { x => comparisonExpr(x.concExpr, x.compExprPostfix, ctxs) }
    values.foldLeft(value0) { (acc, x) => XPathFunctions.and(acc, x) }
  }

  def comparisonExpr(concExpr: StringConcatExpr, compExprPostfix: Option[ComparisonExprPostfix], ctxs: List[Ctx]) = {
    val value0 = stringConcatExpr(concExpr.rangeExpr, concExpr.rangeExprs, ctxs)
    compExprPostfix match {
      case None => value0
      case Some(ComparisonExprPostfix(compOp, concExpr)) =>
        val right = comparisonExprPostfix(compOp, concExpr, ctxs)
        compOp match {
          case GeneralComp(op) =>
            op match {
              case "="  => XPathFunctions.eq(value0, right)
              case "!=" => XPathFunctions.ne(value0, right)
              case "<=" => XPathFunctions.le(value0, right)
              case "<"  => XPathFunctions.lt(value0, right)
              case ">=" => XPathFunctions.ge(value0, right)
              case ">"  => XPathFunctions.gt(value0, right)
            }
          case ValueComp(op) =>
            op match {
              case "eq" => XPathFunctions.eq(value0, right)
              case "ne" => XPathFunctions.ne(value0, right)
              case "le" => XPathFunctions.le(value0, right)
              case "lt" => XPathFunctions.lt(value0, right)
              case "ge" => XPathFunctions.ge(value0, right)
              case "gt" => XPathFunctions.gt(value0, right)
            }
          case NodeComp(op) => value0 // TODO
        }
    }
  }

  /**
   * generalcomp: "=", "!=", "<=", "<", ">=", ">"
   * valuecomp: "eq", "ne", "lt", "le", "gt", "ge"
   * nodecomp: "is", "<<", ">>"
   */
  def comparisonExprPostfix(compOp: CompOperator, concExpr: StringConcatExpr, ctxs: List[Ctx]) = {
    stringConcatExpr(concExpr.rangeExpr, concExpr.rangeExprs, ctxs)
  }

  def stringConcatExpr(_rangeExpr: RangeExpr, rangeExprs: List[RangeExpr], ctxs: List[Ctx]) = {
    val res = _rangeExpr :: rangeExprs map { x => rangeExpr(x.addExpr, x.toExpr, ctxs) }
    res.head // TODO
  }

  def rangeExpr(addExpr: AdditiveExpr, toExpr: Option[AdditiveExpr], ctxs: List[Ctx]) = {
    val value = additiveExpr(addExpr.multiExpr, addExpr.prefixedMultiExprs, ctxs)
    toExpr map { x => additiveExpr(x.multiExpr, x.prefixedMultiExprs, ctxs) }
    value
  }

  def additiveExpr(multiExpr: MultiplicativeExpr, prefixedMultiExprs: List[MultiplicativeExpr], ctxs: List[Ctx]) = {
    val v0 = multiplicativeExpr(multiExpr.prefix, multiExpr.unionExpr, multiExpr.prefixedUnionExprs, ctxs)
    val value0 = multiExpr.prefix match {
      case Nop | Plus => v0
      case Minus      => XPathFunctions.neg(v0)
    }

    val values = prefixedMultiExprs map { x => (x.prefix, multiplicativeExpr(x.prefix, x.unionExpr, x.prefixedUnionExprs, ctxs)) }
    values.foldLeft(value0) {
      case (acc, (Plus, x))  => XPathFunctions.plus(acc, x)
      case (acc, (Minus, x)) => XPathFunctions.minus(acc, x)
      case _                 => value0
    }
  }

  /**
   * prefix is "", or "+", "-"
   */
  def multiplicativeExpr(prefix: Prefix, _unionExpr: UnionExpr, prefixedUnionExprs: List[UnionExpr], ctxs: List[Ctx]) = {
    val value0 = unionExpr(_unionExpr.prefix, _unionExpr.intersectExceptExpr, _unionExpr.prefixedIntersectExceptExprs, ctxs)

    val values = prefixedUnionExprs map { x => (x.prefix, unionExpr(x.prefix, x.intersectExceptExpr, x.prefixedIntersectExceptExprs, ctxs)) }
    values.foldLeft(value0) {
      case (acc, (Aster, x)) => XPathFunctions.multiply(acc, x)
      case (acc, (Div, x))   => XPathFunctions.divide(acc, x)
      case (acc, (IDiv, x))  => XPathFunctions.idivide(acc, x)
      case (acc, (Mod, x))   => XPathFunctions.mod(acc, x)
      case _                 => value0
    }
  }

  /**
   * prefix is "", or "*", "div", "idiv", "mod"
   */
  def unionExpr(prefix: Prefix, _intersectExceptExpr: IntersectExceptExpr, prefixedIntersectExceptExprs: List[IntersectExceptExpr], ctxs: List[Ctx]) = {
    val value0 = intersectExceptExpr(_intersectExceptExpr.prefix, _intersectExceptExpr.instanceOfExpr, _intersectExceptExpr.prefixedInstanceOfExprs, ctxs)

    val values = prefixedIntersectExceptExprs map { x => (x.prefix, intersectExceptExpr(x.prefix, x.instanceOfExpr, x.prefixedInstanceOfExprs, ctxs)) }
    values.foldLeft(value0) {
      case (acc, (Union, x)) => value0 // TODO
      case (acc, (Pipe, x))  => value0 // TODO
      case _                 => value0
    }
  }

  /**
   * prefix is "", or "union", "|"
   */
  def intersectExceptExpr(prefix: Prefix, _instanceOfExpr: InstanceofExpr, prefixedInstanceOfExprs: List[InstanceofExpr], ctxs: List[Ctx]) = {
    val value0 = instanceofExpr(_instanceOfExpr.prefix, _instanceOfExpr.treatExpr, _instanceOfExpr.ofType, ctxs)

    val values = prefixedInstanceOfExprs map { x => (x.prefix, instanceofExpr(x.prefix, x.treatExpr, x.ofType, ctxs)) }
    values.foldLeft(value0) {
      case (acc, (Intersect, x)) => value0 // TODO
      case (acc, (Except, x))    => value0 // TODO
      case _                     => value0
    }
  }

  /**
   * prefix is "", or "intersect", "except"
   */
  def instanceofExpr(prefix: Prefix, _treatExpr: TreatExpr, ofType: Option[SequenceType], ctxs: List[Ctx]) = {
    val value = treatExpr(_treatExpr.castableExpr, _treatExpr.asType, ctxs)

    ofType match {
      case Some(x) =>
        val tpe = sequenceType(x, ctxs)
        value // TODO
      case None => value
    }
  }

  def treatExpr(_castableExpr: CastableExpr, asType: Option[SequenceType], ctxs: List[Ctx]) = {
    val value = castableExpr(_castableExpr.castExpr, _castableExpr.asType, ctxs)

    asType match {
      case Some(x) =>
        val tpe = sequenceType(x, ctxs)
        value // TODO
      case None => value
    }
  }

  def castableExpr(_castExpr: CastExpr, asType: Option[SingleType], ctxs: List[Ctx]) = {
    val value = castExpr(_castExpr.unaryExpr, _castExpr.asType, ctxs)

    asType match {
      case Some(x) =>
        val tpe = singleType(x.name, x.withQuestionMark, ctxs)
        value // TODO
      case None => value
    }
  }

  def castExpr(_unaryExpr: UnaryExpr, asType: Option[SingleType], ctxs: List[Ctx]) = {
    val v0 = unaryExpr(_unaryExpr.prefix, _unaryExpr.valueExpr, ctxs)
    val value = _unaryExpr.prefix match {
      case Nop | Plus => v0
      case Minus      => XPathFunctions.neg(v0)
    }

    asType match {
      case Some(x) =>
        val tpe = singleType(x.name, x.withQuestionMark, ctxs)
        value // TODO
      case None => value
    }
  }

  /**
   * prefix is "", or "-", "+"
   */
  def unaryExpr(prefix: Prefix, _valueExpr: ValueExpr, ctxs: List[Ctx]) = {
    valueExpr(_valueExpr.simpleMapExpr, ctxs)
  }

  /**
   * TODO, fetch value here or later?
   */
  def valueExpr(_simpleMapExpr: SimpleMapExpr, ctxs: List[Ctx]) = {
    val newCtxs = simpleMapExpr(_simpleMapExpr.pathExpr, _simpleMapExpr.exclamExprs, ctxs)
    newCtxs.head.target
  }

  def simpleMapExpr(_pathExpr: PathExpr, exclamExprs: List[PathExpr], ctxs: List[Ctx]) = {
    val path = pathExpr(_pathExpr.prefix, _pathExpr.relativeExpr, ctxs)
    exclamExprs map { x => pathExpr(x.prefix, x.relativeExpr, ctxs) }
    path
  }

  /**
   * prefix is "" or "//", "/"
   *
   *   "//" RelativePathExpr
   * / "/"  RelativePathExpr?
   * / RelativePathExpr
   */
  def pathExpr(prefix: Prefix, relativeExpr: Option[RelativePathExpr], ctxs: List[Ctx]): List[Ctx] = {
    prefix match {
      case Nop      =>
      case Absolute =>
      case Relative =>
    }
    relativeExpr match {
      case Some(x) => relativePathExpr(x.stepExpr, x.prefixedStepExprs, ctxs)
      case None    => ctxs
    }
  }

  def relativePathExpr(_stepExpr: StepExpr, prefixedStepExprs: List[StepExpr], ctxs: List[Ctx]): List[Ctx] = {
    val path0 = stepExpr(_stepExpr.prefix, _stepExpr.expr, ctxs)
    prefixedStepExprs.foldLeft(path0) { (acc, x) => stepExpr(x.prefix, x.expr, acc) ::: acc }
  }

  /**
   * prefix is "" or "/" or "//"
   */
  def stepExpr(prefix: Prefix, expr: Either[PostfixExpr, AxisStep], ctxs: List[Ctx]): List[Ctx] = {
    prefix match {
      case Nop      =>
      case Absolute =>
      case Relative =>
    }
    expr match {
      case Left(PostfixExpr(expr, postfixes)) =>
        postfixExpr(expr, postfixes, ctxs)
      case Right(ReverseAxisStep(step, predicates)) =>
        reverseAxisStep(step, predicates, ctxs)
      case Right(ForwardAxisStep(step, predicates)) =>
        forwardAxisStep(step, predicates, ctxs)
    }
  }

  def reverseAxisStep(step: ReverseStep, predicates: PredicateList, ctxs: List[Ctx]) = {
    val preds = predicateList(predicates.predicates, ctxs)
    step match {
      case ReverseStep_Axis(axis, _nodeTest) =>
        axis match {
          case Parent           =>
          case Ancestor         =>
          case PrecedingSibling =>
          case Preceding        =>
          case AncestorOrSelf   =>
        }
        nodeTest(_nodeTest, ctxs)
        ctxs
      case AbbrevReverseStep => ctxs
    }
  }

  def forwardAxisStep(step: ForwardStep, predicates: PredicateList, ctxs: List[Ctx]): List[Ctx] = {
    step match {
      case AbbrevForwardStep(_nodeTest, withAtMark) =>
        val res = nodeTest(_nodeTest, ctxs)
        val newCtxs = internal_ctxOfNodeTest(res, withAtMark, ctxs)
        val preds = predicateList(predicates.predicates, newCtxs)
        //println("preds: ", preds)
        internal_filterByPredicateList(preds, newCtxs.head) :: newCtxs.tail

      case ForwardStep_Axis(axis, _nodeTest) =>
        axis match {
          case Child            =>
          case Descendant       =>
          case Attribute        =>
          case Self             =>
          case DescendantOrSelf =>
          case FollowingSibling =>
          case Following        =>
          case Namespace        =>
        }
        nodeTest(_nodeTest, ctxs)
        ctxs
    }
  }

  def internal_ctxOfNodeTest(testResult: Any, withAtMark: Boolean, ctxs: List[Ctx]) = {
    testResult match {
      case URIQualifiedName(uri, name) => ctxs
      case PrefixedName(prefix, local) => ctxs
      case UnprefixedName(local) =>
        ctxs.head match {
          case Ctx(schema, target) =>
            //println("target: ", target)
            val (newSchema, newTarget) = target match {
              case rec: IndexedRecord =>
                val field = schema.getField(local)
                val fieldSchema = avro.getNonNull(field.schema)
                (fieldSchema, rec.get(field.pos))

              case map: java.util.Map[String, Any] @unchecked =>
                if (withAtMark) { // ok we're fetching a map value via key
                  val valueSchema = avro.getValueType(schema)
                  (valueSchema, map.get(local))
                } else {
                  throw new XPathRuntimeException(map, "try to get value from a non @key: " + local)
                }

              case arr: java.util.Collection[Any] @unchecked =>
                var elems = List[Any]()
                val elemSchema = avro.getElementType(schema)
                val field = elemSchema.getField(local)
                val fieldSchema = avro.getNonNull(field.schema)
                val itr = arr.iterator
                while (itr.hasNext) {
                  val elem = itr.next.asInstanceOf[IndexedRecord]
                  elems ::= elem.get(field.pos)
                }
                (fieldSchema, elems.reverse)

              case xs: List[IndexedRecord] @unchecked =>
                //println("local: " + local + ", schema: " + schema)
                val field = schema.getField(local)
                val fieldSchema = avro.getNonNull(field.schema)

                val elems = xs map (_.get(field.pos))
                (fieldSchema, elems)
            }

            Ctx(newSchema, newTarget) :: ctxs

          case _ => ctxs
        }

      case x: Wildcard =>
        x match {
          case Aster =>
            ctxs.head match {
              case Ctx(schema, target) =>
                //println("target: ", target)
                val (newSchema, newTarget) = target match {
                  case map: java.util.Map[String, Any] @unchecked =>
                    var elems = List()
                    // ok we're fetching all map values
                    val valueSchema = avro.getValueType(schema)
                    (Schema.createArray(valueSchema), map.values)

                  case _ => // TODO
                    (schema, target)
                }

                Ctx(newSchema, newTarget) :: ctxs
            }
          case NameAster(name) => ctxs // TODO
          case AsterName(name) => ctxs // TODO
          case UriAster(uri)   => ctxs // TODO
        }

      case _ =>
        //println("NodeTest res: " + testResult)
        ctxs

    }
  }

  def internal_filterByPredicateList(preds: List[Any], ctx: Ctx): Ctx = {
    //println("ctx: " + ctx + " in predicates: " + preds)
    if (preds.nonEmpty) {
      ctx.schema.getType match {
        case Schema.Type.ARRAY =>
          val arr = ctx.target.asInstanceOf[java.util.Collection[Any]].iterator
          val elemSchema = avro.getElementType(ctx.schema)
          preds.head match {
            case List(x: Number) =>
              var elems = List[Any]()
              val idx = x.intValue
              var i = 1
              while (arr.hasNext && i <= idx) {
                if (i == idx) {
                  elems ::= arr.next
                } else {
                  arr.next
                }
                i += 1
              }

              Ctx(elemSchema, elems)

            case List(xs: List[Boolean] @unchecked) =>
              var elems = List[Any]()
              val conds = xs.iterator
              while (arr.hasNext && conds.hasNext) {
                if (conds.next) {
                  elems ::= arr.next
                } else {
                  arr.next
                }
              }

              Ctx(elemSchema, elems.reverse)

            case _ =>
              ctx // TODO
          }

        case Schema.Type.MAP => // TODO
          val map = ctx.target.asInstanceOf[java.util.Map[String, Any]]
          val valueSchema = avro.getValueType(ctx.schema)
          preds.head match {
            case List(xs: List[Boolean] @unchecked) =>
              var elems = List[Any]()
              val conds = xs.iterator
              //              while (arr.hasNext && conds.hasNext) {
              //                if (conds.next) {
              //                  elems ::= arr.next
              //                } else {
              //                  arr.next
              //                }
              //              }

              //Ctx(valueSchema, elems.reverse)
              ctx // TODO

            case _ =>
              ctx // TODO
          }

        case x =>
          println("schema type: " + x + ", value: " + ctx.target)
          preds.head match {
            case List(x: Boolean) => if (x) ctx else Ctx(ctx.schema, ())
            case _                => ctx
          }
      }
    } else ctx
  }

  def nodeTest(test: NodeTest, ctxs: List[Ctx]) = {
    test match {
      case x: NameTest => nameTest(x, ctxs)
      case x: KindTest => kindTest(x, ctxs)
    }
  }

  def nameTest(test: NameTest, ctxs: List[Ctx]) = {
    test match {
      case NameTest_Name(name)         => name
      case NameTest_Wildcard(wildcard) => wildcard
    }
  }

  def kindTest(test: KindTest, ctxs: List[Ctx]) = {
    test match {
      case AnyKindTest =>
      case DocumentTest(elemTest) => documentTest(elemTest, ctxs)
      case TextTest =>
      case CommentTest =>
      case NamespaceNodeTest =>
      case PITest(name) => piTest(name, ctxs)
      case AttributeTest_Empty =>
      case AttributeTest_Name(name, typeName) => attributeTest_Name(name, typeName, ctxs)
      case SchemaAttributeTest(attrDecl) => schemaAttributeTest(attrDecl, ctxs)
      case ElementTest_Empty =>
      case ElementTest_Name(name, typeName, withQuestionMark) => elementTest_Name(name, typeName, withQuestionMark, ctxs)
      case SchemaElementTest(elemDecl) => schemaElementTest(elemDecl, ctxs)
    }
  }

  def postfixExpr(expr: PrimaryExpr, postfixes: List[PostFix], ctxs: List[Ctx]): List[Ctx] = {
    val value = primaryExpr(expr, ctxs)
    postfixes map { x => postfix(x, ctxs) }
    Ctx(null, value) :: ctxs
  }

  def postfix(post: PostFix, ctxs: List[Ctx]) = {
    post match {
      case Postfix_Predicate(predicate)       => postfix_Predicate(predicate, ctxs)
      case Postfix_Arguments(args)            => postfix_Arguments(args, ctxs)
      case Postfix_Lookup(lookup)             => postfix_Lookup(lookup, ctxs)
      case Postfix_ArrowPostfix(arrowPostfix) => postfix_ArrowPostfix(arrowPostfix, ctxs)
    }
  }

  def postfix_Predicate(_predicate: Predicate, ctxs: List[Ctx]) = {
    predicate(_predicate.expr, ctxs)
  }

  def postfix_Arguments(args: ArgumentList, ctxs: List[Ctx]) = {
    argumentList(args.args, ctxs)
  }

  def postfix_Lookup(_lookup: Lookup, ctxs: List[Ctx]) = {
    lookup(_lookup.keySpecifier, ctxs)
  }

  def postfix_ArrowPostfix(_arrowPostfix: ArrowPostfix, ctxs: List[Ctx]) = {
    arrowPostfix(_arrowPostfix.arrowFunction, _arrowPostfix.args, ctxs)
  }

  def argumentList(args: List[Argument], ctxs: List[Ctx]): Any = {
    args map {
      case Argument_ExprSingle(expr) => exprSingle(expr, ctxs)
      case ArgumentPlaceholder       =>
    }
  }

  def predicateList(predicates: List[Predicate], ctxs: List[Ctx]) = {
    predicates map { x => predicate(x.expr, ctxs) }
  }

  def predicate(_expr: Expr, ctxs: List[Ctx]): Any = {
    expr(_expr.expr, _expr.exprs, ctxs)
  }

  def lookup(_keySpecifier: KeySpecifier, ctxs: List[Ctx]) = {
    keySpecifier(_keySpecifier, ctxs)
  }

  def keySpecifier(specifier: KeySpecifier, ctxs: List[Ctx]) = {
    specifier match {
      case KeySpecifier_NCName(ncName)          =>
      case KeySpecifier_IntegerLiteral(x)       => x
      case KeySpecifier_ParenthesizedExpr(expr) =>
      case KeySpecifier_ASTER                   =>
    }
  }

  def arrowPostfix(arrowFunction: ArrowFunctionSpecifier, args: ArgumentList, ctxs: List[Ctx]) = {
    arrowFunction match {
      case ArrowFunctionSpecifier_EQName(name)            =>
      case ArrowFunctionSpecifier_VarRef(varRef)          =>
      case ArrowFunctionSpecifier_ParenthesizedExpr(expr) => parenthesizedExpr(expr.expr, ctxs)
    }
  }

  def primaryExpr(expr: PrimaryExpr, ctxs: List[Ctx]) = {
    expr match {
      case PrimaryExpr_Literal(_literal) =>
        literal(_literal, ctxs)

      case PrimaryExpr_VarRef(ref) =>
        varRef(ref.varName, ctxs)

      case PrimaryExpr_ParenthesizedExpr(expr) =>
        parenthesizedExpr(expr.expr, ctxs)

      case PrimaryExpr_ContextItemExpr => // TODO

      case PrimaryExpr_FunctionCall(call) =>
        functionCall(call.name, call.args, ctxs)

      case PrimaryExpr_FunctionItemExpr(item) =>
        item match {
          case NamedFunctionRef(name, index) =>
            namedFunctionRef(name, index, ctxs)
          case InlineFunctionExpr(params, asType, functionBody) =>
            inlineFunctionExpr(params, asType, functionBody, ctxs)
        }

      case PrimaryExpr_MapConstructor(constructor) =>
        mapConstructor(constructor.entrys, ctxs)

      case PrimaryExpr_ArrayConstructor(constructor) =>
        arrayConstructor(constructor, ctxs)

      case PrimaryExpr_UnaryLookup(lookup) =>
        unaryLookup(lookup.key, ctxs)
    }
  }

  /**
   * Force Any to AnyRef to avoid AnyVal be boxed/unboxed again and again
   */
  def literal(v: Literal, ctxs: List[Ctx]): AnyRef = {
    v.x.asInstanceOf[AnyRef]
  }

  def varRef(_varName: VarName, ctxs: List[Ctx]) = {
    varName(_varName.eqName, ctxs)
  }

  def varName(eqName: EQName, ctxs: List[Ctx]): EQName = {
    eqName
  }

  def parenthesizedExpr(_expr: Option[Expr], ctxs: List[Ctx]): Any = {
    _expr map { x => expr(x.expr, x.exprs, ctxs) }
  }

  def functionCall(name: EQName, _args: ArgumentList, ctxs: List[Ctx]) = {
    val fnName = name match {
      case UnprefixedName(x)           => x
      case PrefixedName(prefix, local) => local
      case URIQualifiedName(uri, name) => name
    }
    val args = argumentList(_args.args, ctxs)

    //println("target: ", ctxs.head.target)
    ctxs.head.target match {
      case arr: java.util.Collection[Any] @unchecked =>
        fnName match {
          case "last"     => XPathFunctions.last(arr)
          case "position" => XPathFunctions.position(arr)
        }

      case _ => ()
    }
  }

  def namedFunctionRef(name: EQName, index: Int, ctxs: List[Ctx]) = {
    // TODO
  }

  def inlineFunctionExpr(params: Option[ParamList], asType: Option[SequenceType], _functionBody: FunctionBody, ctxs: List[Ctx]) = {
    params match {
      case Some(x) => paramList(x.param, x.params, ctxs)
      case None    => Nil
    }
    asType map { sequenceType(_, ctxs) }
    functionBody(_functionBody.expr, ctxs)
  }

  def mapConstructor(entrys: List[MapConstructorEntry], ctxs: List[Ctx]) = {
    entrys map { x => mapConstructorEntry(x.key, x.value, ctxs) }
  }

  def mapConstructorEntry(key: MapKeyExpr, value: MapValueExpr, ctxs: List[Ctx]): Any = {
    exprSingle(key.expr, ctxs)
    exprSingle(value.expr, ctxs)
  }

  def arrayConstructor(constructor: ArrayConstructor, ctxs: List[Ctx]): Any = {
    constructor match {
      case SquareArrayConstructor(exprs) =>
        exprs map { x => exprSingle(x, ctxs) }
      case BraceArrayConstructor(_expr) =>
        _expr match {
          case Some(Expr(expr0, exprs)) => expr(expr0, exprs, ctxs)
          case None                     =>
        }
    }
  }

  def unaryLookup(key: KeySpecifier, ctxs: List[Ctx]) = {
    keySpecifier(key, ctxs)
  }

  def singleType(name: SimpleTypeName, withQuestionMark: Boolean, ctxs: List[Ctx]) = {
    simpleTypeName(name.name, ctxs)
  }

  def typeDeclaration(asType: SequenceType, ctxs: List[Ctx]) = {
    sequenceType(asType, ctxs)
  }

  def sequenceType(tpe: SequenceType, ctxs: List[Ctx]) = {
    tpe match {
      case SequenceType_Empty =>
      case SequenceType_ItemType(_itemType, occurrence) =>
        itemType(_itemType, ctxs)
        occurrence match {
          case None                            =>
          case Some(OccurrenceIndicator_QUEST) =>
          case Some(OccurrenceIndicator_ASTER) =>
          case Some(OccurrenceIndicator_PLUS)  =>
        }
    }
  }

  def itemType(tpe: ItemType, ctxs: List[Ctx]): Any = {
    tpe match {
      case ItemType_ITEM                    =>
      case AtomicOrUnionType(eqName)        => eqName
      case x: KindTest                      => kindTest(x, ctxs)
      case x: FunctionTest                  => functionTest(x, ctxs)
      case x: MapTest                       => mapTest(x, ctxs)
      case x: ArrayTest                     => arrayTest(x, ctxs)
      case ParenthesizedItemType(_itemType) => itemType(_itemType, ctxs)
    }
  }

  /**
   * elemTest should be either ElementTest or SchemaElementTest
   */
  def documentTest(elemTest: Option[KindTest], ctxs: List[Ctx]): Any = {
    elemTest map { x => kindTest(x, ctxs) }
  }

  def piTest(name: Option[String], ctxs: List[Ctx]) = {
    name match {
      case Some(x) =>
      case None    =>
    }
  }

  def attributeTest_Name(name: AttribNameOrWildcard, typeName: Option[TypeName], ctxs: List[Ctx]) = {
    // TODO optional type name
    name match {
      case Left(x)  => attributeName(x.name, ctxs)
      case Right(x) => // Aster - QName
    }
  }

  def schemaAttributeTest(attrDecl: AttributeDeclaration, ctxs: List[Ctx]) = {
    attributeDeclaration(attrDecl.name, ctxs)
  }

  def attributeDeclaration(name: AttributeName, ctxs: List[Ctx]) = {
    attributeName(name.name, ctxs)
  }

  def elementTest_Name(name: ElementNameOrWildcard, typeName: Option[TypeName], withQuestionMark: Boolean, ctxs: List[Ctx]) = {
    name match {
      case Left(x)  => elementName(x.name, ctxs)
      case Right(x) => // Aster - QName
    }
  }

  def schemaElementTest(elemDecl: ElementDeclaration, ctxs: List[Ctx]) = {
    elementDeclaration(elemDecl.name, ctxs)
  }

  def elementDeclaration(name: ElementName, ctxs: List[Ctx]) = {
    elementName(name.name, ctxs)
  }

  def attributeName(name: EQName, ctxs: List[Ctx]): EQName = {
    name
  }

  def elementName(name: EQName, ctxs: List[Ctx]): EQName = {
    name
  }

  def simpleTypeName(name: TypeName, ctxs: List[Ctx]): EQName = {
    typeName(name.name, ctxs)
  }

  def typeName(name: EQName, ctxs: List[Ctx]): EQName = {
    name
  }

  def functionTest(test: FunctionTest, ctxs: List[Ctx]) = {
    test match {
      case AnyFunctionTest                  =>
      case TypedFunctionTest(types, asType) => typedFunctionTest(types, asType, ctxs)
    }
  }

  def typedFunctionTest(types: List[SequenceType], asType: SequenceType, ctxs: List[Ctx]): Any = {
    types map { sequenceType(_, ctxs) }
    sequenceType(asType, ctxs)
  }

  def mapTest(test: MapTest, ctxs: List[Ctx]) = {
    test match {
      case AnyMapTest                =>
      case TypedMapTest(tpe, asType) => typedMapTest(tpe, asType, ctxs)
    }
  }

  def typedMapTest(tpe: AtomicOrUnionType, asType: SequenceType, ctxs: List[Ctx]) = {
    //tpe.eqName
    sequenceType(asType, ctxs)
  }

  def arrayTest(test: ArrayTest, ctxs: List[Ctx]) = {
    test match {
      case AnyArrayTest        =>
      case TypedArrayTest(tpe) => typedArrayTest(tpe, ctxs)
    }
  }

  def typedArrayTest(tpe: SequenceType, ctxs: List[Ctx]) = {
    sequenceType(tpe, ctxs)
  }

}
