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

  def simpleEval(xpath: Expr, ctx: Ctx) = {
    expr(xpath.expr, xpath.exprs, ctx)
  }

  // -------------------------------------------------------------------------

  def paramList(param0: Param, params: List[Param], ctx: Ctx): List[Any] = {
    param0 :: params map { x => param(x.name, x.typeDecl, ctx) }
  }

  def param(name: EQName, typeDecl: Option[TypeDeclaration], ctx: Ctx) = {
    // name
    typeDecl map { x => typeDeclaration(x.asType, ctx) }
  }

  def functionBody(expr: EnclosedExpr, ctx: Ctx) = {
    enclosedExpr(expr.expr, ctx)
  }

  def enclosedExpr(_expr: Expr, ctx: Ctx): Any = {
    expr(_expr.expr, _expr.exprs, ctx)
  }

  // TODO should we support multiple exprSingles? which brings List values
  def expr(expr: ExprSingle, exprs: List[ExprSingle], ctx: Ctx): List[Any] = {
    expr :: exprs map { exprSingle(_, ctx) }
  }

  def exprSingle(expr: ExprSingle, ctx: Ctx): Any = {
    expr match {
      case ForExpr(forClause, returnExpr)                         => forExpr(forClause, returnExpr, ctx)
      case LetExpr(letClause, returnExpr)                         => letExpr(letClause, returnExpr, ctx)
      case QuantifiedExpr(isEvery, varExpr, varExprs, statisExpr) => quantifiedExpr(isEvery, varExpr, varExprs, statisExpr, ctx)
      case IfExpr(_ifExpr, thenExpr, elseExpr)                    => ifExpr(_ifExpr, thenExpr, elseExpr, ctx)
      case OrExpr(andExpr, andExprs)                              => orExpr(andExpr, andExprs, ctx)
    }
  }

  def forExpr(forClause: SimpleForClause, returnExpr: ExprSingle, ctx: Ctx): Any = {
    simpleForClause(forClause.binding, forClause.bindings, ctx)
    exprSingle(returnExpr, ctx)
  }

  def simpleForClause(binding: SimpleForBinding, bindings: List[SimpleForBinding], ctx: Ctx) = {
    binding :: bindings map { x => simpleForBinding(x.varName, x.inExpr, ctx) }
  }

  def simpleForBinding(_varName: VarName, inExpr: ExprSingle, ctx: Ctx) = {
    varName(_varName.eqName, ctx)
    exprSingle(inExpr, ctx)
  }

  def letExpr(letClause: SimpleLetClause, returnExpr: ExprSingle, ctx: Ctx): Any = {
    simpleLetClause(letClause.binding, letClause.bindings, ctx)
    exprSingle(returnExpr, ctx)
  }

  def simpleLetClause(binding: SimpleLetBinding, bindings: List[SimpleLetBinding], ctx: Ctx) = {
    binding :: bindings map { x => simpleLetBinding(x.varName, x.boundTo, ctx) }
  }

  def simpleLetBinding(_varName: VarName, boundTo: ExprSingle, ctx: Ctx) = {
    varName(_varName.eqName, ctx)
    exprSingle(boundTo, ctx)
  }

  def quantifiedExpr(isEvery: Boolean, varExpr: VarInExprSingle, varExprs: List[VarInExprSingle], statisExpr: ExprSingle, ctx: Ctx): Any = {
    varExpr :: varExprs map { x => varInExprSingle(x.varName, x.inExpr, ctx) }
    exprSingle(statisExpr, ctx)
  }

  def varInExprSingle(_varName: VarName, inExpr: ExprSingle, ctx: Ctx) = {
    varName(_varName.eqName, ctx)
    exprSingle(inExpr, ctx)
  }

  def ifExpr(ifExpr: Expr, thenExpr: ExprSingle, elseExpr: ExprSingle, ctx: Ctx): Any = {
    expr(ifExpr.expr, ifExpr.exprs, ctx)
    exprSingle(thenExpr, ctx)
    exprSingle(elseExpr, ctx)
  }

  def orExpr(_andExpr: AndExpr, andExprs: List[AndExpr], ctx: Ctx) = {
    val value0 = andExpr(_andExpr.compExpr, _andExpr.compExprs, ctx)
    val values = andExprs map { x => andExpr(x.compExpr, x.compExprs, ctx) }
    values.foldLeft(value0) { (acc, x) => XPathFunctions.or(acc, x) }
  }

  def andExpr(compExpr: ComparisonExpr, compExprs: List[ComparisonExpr], ctx: Ctx) = {
    val value0 = comparisonExpr(compExpr.concExpr, compExpr.compExprPostfix, ctx)
    val values = compExprs map { x => comparisonExpr(x.concExpr, x.compExprPostfix, ctx) }
    values.foldLeft(value0) { (acc, x) => XPathFunctions.and(acc, x) }
  }

  def comparisonExpr(concExpr: StringConcatExpr, compExprPostfix: Option[ComparisonExprPostfix], ctx: Ctx) = {
    val value0 = stringConcatExpr(concExpr.rangeExpr, concExpr.rangeExprs, ctx)
    compExprPostfix match {
      case None => value0
      case Some(ComparisonExprPostfix(compOp, concExpr)) =>
        val right = comparisonExprPostfix(compOp, concExpr, ctx)
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
  def comparisonExprPostfix(compOp: CompOperator, concExpr: StringConcatExpr, ctx: Ctx) = {
    stringConcatExpr(concExpr.rangeExpr, concExpr.rangeExprs, ctx)
  }

  def stringConcatExpr(_rangeExpr: RangeExpr, rangeExprs: List[RangeExpr], ctx: Ctx) = {
    val res = _rangeExpr :: rangeExprs map { x => rangeExpr(x.addExpr, x.toExpr, ctx) }
    res.head // TODO
  }

  def rangeExpr(addExpr: AdditiveExpr, toExpr: Option[AdditiveExpr], ctx: Ctx) = {
    val value = additiveExpr(addExpr.multiExpr, addExpr.prefixedMultiExprs, ctx)
    toExpr map { x => additiveExpr(x.multiExpr, x.prefixedMultiExprs, ctx) }
    value
  }

  def additiveExpr(multiExpr: MultiplicativeExpr, prefixedMultiExprs: List[MultiplicativeExpr], ctx: Ctx) = {
    val v0 = multiplicativeExpr(multiExpr.prefix, multiExpr.unionExpr, multiExpr.prefixedUnionExprs, ctx)
    val value0 = multiExpr.prefix match {
      case Nop | Plus => v0
      case Minus      => XPathFunctions.neg(v0)
    }

    val values = prefixedMultiExprs map { x => (x.prefix, multiplicativeExpr(x.prefix, x.unionExpr, x.prefixedUnionExprs, ctx)) }
    values.foldLeft(value0) {
      case (acc, (Plus, x))  => XPathFunctions.plus(acc, x)
      case (acc, (Minus, x)) => XPathFunctions.minus(acc, x)
      case _                 => value0
    }
  }

  /**
   * prefix is "", or "+", "-"
   */
  def multiplicativeExpr(prefix: Prefix, _unionExpr: UnionExpr, prefixedUnionExprs: List[UnionExpr], ctx: Ctx) = {
    val value0 = unionExpr(_unionExpr.prefix, _unionExpr.intersectExceptExpr, _unionExpr.prefixedIntersectExceptExprs, ctx)

    val values = prefixedUnionExprs map { x => (x.prefix, unionExpr(x.prefix, x.intersectExceptExpr, x.prefixedIntersectExceptExprs, ctx)) }
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
  def unionExpr(prefix: Prefix, _intersectExceptExpr: IntersectExceptExpr, prefixedIntersectExceptExprs: List[IntersectExceptExpr], ctx: Ctx) = {
    val value0 = intersectExceptExpr(_intersectExceptExpr.prefix, _intersectExceptExpr.instanceOfExpr, _intersectExceptExpr.prefixedInstanceOfExprs, ctx)

    val values = prefixedIntersectExceptExprs map { x => (x.prefix, intersectExceptExpr(x.prefix, x.instanceOfExpr, x.prefixedInstanceOfExprs, ctx)) }
    values.foldLeft(value0) {
      case (acc, (Union, x)) => value0 // TODO
      case (acc, (Pipe, x))  => value0 // TODO
      case _                 => value0
    }
  }

  /**
   * prefix is "", or "union", "|"
   */
  def intersectExceptExpr(prefix: Prefix, _instanceOfExpr: InstanceofExpr, prefixedInstanceOfExprs: List[InstanceofExpr], ctx: Ctx) = {
    val value0 = instanceofExpr(_instanceOfExpr.prefix, _instanceOfExpr.treatExpr, _instanceOfExpr.ofType, ctx)

    val values = prefixedInstanceOfExprs map { x => (x.prefix, instanceofExpr(x.prefix, x.treatExpr, x.ofType, ctx)) }
    values.foldLeft(value0) {
      case (acc, (Intersect, x)) => value0 // TODO
      case (acc, (Except, x))    => value0 // TODO
      case _                     => value0
    }
  }

  /**
   * prefix is "", or "intersect", "except"
   */
  def instanceofExpr(prefix: Prefix, _treatExpr: TreatExpr, ofType: Option[SequenceType], ctx: Ctx) = {
    val value = treatExpr(_treatExpr.castableExpr, _treatExpr.asType, ctx)

    ofType match {
      case Some(x) =>
        val tpe = sequenceType(x, ctx)
        value // TODO
      case None => value
    }
  }

  def treatExpr(_castableExpr: CastableExpr, asType: Option[SequenceType], ctx: Ctx) = {
    val value = castableExpr(_castableExpr.castExpr, _castableExpr.asType, ctx)

    asType match {
      case Some(x) =>
        val tpe = sequenceType(x, ctx)
        value // TODO
      case None => value
    }
  }

  def castableExpr(_castExpr: CastExpr, asType: Option[SingleType], ctx: Ctx) = {
    val value = castExpr(_castExpr.unaryExpr, _castExpr.asType, ctx)

    asType match {
      case Some(x) =>
        val tpe = singleType(x.name, x.withQuestionMark, ctx)
        value // TODO
      case None => value
    }
  }

  def castExpr(_unaryExpr: UnaryExpr, asType: Option[SingleType], ctx: Ctx) = {
    val v0 = unaryExpr(_unaryExpr.prefix, _unaryExpr.valueExpr, ctx)
    val value = _unaryExpr.prefix match {
      case Nop | Plus => v0
      case Minus      => XPathFunctions.neg(v0)
    }

    asType match {
      case Some(x) =>
        val tpe = singleType(x.name, x.withQuestionMark, ctx)
        value // TODO
      case None => value
    }
  }

  /**
   * prefix is "", or "-", "+"
   */
  def unaryExpr(prefix: Prefix, _valueExpr: ValueExpr, ctx: Ctx) = {
    valueExpr(_valueExpr.simpleMapExpr, ctx)
  }

  /**
   * TODO, fetch value here or later?
   */
  def valueExpr(_simpleMapExpr: SimpleMapExpr, ctx: Ctx) = {
    val newCtxs = simpleMapExpr(_simpleMapExpr.pathExpr, _simpleMapExpr.exclamExprs, ctx)
    newCtxs.target
  }

  def simpleMapExpr(_pathExpr: PathExpr, exclamExprs: List[PathExpr], ctx: Ctx) = {
    val path = pathExpr(_pathExpr.prefix, _pathExpr.relativeExpr, ctx)
    exclamExprs map { x => pathExpr(x.prefix, x.relativeExpr, ctx) }
    path
  }

  /**
   * prefix is "" or "//", "/"
   *
   *   "//" RelativePathExpr
   * / "/"  RelativePathExpr?
   * / RelativePathExpr
   */
  def pathExpr(prefix: Prefix, relativeExpr: Option[RelativePathExpr], ctx: Ctx): Ctx = {
    prefix match {
      case Nop      =>
      case Absolute =>
      case Relative =>
    }
    relativeExpr match {
      case Some(x) => relativePathExpr(x.stepExpr, x.prefixedStepExprs, ctx)
      case None    => ctx
    }
  }

  def relativePathExpr(_stepExpr: StepExpr, prefixedStepExprs: List[StepExpr], ctx: Ctx): Ctx = {
    val path0 = stepExpr(_stepExpr.prefix, _stepExpr.expr, ctx)
    prefixedStepExprs.foldLeft(path0) { (acc, x) => stepExpr(x.prefix, x.expr, acc) }
  }

  /**
   * prefix is "" or "/" or "//"
   */
  def stepExpr(prefix: Prefix, expr: Either[PostfixExpr, AxisStep], ctx: Ctx): Ctx = {
    prefix match {
      case Nop      =>
      case Absolute =>
      case Relative =>
    }

    expr match {
      case Right(ReverseAxisStep(step, predicates)) =>
        reverseAxisStep(step, predicates, ctx)
      case Right(ForwardAxisStep(step, predicates)) =>
        forwardAxisStep(step, predicates, ctx)

      case Left(x @ PostfixExpr(expr, postfixes)) =>
        val value0 = primaryExpr(expr, ctx)

        var preds = List[Predicate]()
        postfixes map {
          case x: Predicate                      => preds ::= x
          case ArgumentList(args)                => argumentList(args, ctx)
          case Lookup(keySpecifier)              => lookup(keySpecifier, ctx)
          case ArrowPostfix(arrowFunction, args) => arrowPostfix(arrowFunction, args, ctx)
        }
        val predicates = PredicateList(preds.reverse)

        value0 match {
          case ContextItemExpr =>
            // ".", or, self::node()
            val step = ForwardStep(Self, AnyKindTest)
            forwardAxisStep(step, predicates, ctx)
          case _ =>
            Ctx(null, value0) // TODO
        }
    }
  }

  def reverseAxisStep(step: ReverseStep, predicates: PredicateList, ctx: Ctx): Ctx = {
    val newCtx = nodeTest(step.axis, step.nodeTest, ctx)
    val preds = predicateList(predicates.predicates, ctx)
    internal_filterByPredicateList(preds, newCtx)
  }

  def forwardAxisStep(step: ForwardStep, predicates: PredicateList, ctx: Ctx): Ctx = {
    val newCtx = nodeTest(step.axis, step.nodeTest, ctx)
    val preds = predicateList(predicates.predicates, newCtx)
    internal_filterByPredicateList(preds, newCtx)
  }

  def internal_filterByPredicateList(preds: List[Any], ctx: Ctx): Ctx = {
    //println("ctx: " + ctx + " in predicates: " + preds)
    if (preds.nonEmpty) {
      ctx.schema.getType match {
        case Schema.Type.ARRAY =>
          import scala.collection.JavaConversions._
          val arr = ctx.target match {
            case xs: java.util.Collection[Any] @unchecked => xs.iterator.toIterator
            case xs: List[Any] @unchecked                 => xs.iterator
          }
          val elemSchema = avro.getElementType(ctx.schema)
          preds.head match {
            case ix: Number =>
              var elems = List[Any]()
              val idx = ix.intValue
              var i = 1
              while (arr.hasNext && i <= idx) {
                if (i == idx) {
                  elems ::= arr.next
                } else {
                  arr.next
                }
                i += 1
              }

              Ctx(ctx.schema, elems.reverse)

            case bools: List[Boolean] @unchecked =>
              var elems = List[Any]()
              val conds = bools.iterator
              while (arr.hasNext && conds.hasNext) {
                if (conds.next) {
                  elems ::= arr.next
                } else {
                  arr.next
                }
              }

              Ctx(ctx.schema, elems.reverse)

            case _ =>
              ctx // TODO
          }

        case Schema.Type.MAP => // TODO
          val map = ctx.target.asInstanceOf[java.util.Map[String, Any]]
          val valueSchema = avro.getValueType(ctx.schema)
          preds.head match {
            case xs: List[Boolean] @unchecked =>
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

        case tpe =>
          //println("value: " + ctx.target + ", pred: " + preds.head)
          preds.head match {
            case x: Boolean => if (x) ctx else Ctx(ctx.schema, ())
            case _          => ctx
          }
      }
    } else ctx
  }

  def nodeTest(axis: Axis, test: NodeTest, ctx: Ctx): Ctx = {
    test match {
      case x: NameTest =>
        val res = nameTest(x, ctx)
        internal_ctxOfNameTest(axis, res, ctx)
      case x: KindTest =>
        kindTest(x, ctx) // TODO
        ctx
    }
  }

  /**
   * Node: Always convert selected collection result to scala list except which
   * is selected by exactly the field name.
   */
  def internal_ctxOfNameTest(axis: Axis, testResult: Any, ctx: Ctx): Ctx = {
    testResult match {
      case URIQualifiedName(uri, name) => ctx
      case PrefixedName(prefix, local) => ctx
      case UnprefixedName(local) =>
        val schema = ctx.schema
        val target = ctx.target

        axis match {
          case Child =>
            target match {
              case rec: IndexedRecord =>
                val field = schema.getField(local)
                val fieldSchema = avro.getNonNull(field.schema)
                Ctx(fieldSchema, rec.get(field.pos))

              case arr: java.util.Collection[IndexedRecord] @unchecked =>
                var elems = List[Any]()
                val elemSchema = avro.getElementType(schema)
                val field = elemSchema.getField(local)
                val fieldSchema = avro.getNonNull(field.schema)
                val itr = arr.iterator
                while (itr.hasNext) {
                  val elem = itr.next
                  elems ::= elem.get(field.pos)
                }
                Ctx(schema, elems.reverse)

              case xs: List[IndexedRecord] @unchecked =>
                //println("local: " + local + ", schema: " + schema)
                val elemSchema = avro.getElementType(schema)
                val field = elemSchema.getField(local)
                val fieldSchema = avro.getNonNull(field.schema)

                val elems = xs map (_.get(field.pos))
                Ctx(schema, elems)

              case x => // what happens?
                throw new XPathRuntimeException(x, "try to get child of: " + local)
            }

          case Attribute =>
            target match {
              case map: java.util.Map[String, Any] @unchecked =>
                // ok we're fetching a map value via key
                val valueSchema = avro.getValueType(schema)
                Ctx(valueSchema, map.get(local))

              case x => // what happens?
                throw new XPathRuntimeException(x, "try to get attribute of: " + local)
            }

        }

      case Aster =>
        val schema = ctx.schema
        val target = ctx.target

        axis match {
          case Child => ctx // TODO

          case Attribute =>
            //println("target: ", target)
            target match {
              case map: java.util.Map[String, Any] @unchecked =>
                var elems = List[Any]()
                // ok we're fetching all map values
                val valueSchema = avro.getValueType(schema)
                val itr = map.entrySet.iterator
                while (itr.hasNext) {
                  elems ::= itr.next.getValue
                }
                // we convert values to a scala List, since it's selected result
                Ctx(Schema.createArray(valueSchema), elems.reverse)

              case x => // what happens?
                throw new XPathRuntimeException(x, "try to get attribute of: " + Aster)
            }

          case _ => ctx // TODO

        }

      case NameAster(name) => ctx // TODO
      case AsterName(name) => ctx // TODO
      case UriAster(uri)   => ctx // TODO

      case _ =>
        //println("NodeTest res: " + testResult)
        ctx

    }
  }

  def nameTest(test: NameTest, ctx: Ctx) = {
    test match {
      case name: EQName       => name
      case wildcard: Wildcard => wildcard
    }
  }

  def kindTest(test: KindTest, ctx: Ctx) = {
    test match {
      case AnyKindTest =>
      case DocumentTest(elemTest) => documentTest(elemTest, ctx)
      case TextTest =>
      case CommentTest =>
      case NamespaceNodeTest =>
      case PITest(name) => piTest(name, ctx)
      case AttributeTest_Empty =>
      case AttributeTest_Name(name, typeName) => attributeTest_Name(name, typeName, ctx)
      case SchemaAttributeTest(attrDecl) => schemaAttributeTest(attrDecl, ctx)
      case ElementTest_Empty =>
      case ElementTest_Name(name, typeName, withQuestionMark) => elementTest_Name(name, typeName, withQuestionMark, ctx)
      case SchemaElementTest(elemDecl) => schemaElementTest(elemDecl, ctx)
    }
  }

  def argumentList(args: List[Argument], ctx: Ctx): List[Any] = {
    args map {
      case expr: ExprSingle    => exprSingle(expr, ctx)
      case ArgumentPlaceholder => ArgumentPlaceholder
    }
  }

  def predicateList(predicates: List[Predicate], ctx: Ctx): List[Any] = {
    predicates map { x => predicate(x.expr, ctx) }
  }

  /**
   * predicate is composed by 'expr' which may have multiple values, but actually
   * it's rare?  So we just pick the head one.
   */
  def predicate(_expr: Expr, ctx: Ctx): Any = {
    expr(_expr.expr, _expr.exprs, ctx).head
  }

  def lookup(_keySpecifier: KeySpecifier, ctx: Ctx) = {
    keySpecifier(_keySpecifier, ctx)
  }

  def keySpecifier(key: AnyRef, ctx: Ctx) = {
    key match {
      case ncName: String          => ncName
      case v: java.lang.Integer    => v
      case ParenthesizedExpr(expr) => parenthesizedExpr(expr, ctx)
      case Aster                   => Aster
      case x                       => throw new XPathRuntimeException(x, "could not to be a KeySpecifier.")
    }
  }

  def arrowPostfix(arrowFunction: ArrowFunctionSpecifier, args: ArgumentList, ctx: Ctx) = {
    arrowFunction match {
      case name: EQName            =>
      case VarRef(varName)         =>
      case ParenthesizedExpr(expr) => parenthesizedExpr(expr, ctx)
    }
  }

  def primaryExpr(expr: PrimaryExpr, ctx: Ctx) = {
    expr match {
      case Literal(x)                                       => literal(x, ctx)
      case VarRef(varName)                                  => varRef(varName, ctx)
      case ParenthesizedExpr(expr)                          => parenthesizedExpr(expr, ctx)
      case ContextItemExpr                                  => ContextItemExpr
      case FunctionCall(name, args)                         => functionCall(name, args, ctx)
      case NamedFunctionRef(name, index)                    => namedFunctionRef(name, index, ctx)
      case InlineFunctionExpr(params, asType, functionBody) => inlineFunctionExpr(params, asType, functionBody, ctx)
      case MapConstructor(entries)                          => mapConstructor(entries, ctx)
      case x: ArrayConstructor                              => arrayConstructor(x, ctx)
      case UnaryLookup(key)                                 => unaryLookup(key, ctx)
    }
  }

  /**
   * Force Any to AnyRef to avoid AnyVal be boxed/unboxed again and again
   */
  def literal(v: Any, ctx: Ctx): AnyRef = {
    v.asInstanceOf[AnyRef]
  }

  def varRef(_varName: VarName, ctx: Ctx) = {
    varName(_varName.eqName, ctx)
  }

  def varName(eqName: EQName, ctx: Ctx): EQName = {
    eqName
  }

  def parenthesizedExpr(_expr: Option[Expr], ctx: Ctx): Option[List[Any]] = {
    _expr map { x => expr(x.expr, x.exprs, ctx) }
  }

  def functionCall(name: EQName, _args: ArgumentList, ctx: Ctx) = {
    val fnName = name match {
      case UnprefixedName(local)       => local
      case PrefixedName(prefix, local) => local
      case URIQualifiedName(uri, name) => name
    }
    val args = argumentList(_args.args, ctx)

    //println("target: ", ctx.target)
    fnName match {
      case "last"     => XPathFunctions.last(ctx.target.asInstanceOf[java.util.Collection[Any]])
      case "position" => XPathFunctions.position(ctx.target.asInstanceOf[java.util.Collection[Any]])

      case "not"      => XPathFunctions.not(args.head)
      case "true"     => true
      case "false"    => false
      case _          => throw new XPathRuntimeException(fnName, "is not a supported functon")
    }
  }

  def namedFunctionRef(name: EQName, index: Int, ctx: Ctx) = {
    // TODO
  }

  def inlineFunctionExpr(params: Option[ParamList], asType: Option[SequenceType], _functionBody: FunctionBody, ctx: Ctx) = {
    params match {
      case Some(x) => paramList(x.param, x.params, ctx)
      case None    => Nil
    }
    asType map { sequenceType(_, ctx) }
    functionBody(_functionBody.expr, ctx)
  }

  def mapConstructor(entrys: List[MapConstructorEntry], ctx: Ctx) = {
    entrys map { x => mapConstructorEntry(x.key, x.value, ctx) }
  }

  def mapConstructorEntry(key: MapKeyExpr, value: MapValueExpr, ctx: Ctx): Any = {
    exprSingle(key.expr, ctx)
    exprSingle(value.expr, ctx)
  }

  def arrayConstructor(constructor: ArrayConstructor, ctx: Ctx): Any = {
    constructor match {
      case SquareArrayConstructor(exprs) =>
        exprs map { x => exprSingle(x, ctx) }
      case BraceArrayConstructor(_expr) =>
        _expr match {
          case Some(Expr(expr0, exprs)) => expr(expr0, exprs, ctx)
          case None                     =>
        }
    }
  }

  def unaryLookup(key: KeySpecifier, ctx: Ctx) = {
    keySpecifier(key, ctx)
  }

  def singleType(name: SimpleTypeName, withQuestionMark: Boolean, ctx: Ctx) = {
    simpleTypeName(name.name, ctx)
  }

  def typeDeclaration(asType: SequenceType, ctx: Ctx) = {
    sequenceType(asType, ctx)
  }

  def sequenceType(tpe: SequenceType, ctx: Ctx) = {
    tpe match {
      case SequenceType_Empty =>
      case SequenceType_ItemType(_itemType, occurrence) =>
        itemType(_itemType, ctx)
        occurrence match {
          case None                     =>
          case Some(OccurrenceQuestion) =>
          case Some(OccurrenceAster)    =>
          case Some(OccurrencePlus)     =>
        }
    }
  }

  def itemType(tpe: ItemType, ctx: Ctx): Any = {
    tpe match {
      case ItemType_ITEM                    =>
      case AtomicOrUnionType(eqName)        => eqName
      case x: KindTest                      => kindTest(x, ctx)
      case x: FunctionTest                  => functionTest(x, ctx)
      case x: MapTest                       => mapTest(x, ctx)
      case x: ArrayTest                     => arrayTest(x, ctx)
      case ParenthesizedItemType(_itemType) => itemType(_itemType, ctx)
    }
  }

  /**
   * elemTest should be either ElementTest or SchemaElementTest
   */
  def documentTest(elemTest: Option[KindTest], ctx: Ctx): Any = {
    elemTest map { x => kindTest(x, ctx) }
  }

  def piTest(name: Option[String], ctx: Ctx) = {
    name match {
      case Some(x) =>
      case None    =>
    }
  }

  def attributeTest_Name(name: AttribNameOrWildcard, typeName: Option[TypeName], ctx: Ctx) = {
    // TODO optional type name
    name match {
      case Left(x)  => attributeName(x.name, ctx)
      case Right(x) => // Aster - QName
    }
  }

  def schemaAttributeTest(attrDecl: AttributeDeclaration, ctx: Ctx) = {
    attributeDeclaration(attrDecl.name, ctx)
  }

  def attributeDeclaration(name: AttributeName, ctx: Ctx) = {
    attributeName(name.name, ctx)
  }

  def elementTest_Name(name: ElementNameOrWildcard, typeName: Option[TypeName], withQuestionMark: Boolean, ctx: Ctx) = {
    name match {
      case Left(x)  => elementName(x.name, ctx)
      case Right(x) => // Aster - QName
    }
  }

  def schemaElementTest(elemDecl: ElementDeclaration, ctx: Ctx) = {
    elementDeclaration(elemDecl.name, ctx)
  }

  def elementDeclaration(name: ElementName, ctx: Ctx) = {
    elementName(name.name, ctx)
  }

  def attributeName(name: EQName, ctx: Ctx): EQName = {
    name
  }

  def elementName(name: EQName, ctx: Ctx): EQName = {
    name
  }

  def simpleTypeName(name: TypeName, ctx: Ctx): EQName = {
    typeName(name.name, ctx)
  }

  def typeName(name: EQName, ctx: Ctx): EQName = {
    name
  }

  def functionTest(test: FunctionTest, ctx: Ctx) = {
    test match {
      case AnyFunctionTest                  =>
      case TypedFunctionTest(types, asType) => typedFunctionTest(types, asType, ctx)
    }
  }

  def typedFunctionTest(types: List[SequenceType], asType: SequenceType, ctx: Ctx): Any = {
    types map { sequenceType(_, ctx) }
    sequenceType(asType, ctx)
  }

  def mapTest(test: MapTest, ctx: Ctx) = {
    test match {
      case AnyMapTest                =>
      case TypedMapTest(tpe, asType) => typedMapTest(tpe, asType, ctx)
    }
  }

  def typedMapTest(tpe: AtomicOrUnionType, asType: SequenceType, ctx: Ctx) = {
    //tpe.eqName
    sequenceType(asType, ctx)
  }

  def arrayTest(test: ArrayTest, ctx: Ctx) = {
    test match {
      case AnyArrayTest        =>
      case TypedArrayTest(tpe) => typedArrayTest(tpe, ctx)
    }
  }

  def typedArrayTest(tpe: SequenceType, ctx: Ctx) = {
    sequenceType(tpe, ctx)
  }

}
