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

object Ctx {
  def apply(schema: Schema, target: Any, container: Container, value: Any) = new Ctx(schema, target, container, value)
  def unapply(x: Ctx): Option[(Schema, Any, Container)] = Some((x.schema, x.target, x.container))
}
final class Ctx(val schema: Schema, val target: Any, val container: Container, private var _value: Any) {
  /**
   * value during evaluating
   */
  def value = _value
  def value(x: Any) = {
    _value = x
    this
  }

  def copy = Ctx(schema, target, container, value)

  override def toString() = {
    new StringBuilder().append("Ctx(")
      .append(schema).append(",")
      .append(target).append(",")
      .append(container).append(",")
      .append(value).append(")")
      .toString
  }
}

sealed trait Container
sealed trait CollectionContainer[T] extends Container { self =>
  private var _keys = List[T]()
  def keys: List[T] = _keys
  def keys(xs: List[_]): this.type = {
    _keys = xs.asInstanceOf[List[T]]
    this
  }
}

object RecordContainer {
  def apply(record: IndexedRecord, field: Schema.Field) = new RecordContainer(record, field)
  def unapply(x: RecordContainer): Option[(IndexedRecord, Schema.Field)] = Some((x.record, x.field))
}
final class RecordContainer(val record: IndexedRecord, var field: Schema.Field) extends Container

object ArrayContainer {
  def apply(array: java.util.Collection[_], arraySchema: Schema, idxes: List[Int]) = new ArrayContainer(array, arraySchema).keys(idxes)
  def unapply(x: ArrayContainer): Option[(java.util.Collection[_], Schema, List[Int])] = Some((x.array, x.arraySchema, x.keys))
}
final class ArrayContainer(val array: java.util.Collection[_], val arraySchema: Schema) extends CollectionContainer[Int]

object MapContainer {
  def apply(map: java.util.Map[String, _], mapSchema: Schema, keys: List[String]) = new MapContainer(map, mapSchema).keys(keys)
  def unapply(x: MapContainer): Option[(java.util.Map[String, _], Schema, List[String])] = Some((x.map, x.mapSchema, x.keys))
}
final class MapContainer(val map: java.util.Map[String, _], val mapSchema: Schema) extends CollectionContainer[String]

class XPathEvaluator {

  def simpleEval(xpath: Expr, ctx: Ctx) = {
    expr(xpath.expr, xpath.exprs, ctx) map (_.value)
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

  def enclosedExpr(_expr: Expr, ctx: Ctx) = {
    expr(_expr.expr, _expr.exprs, ctx)
  }

  // TODO should we support multiple exprSingles? which brings List values
  def expr(expr: ExprSingle, exprs: List[ExprSingle], ctx: Ctx): List[Ctx] = {
    expr :: exprs map { exprSingle(_, ctx) }
  }

  def exprSingle(expr: ExprSingle, ctx: Ctx): Ctx = {
    expr match {
      case ForExpr(forClause, returnExpr)                         => forExpr(forClause, returnExpr, ctx)
      case LetExpr(letClause, returnExpr)                         => letExpr(letClause, returnExpr, ctx)
      case QuantifiedExpr(isEvery, varExpr, varExprs, statisExpr) => quantifiedExpr(isEvery, varExpr, varExprs, statisExpr, ctx)
      case IfExpr(_ifExpr, thenExpr, elseExpr)                    => ifExpr(_ifExpr, thenExpr, elseExpr, ctx)
      case OrExpr(andExpr, andExprs)                              => orExpr(andExpr, andExprs, ctx)
    }
  }

  def forExpr(forClause: SimpleForClause, returnExpr: ExprSingle, ctx: Ctx) = {
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

  def letExpr(letClause: SimpleLetClause, returnExpr: ExprSingle, ctx: Ctx) = {
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

  def quantifiedExpr(isEvery: Boolean, varExpr: VarInExprSingle, varExprs: List[VarInExprSingle], statisExpr: ExprSingle, ctx: Ctx) = {
    varExpr :: varExprs map { x => varInExprSingle(x.varName, x.inExpr, ctx) }
    exprSingle(statisExpr, ctx)
  }

  def varInExprSingle(_varName: VarName, inExpr: ExprSingle, ctx: Ctx) = {
    varName(_varName.eqName, ctx)
    exprSingle(inExpr, ctx)
  }

  def ifExpr(ifExpr: Expr, thenExpr: ExprSingle, elseExpr: ExprSingle, ctx: Ctx) = {
    expr(ifExpr.expr, ifExpr.exprs, ctx)
    exprSingle(thenExpr, ctx)
    exprSingle(elseExpr, ctx)
  }

  def orExpr(_andExpr: AndExpr, andExprs: List[AndExpr], ctx: Ctx): Ctx = {
    val value0 = andExpr(_andExpr.compExpr, _andExpr.compExprs, ctx).value

    val values = andExprs map { x => andExpr(x.compExpr, x.compExprs, ctx).value }
    val value1 = values.foldLeft(value0) { (acc, x) => XPathFunctions.or(acc, x) }
    ctx.value(value1)
  }

  def andExpr(compExpr: ComparisonExpr, compExprs: List[ComparisonExpr], ctx: Ctx): Ctx = {
    val value0 = comparisonExpr(compExpr.concExpr, compExpr.compExprPostfix, ctx).value

    val values = compExprs map { x => comparisonExpr(x.concExpr, x.compExprPostfix, ctx).value }
    val value1 = values.foldLeft(value0) { (acc, x) => XPathFunctions.and(acc, x) }
    ctx.value(value1)
  }

  def comparisonExpr(concExpr: StringConcatExpr, compExprPostfix: Option[ComparisonExprPostfix], ctx: Ctx): Ctx = {
    val value0 = stringConcatExpr(concExpr.rangeExpr, concExpr.rangeExprs, ctx).value

    val value1 = compExprPostfix match {
      case None => value0
      case Some(ComparisonExprPostfix(compOp, concExpr)) =>
        val right = comparisonExprPostfix(compOp, concExpr, ctx).value
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
    ctx.value(value1)
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
    val value0 = rangeExpr(_rangeExpr.addExpr, _rangeExpr.toExpr, ctx).value

    val values = rangeExprs map { x => rangeExpr(x.addExpr, x.toExpr, ctx).value }
    val value1 = if (values.nonEmpty) {
      XPathFunctions.strConcat(value0 :: values)
    } else {
      value0
    }
    ctx.value(value1)
  }

  def rangeExpr(addExpr: AdditiveExpr, toExpr: Option[AdditiveExpr], ctx: Ctx): Ctx = {
    val value0 = additiveExpr(addExpr.multiExpr, addExpr.prefixedMultiExprs, ctx).value

    val value1 = toExpr map { x => additiveExpr(x.multiExpr, x.prefixedMultiExprs, ctx).value } match {
      case None    => value0
      case Some(x) => XPathFunctions.range(value0, x)
    }
    ctx.value(value1)
  }

  def additiveExpr(multiExpr: MultiplicativeExpr, prefixedMultiExprs: List[MultiplicativeExpr], ctx: Ctx): Ctx = {
    val v0 = multiplicativeExpr(multiExpr.prefix, multiExpr.unionExpr, multiExpr.prefixedUnionExprs, ctx).value
    val value0 = multiExpr.prefix match {
      case Nop | Plus => v0
      case Minus      => XPathFunctions.neg(v0)
    }

    val values = prefixedMultiExprs map { x => (x.prefix, multiplicativeExpr(x.prefix, x.unionExpr, x.prefixedUnionExprs, ctx).value) }
    val value1 = values.foldLeft(value0) {
      case (acc, (Plus, x))  => XPathFunctions.plus(acc, x)
      case (acc, (Minus, x)) => XPathFunctions.minus(acc, x)
      case _                 => value0
    }
    ctx.value(value1)
  }

  /**
   * prefix is "", or "+", "-"
   */
  def multiplicativeExpr(prefix: Prefix, _unionExpr: UnionExpr, prefixedUnionExprs: List[UnionExpr], ctx: Ctx): Ctx = {
    val value0 = unionExpr(_unionExpr.prefix, _unionExpr.intersectExceptExpr, _unionExpr.prefixedIntersectExceptExprs, ctx).value

    val values = prefixedUnionExprs map { x => (x.prefix, unionExpr(x.prefix, x.intersectExceptExpr, x.prefixedIntersectExceptExprs, ctx).value) }
    val value1 = values.foldLeft(value0) {
      case (acc, (Aster, x)) => XPathFunctions.multiply(acc, x)
      case (acc, (Div, x))   => XPathFunctions.divide(acc, x)
      case (acc, (IDiv, x))  => XPathFunctions.idivide(acc, x)
      case (acc, (Mod, x))   => XPathFunctions.mod(acc, x)
      case _                 => value0
    }
    ctx.value(value1)
  }

  /**
   * prefix is "", or "*", "div", "idiv", "mod"
   */
  def unionExpr(prefix: Prefix, _intersectExceptExpr: IntersectExceptExpr, prefixedIntersectExceptExprs: List[IntersectExceptExpr], ctx: Ctx): Ctx = {
    val value0 = intersectExceptExpr(_intersectExceptExpr.prefix, _intersectExceptExpr.instanceOfExpr, _intersectExceptExpr.prefixedInstanceOfExprs, ctx).value

    val values = prefixedIntersectExceptExprs map { x => (x.prefix, intersectExceptExpr(x.prefix, x.instanceOfExpr, x.prefixedInstanceOfExprs, ctx).value) }
    val value1 = values.foldLeft(value0) {
      case (acc, (Union, x)) => value0 // TODO
      case _                 => value0
    }
    ctx.value(value1)
  }

  /**
   * prefix is "", or "union", "|". The union and | operators are equivalent
   */
  def intersectExceptExpr(prefix: Prefix, _instanceOfExpr: InstanceofExpr, prefixedInstanceOfExprs: List[InstanceofExpr], ctx: Ctx): Ctx = {
    val value0 = instanceofExpr(_instanceOfExpr.prefix, _instanceOfExpr.treatExpr, _instanceOfExpr.ofType, ctx).value

    val values = prefixedInstanceOfExprs map { x => (x.prefix, instanceofExpr(x.prefix, x.treatExpr, x.ofType, ctx).value) }
    val value1 = values.foldLeft(value0) {
      case (acc, (Intersect, x)) => value0 // TODO
      case (acc, (Except, x))    => value0 // TODO
      case _                     => value0
    }
    ctx.value(value1)
  }

  /**
   * prefix is "", or "intersect", "except"
   */
  def instanceofExpr(prefix: Prefix, _treatExpr: TreatExpr, ofType: Option[SequenceType], ctx: Ctx): Ctx = {
    val value = treatExpr(_treatExpr.castableExpr, _treatExpr.asType, ctx).value

    val value1 = ofType match {
      case Some(x) =>
        val tpe = sequenceType(x, ctx)
        value // TODO
      case None => value
    }
    ctx.value(value1)
  }

  def treatExpr(_castableExpr: CastableExpr, asType: Option[SequenceType], ctx: Ctx): Ctx = {
    val value = castableExpr(_castableExpr.castExpr, _castableExpr.asType, ctx).value

    val value1 = asType match {
      case Some(x) =>
        val tpe = sequenceType(x, ctx)
        value // TODO
      case None => value
    }
    ctx.value(value1)
  }

  def castableExpr(_castExpr: CastExpr, asType: Option[SingleType], ctx: Ctx): Ctx = {
    val value = castExpr(_castExpr.unaryExpr, _castExpr.asType, ctx).value

    val value1 = asType match {
      case Some(x) =>
        val tpe = singleType(x.name, x.withQuestionMark, ctx)
        value // TODO
      case None => value
    }
    ctx.value(value1)
  }

  def castExpr(_unaryExpr: UnaryExpr, asType: Option[SingleType], ctx: Ctx): Ctx = {
    val v = unaryExpr(_unaryExpr.prefix, _unaryExpr.valueExpr, ctx).value
    val value = _unaryExpr.prefix match {
      case Nop | Plus => v
      case Minus      => XPathFunctions.neg(v)
    }

    val value1 = asType match {
      case Some(x) =>
        val tpe = singleType(x.name, x.withQuestionMark, ctx)
        value // TODO
      case None => value
    }
    ctx.value(value1)
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
    val newCtx = simpleMapExpr(_simpleMapExpr.pathExpr, _simpleMapExpr.exclamExprs, ctx)
    newCtx
  }

  def simpleMapExpr(_pathExpr: PathExpr, exclamExprs: List[PathExpr], ctx: Ctx): Ctx = {
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
            Ctx(null, value0, null, value0) // TODO
        }
    }
  }

  def reverseAxisStep(step: ReverseStep, predicates: PredicateList, ctx: Ctx): Ctx = {
    val nodeTestCtx = nodeTest(step.axis, step.nodeTest, ctx)
    // We should separate the node test ctx and predicate ctx, the node test ctx will
    // be applied by predicates, and predicate ctx will use it as the beginning
    // ctx. So, we'll rely on predicateList to make a new copy for each predicate 
    // evaluating
    val preds = predicateList(predicates.predicates, nodeTestCtx)
    _applyPredicateList(preds, nodeTestCtx)
  }

  def forwardAxisStep(step: ForwardStep, predicates: PredicateList, ctx: Ctx): Ctx = {
    val nodeTestCtx = nodeTest(step.axis, step.nodeTest, ctx)
    // We should separate the node test ctx and predicate ctx, the node test ctx will
    // be applied by predicates, and predicate ctx will use it as the beginning
    // ctx. So, we'll rely on predicateList to make a new copy for each predicate 
    // evaluating
    val preds = predicateList(predicates.predicates, nodeTestCtx)
    _applyPredicateList(preds, nodeTestCtx)
  }

  private def _applyPredicateList(preds: List[Ctx], targetCtx: Ctx): Ctx = {
    //println("ctx: " + ctx + " in predicates: " + preds)
    if (preds.nonEmpty) {
      val predicateds = preds map { pred => _applyPredicate(pred, targetCtx) }
      predicateds.head // TODO process multiple predicates
    } else {
      targetCtx
    }
  }

  private def _applyPredicate(pred: Ctx, targetCtx: Ctx): Ctx = {
    targetCtx.schema.getType match {
      case Schema.Type.ARRAY =>
        pred.value match {
          case ix: Number =>
            val container = _checkIfApplyOnCollectionField(targetCtx)
            val origKeys = container.keys.iterator

            val arr = targetCtx.target.asInstanceOf[java.util.Collection[Any]].iterator

            var elems = List[Any]()
            var keys = List[Any]()
            val idx = ix.intValue
            var i = 1
            while (arr.hasNext && i <= idx) {
              val elem = arr.next
              val key = origKeys.next
              if (i == idx) {
                elems ::= elem
                keys ::= key
              }
              i += 1
            }

            //println(ctx.target + ", " + container.keys + ", ix:" + ix + ", elems: " + elems)
            val got = elems.reverse
            Ctx(targetCtx.schema, got, container.keys(keys.reverse), got)

          case bools: List[Boolean] @unchecked =>
            val container = _checkIfApplyOnCollectionField(targetCtx)
            val origKeys = container.keys.iterator

            import scala.collection.JavaConversions._
            val arr = targetCtx.target match {
              case xs: java.util.Collection[Any] @unchecked => xs.iterator.toIterator
              case xs: List[Any] @unchecked                 => xs.iterator
            }

            var elems = List[Any]()
            var keys = List[Any]()
            val conds = bools.iterator
            while (arr.hasNext && conds.hasNext) {
              val elem = arr.next
              val key = origKeys.next
              val cond = conds.next
              if (cond) {
                elems ::= elem
                keys ::= key
              }
            }

            //println(ctx.target + ", " + container.keys + ", elems: " + elems)
            val got = elems.reverse
            Ctx(targetCtx.schema, got, container.keys(keys.reverse), got)

          case _ =>
            targetCtx // TODO
        }

      case Schema.Type.MAP => // TODO
        pred.value match {
          case bools: List[Boolean] @unchecked =>
            val container = _checkIfApplyOnCollectionField(targetCtx)
            val origKeys = container.keys.iterator

            val map = targetCtx.target.asInstanceOf[java.util.Map[String, Any]].entrySet.iterator

            var entries = List[Any]()
            var keys = List[Any]()
            val conds = bools.iterator
            while (map.hasNext && conds.hasNext) {
              val entry = map.next
              val key = origKeys.next
              val cond = conds.next
              if (cond) {
                entries ::= entry
                keys ::= key
              }
            }

            //println(ctx.target + ", " + container.keys + ", entries: " + entries)
            val got = entries.reverse
            Ctx(targetCtx.schema, got, container.keys(keys.reverse), got)

          case _ =>
            targetCtx // TODO
        }

      case tpe =>
        //println("value: " + ctx.target + ", pred: " + pred)
        pred.value match {
          case x: Boolean => if (x) targetCtx else Ctx(targetCtx.schema, (), targetCtx.container, ())
          case _          => targetCtx
        }
    }

  }

  /**
   * When apply filtering on record's collection field, we know that the
   * container should be transfered to a CollectionContainer now.
   */
  private def _checkIfApplyOnCollectionField(ctx: Ctx): CollectionContainer[_] = {
    ctx.container match {
      case x: RecordContainer =>
        ctx.target match {
          case xs: java.util.Collection[Any] @unchecked =>
            val n = xs.size
            var keys = List[Int]()
            var i = 1
            while (i <= n) {
              keys ::= i
              i += 1
            }

            ArrayContainer(xs, ctx.schema, keys.reverse)

          case xs: java.util.Map[String, Any] @unchecked =>
            var keys = List[String]()
            val itr = xs.keySet.iterator
            while (itr.hasNext) {
              keys ::= itr.next
            }

            MapContainer(xs, ctx.schema, keys.reverse)
        }
      case x: CollectionContainer[_] => x
    }
  }

  def nodeTest(axis: Axis, test: NodeTest, ctx: Ctx): Ctx = {
    test match {
      case x: NameTest =>
        val res = nameTest(x, ctx)
        _ctxOfNameTest(axis, res, ctx)
      case x: KindTest =>
        kindTest(x, ctx) // TODO
        ctx
    }
  }

  /**
   * Node: Always convert selected collection result to scala list except which
   * is selected by exactly the field name.
   */
  private def _ctxOfNameTest(axis: Axis, testResult: Any, ctx: Ctx): Ctx = {
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
                val got = rec.get(field.pos)
                Ctx(fieldSchema, got, RecordContainer(rec, field), got)

              case arr: java.util.Collection[IndexedRecord] @unchecked =>
                var elems = List[Any]()
                var idxes = List[Int]()
                val elemSchema = avro.getElementType(schema)
                val field = elemSchema.getField(local)
                val fieldSchema = avro.getNonNull(field.schema)
                val itr = arr.iterator
                var i = 1
                while (itr.hasNext) {
                  val elem = itr.next
                  elems ::= elem.get(field.pos)
                  idxes ::= i
                  i += 1
                }
                val got = elems.reverse
                Ctx(schema, got, ArrayContainer(arr, schema, idxes.reverse), got)

              case map: java.util.Map[String, IndexedRecord] @unchecked =>
                var entries = List[Any]()
                var keys = List[String]()
                val valueSchema = avro.getValueType(schema)
                val field = valueSchema.getField(local)
                val fieldSchema = avro.getNonNull(field.schema)
                val itr = map.entrySet.iterator
                while (itr.hasNext) {
                  val entry = itr.next
                  entries ::= entry.getValue.get(field.pos)
                  keys ::= entry.getKey
                }
                val got = entries.reverse
                Ctx(Schema.createArray(fieldSchema), got, MapContainer(map, schema, keys.reverse), got)

              case xs: List[IndexedRecord] @unchecked =>
                //println("local: " + local + ", schema: " + schema)
                val elemSchema = avro.getElementType(schema)
                val field = elemSchema.getField(local)
                val fieldSchema = avro.getNonNull(field.schema)
                val got = xs map (_.get(field.pos))
                Ctx(schema, got, ctx.container, got)

              case x => // what happens?
                throw new XPathRuntimeException(x, "try to get child: " + local)
            }

          case Attribute =>
            target match {
              case map: java.util.Map[String, Any] @unchecked =>
                // ok we're fetching a map value via key
                val valueSchema = avro.getValueType(schema)
                val got = map.get(local)
                Ctx(valueSchema, got, MapContainer(map, schema, List(local)), got)

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
                var keys = List[String]()
                // ok we're fetching all map values
                val valueSchema = avro.getValueType(schema)
                val itr = map.entrySet.iterator
                while (itr.hasNext) {
                  val entry = itr.next
                  elems ::= entry.getValue
                  keys ::= entry.getKey
                }
                val got = elems.reverse
                // we convert values to a scala List, since it's selected result
                Ctx(Schema.createArray(valueSchema), got, MapContainer(map, schema, keys.reverse), got)

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

  def argumentList(args: List[Argument], ctx: Ctx): List[Ctx] = {
    args map {
      case expr: ExprSingle    => exprSingle(expr, ctx)
      case ArgumentPlaceholder => ctx.value(ArgumentPlaceholder)
    }
  }

  /**
   * Should make a new copy of ctx for each predicate, since we are in new ctx
   * during each predicate evaluating.
   */
  def predicateList(predicates: List[Predicate], ctx: Ctx): List[Ctx] = {
    predicates map { x => predicate(x.expr, ctx.copy) }
  }

  /**
   * predicate is composed by 'expr' which may have multiple values, but actually
   * it's rare?  So we just pick the head one.
   */
  def predicate(_expr: Expr, ctx: Ctx): Ctx = {
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

  def primaryExpr(expr: PrimaryExpr, ctx: Ctx): Any = {
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

  def parenthesizedExpr(_expr: Option[Expr], ctx: Ctx): Option[List[Ctx]] = {
    _expr map { x => expr(x.expr, x.exprs, ctx) }
  }

  def functionCall(name: EQName, _args: ArgumentList, ctx: Ctx): Any = {
    val fnName = name match {
      case UnprefixedName(local)       => local
      case PrefixedName(prefix, local) => local
      case URIQualifiedName(uri, name) => name
    }
    val args = argumentList(_args.args, ctx)

    fnName match {
      case "last"     => XPathFunctions.last(ctx.target.asInstanceOf[java.util.Collection[Any]])
      case "position" => XPathFunctions.position(ctx.target.asInstanceOf[java.util.Collection[Any]])

      case "not"      => XPathFunctions.not(args.head.value)
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
