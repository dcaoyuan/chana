package chana.avpath

import chana.avpath.Parser._
import chana.avro
import chana.avro.FromJson
import chana.avro.Changelog
import chana.avro.Deletelog
import chana.avro.Insertlog
import chana.avro.UpdateAction
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.IndexedRecord
import scala.collection.mutable

object Evaluator {
  sealed trait Target
  final case class TargetRecord(record: IndexedRecord, field: Schema.Field) extends Target
  final case class TargetArray(array: java.util.Collection[Any], idx: Int, arraySchema: Schema) extends Target
  final case class TargetMap(map: java.util.Map[String, Any], key: String, mapSchema: Schema) extends Target

  final case class Ctx(value: Any, schema: Schema, topLevelField: Schema.Field, target: Option[Target] = None)

  def select(root: IndexedRecord, ast: PathSyntax): List[Ctx] = {
    evaluatePath(ast, List(Ctx(root, root.getSchema, null)), true)
  }

  def update(root: IndexedRecord, ast: PathSyntax, value: Any): List[UpdateAction] = {
    val ctxs = select(root, ast)
    opUpdate(ctxs, value, false)
  }

  def updateJson(root: IndexedRecord, ast: PathSyntax, value: String): List[UpdateAction] = {
    val ctxs = select(root, ast)
    opUpdate(ctxs, value, true)
  }

  def insert(root: IndexedRecord, ast: PathSyntax, value: Any): List[UpdateAction] = {
    val ctxs = select(root, ast)
    opInsert(ctxs, value, false)
  }

  def insertJson(root: IndexedRecord, ast: PathSyntax, value: String): List[UpdateAction] = {
    val ctxs = select(root, ast)
    opInsert(ctxs, value, true)
  }

  def insertAll(root: IndexedRecord, ast: PathSyntax, values: java.util.Collection[_]): List[UpdateAction] = {
    val ctxs = select(root, ast)
    opInsertAll(ctxs, values, false)
  }

  def insertAllJson(root: IndexedRecord, ast: PathSyntax, values: String): List[UpdateAction] = {
    val ctxs = select(root, ast)
    opInsertAll(ctxs, values, true)
  }

  def delete(root: IndexedRecord, ast: PathSyntax): List[UpdateAction] = {
    val ctxs = select(root, ast)
    opDelete(ctxs)
  }

  def clear(root: IndexedRecord, ast: PathSyntax): List[UpdateAction] = {
    val ctxs = select(root, ast)
    opClear(ctxs)
  }

  private def targets(ctxs: List[Ctx]) = ctxs.flatMap(_.target)

  private def opDelete(ctxs: List[Ctx]): List[UpdateAction] = {
    var actions = List[UpdateAction]()

    val processingArrs = new mutable.HashMap[java.util.Collection[Any], (Schema, List[Int])]()
    targets(ctxs) foreach {
      case _: TargetRecord => // cannot apply delete on record

      case TargetArray(arr, idx, arrSchema) =>
        processingArrs += arr -> (arrSchema, idx :: processingArrs.getOrElse(arr, (arrSchema, List[Int]()))._2)

      case TargetMap(map, key, mapSchema) =>
        val prev = map.get(key)
        val rlback = { () => map.put(key, prev) }
        val commit = { () => map.remove(key) }
        actions ::= UpdateAction(commit, rlback, Deletelog("", (key, prev), mapSchema))
    }

    for ((arr, (arrSchema, _toRemove)) <- processingArrs) {
      val toRemove = _toRemove.sortBy(-_)

      // toRlback will be asc sorted
      var prev = toRemove
      var toRlback = List[(Int, Any)]()
      var i = 0
      var arrItr = arr.iterator
      while (prev.nonEmpty) {
        val idx = prev.head
        prev = prev.tail
        while (arrItr.hasNext && i <= idx) {
          if (i == idx) {
            toRlback ::= (i, arrItr.next)
          } else {
            arrItr.next
          }
          i += 1
        }
      }

      val rlback = { () => avro.arrayInsert(arr, toRlback) }
      val commit = { () => avro.arrayRemove(arr, toRemove) }
      actions ::= UpdateAction(commit, rlback, Deletelog("", toRemove, arrSchema))
    }

    actions.reverse
  }

  private def opClear(ctxs: List[Ctx]): List[UpdateAction] = {
    var actions = List[UpdateAction]()

    targets(ctxs) foreach {
      case TargetRecord(rec, field) =>
        rec.get(field.pos) match {
          case arr: java.util.Collection[_] =>
            val prev = new java.util.ArrayList(arr)
            val commit = { () => arr.clear }
            val rlback = { () => arr.addAll(prev) }
            actions ::= UpdateAction(commit, rlback, Deletelog("", prev, field.schema))

          case map: java.util.Map[String, Any] @unchecked =>
            val prev = new java.util.HashMap[String, Any](map)
            val rlback = { () => map.putAll(prev) }
            val commit = { () => map.clear }
            actions ::= UpdateAction(commit, rlback, Deletelog("", prev, field.schema))

          case _ => // ?
        }

      case _: TargetArray =>
      // why are you here, for Insert, you should op on record's arr field directly

      case _: TargetMap   =>
      // why are you here, for Insert, you should op on record's map field directly
    }

    actions.reverse
  }

  private def opUpdate(ctxs: List[Ctx], value: Any, isJsonValue: Boolean): List[UpdateAction] = {
    var actions = List[UpdateAction]()

    targets(ctxs) foreach {
      case TargetRecord(rec, null) =>
        val value1 = if (isJsonValue) FromJson.fromJsonString(value.asInstanceOf[String], rec.getSchema, false) else value
        value1 match {
          case v: IndexedRecord =>
            val prev = new GenericData.Record(rec.asInstanceOf[GenericData.Record], true)
            val rlback = { () => avro.replace(rec, prev) }
            val commit = { () => avro.replace(rec, v) }
            actions ::= UpdateAction(commit, rlback, Changelog("/", v, rec.getSchema))
          case _ => // log.error 
        }

      case TargetRecord(rec, field) =>
        val value1 = if (isJsonValue) FromJson.fromJsonString(value.asInstanceOf[String], field.schema, false) else value

        val prev = rec.get(field.pos)
        val rlback = { () => rec.put(field.pos, prev) }
        val commit = { () => rec.put(field.pos, value1) }
        actions ::= UpdateAction(commit, rlback, Changelog("/" + field.name, value1, field.schema))

      case TargetArray(arr, idx, arrSchema) =>
        val elemSchema = avro.getElementType(arrSchema)
        val value1 = if (isJsonValue) FromJson.fromJsonString(value.asInstanceOf[String], elemSchema, false) else value

        val prev = avro.arraySelect(arr, idx)
        val rlback = { () => avro.arrayUpdate(arr, idx, prev) }
        val commit = { () => avro.arrayUpdate(arr, idx, value1) }
        actions ::= UpdateAction(commit, rlback, Changelog("", (idx, value1), arrSchema))

      case TargetMap(map, key, mapSchema) =>
        val valueSchema = avro.getValueType(mapSchema)
        val value1 = if (isJsonValue) FromJson.fromJsonString(value.asInstanceOf[String], valueSchema, false) else value

        val prev = map.get(key)
        val rlback = { () => map.put(key, prev) }
        val commit = { () => map.put(key, value1) }
        actions ::= UpdateAction(commit, rlback, Changelog("", (key, value1), mapSchema))
    }

    actions.reverse
  }

  private def opInsert(ctxs: List[Ctx], value: Any, isJsonValue: Boolean): List[UpdateAction] = {
    var actions = List[UpdateAction]()

    targets(ctxs) foreach {
      case TargetRecord(rec, field) =>
        rec.get(field.pos) match {
          case arr: java.util.Collection[Any] @unchecked =>
            val elemSchema = avro.getElementType(field.schema)
            val value1 = if (isJsonValue) FromJson.fromJsonString(value.asInstanceOf[String], elemSchema, false) else value

            val rlback = { () => arr.remove(value1) }
            val commit = { () => arr.add(value1) }
            actions ::= UpdateAction(commit, rlback, Insertlog("", value1, field.schema))

          case map: java.util.Map[String, Any] @unchecked =>
            val value1 = if (isJsonValue) FromJson.fromJsonString(value.asInstanceOf[String], field.schema, false) else value
            value1 match {
              case (k: String, v) =>
                val prev = map.get(k)
                val rlback = { () => map.put(k, prev) }
                val commit = { () => map.put(k, v) }
                actions ::= UpdateAction(commit, rlback, Insertlog("", (k, v), field.schema))

              case kvs: java.util.Map[String, _] @unchecked =>
                val entries = kvs.entrySet.iterator
                // should contains only one entry
                if (entries.hasNext) {
                  val entry = entries.next

                  val prev = map.get(entry.getKey)
                  val rlback = { () => map.put(entry.getKey, prev) }
                  val commit = { () => map.put(entry.getKey, entry.getValue) }
                  actions ::= UpdateAction(commit, rlback, Insertlog("", (entry.getKey, entry.getValue), field.schema))
                }
              case _ =>
            }

          case _ => // can only insert to array/map field
        }

      case _: TargetArray =>
      // why are you here, for Insert, you should op on record's arr field directly

      case _: TargetMap   =>
      // why are you here, for Insert, you should op on record's map field directly
    }

    actions.reverse
  }

  private def opInsertAll(ctxs: List[Ctx], value: Any, isJsonValue: Boolean): List[UpdateAction] = {
    var actions = List[UpdateAction]()

    targets(ctxs) foreach {
      case TargetRecord(rec, field) =>
        rec.get(field.pos) match {
          case arr: java.util.Collection[Any] @unchecked =>
            val elemSchema = avro.getElementType(field.schema)
            val value1 = if (isJsonValue) FromJson.fromJsonString(value.asInstanceOf[String], field.schema, false) else value
            value1 match {
              case xs: java.util.Collection[Any] @unchecked =>
                val rlback = { () => arr.removeAll(xs) }
                val commit = { () => arr.addAll(xs) }
                actions ::= UpdateAction(commit, rlback, Insertlog("", xs, field.schema))

              case _ => // ?
            }

          case map: java.util.Map[String, Any] @unchecked =>
            val value1 = if (isJsonValue) FromJson.fromJsonString(value.asInstanceOf[String], field.schema, false) else value
            value1 match {
              case xs: java.util.Collection[(String, Any)] @unchecked =>
                val prev = new java.util.HashMap[String, Any]()
                val toPut = new java.util.HashMap[String, Any]()
                val itr = xs.iterator
                while (itr.hasNext) {
                  val entry = itr.next
                  val (k, v) = entry
                  prev.put(k, map.get(k))
                  toPut.put(k, v)
                }
                val rlback = { () => map.putAll(prev) }
                val commit = { () => map.putAll(toPut) }
                actions ::= UpdateAction(commit, rlback, Insertlog("", toPut, field.schema))

              case xs: java.util.Map[String, Any] @unchecked =>
                val prev = new java.util.HashMap[String, Any]()
                val itr = xs.entrySet.iterator
                while (itr.hasNext) {
                  val entry = itr.next
                  val k = entry.getKey
                  prev.put(k, map.get(k))
                }
                val rlback = { () => map.putAll(prev) }
                val commit = { () => map.putAll(xs) }
                actions ::= UpdateAction(commit, rlback, Insertlog("", xs, field.schema))

              case _ => // ?
            }
        }

      case _: TargetArray =>
      // why are you here, for Insert, you should op on record's arr field directly

      case _: TargetMap   =>
      // why are you here, for Insert, you should op on record's map field directly
    }

    actions.reverse
  }

  private def evaluatePath(path: PathSyntax, _ctxs: List[Ctx], isTopLevel: Boolean): List[Ctx] = {
    var ctxs = _ctxs

    val parts = path.parts
    val len = parts.length
    var i = 0
    var isResArray = true
    while (i < len) {
      parts(i) match {
        case x: SelectorSyntax =>
          x.selector match {
            case "."  => ctxs = evaluateSelector(x, ctxs, isTopLevel)
            case ".." => ctxs = evaluateDescendantSelector(x, ctxs)
          }
          isResArray = true

        case x: ObjPredSyntax =>
          ctxs = evaluateObjectPredicate(x, ctxs)

        case x: PosPredSyntax =>
          ctxs = evaluatePosPredicate(x, ctxs) match {
            case List()          => List()
            case List(Left(v))   => List(v)
            case List(Right(xs)) => List(xs.toList).flatten
          }

        case x: MapKeysSyntax =>
          ctxs = evaluateMapValues(x, ctxs)
          isResArray = true

        case x: ConcatExprSyntax =>
          ctxs = evaluateConcatExpr(x, ctxs)
          isResArray = true

        case _ => // should not happen

      }
      i += 1
    }

    ctxs
  }

  private def evaluateSelector(sel: SelectorSyntax, ctxs: List[Ctx], isTopLevel: Boolean): List[Ctx] = {
    var res = List[Ctx]()
    sel.prop match {
      case null =>
        // select record itself
        ctxs foreach {
          case Ctx(rec: IndexedRecord, schema, topLevelField, _) =>
            res ::= Ctx(rec, schema, null, Some(TargetRecord(rec, null)))
          case _ => // should be rec
        }
      case "*" =>
        ctxs foreach {
          case Ctx(rec: IndexedRecord, schema, topLevelField, _) =>
            val fields = rec.getSchema.getFields.iterator
            while (fields.hasNext) {
              val field = fields.next
              val value = rec.get(field.pos)
              res ::= Ctx(value, field.schema, if (topLevelField == null) field else topLevelField, Some(TargetRecord(rec, field)))
            }
          case Ctx(arr: java.util.Collection[Any] @unchecked, schema, topLevelField, _) =>
            val elemSchema = avro.getElementType(schema)
            val values = arr.iterator
            var j = 0
            while (values.hasNext) {
              val value = values.next
              res ::= Ctx(value, elemSchema, topLevelField, Some(TargetArray(arr, j, schema)))
              j += 1
            }
          case _ => // TODO map
        }

      case fieldName =>
        ctxs foreach {
          case Ctx(rec: IndexedRecord, schema, topLevelField, _) =>
            val field = rec.getSchema.getField(fieldName)
            if (field != null) {
              res ::= Ctx(rec.get(field.pos), field.schema, if (topLevelField == null) field else topLevelField, Some(TargetRecord(rec, field)))
            }
          case _ => // should be rec
        }
    }

    res.reverse
  }

  // TODO not support yet
  private def evaluateDescendantSelector(sel: SelectorSyntax, ctxs: List[Ctx]): List[Ctx] = {
    ctxs
  }

  private def evaluateObjectPredicate(expr: ObjPredSyntax, ctxs: List[Ctx]): List[Ctx] = {
    var res = List[Ctx]()
    ctxs foreach {
      case currCtx @ Ctx(rec: IndexedRecord, _, _, _) =>
        evaluateExpr(expr.arg, currCtx) match {
          case true => res ::= currCtx
          case _    =>
        }

      case Ctx(arr: java.util.Collection[Any] @unchecked, schema, topLevelField, _) =>
        val elemSchema = avro.getElementType(schema)
        val values = arr.iterator
        var i = 0
        while (values.hasNext) {
          val value = values.next
          val elemCtx = Ctx(value, elemSchema, topLevelField, Some(TargetArray(arr, i, schema)))
          evaluateExpr(expr.arg, elemCtx) match {
            case true => res ::= elemCtx
            case _ =>
              i += 1
          }
        }

      case Ctx(map: java.util.Map[String, Any] @unchecked, schema, topLevelField, _) =>
        val valSchema = avro.getValueType(schema)
        val entries = map.entrySet.iterator
        while (entries.hasNext) {
          val entry = entries.next
          val elemCtx = Ctx(entry.getValue, valSchema, topLevelField, Some(TargetMap(map, entry.getKey, schema)))
          evaluateExpr(expr.arg, elemCtx) match {
            case true => res ::= elemCtx
            case _    =>
          }
        }

      case x => println("what? " + x) // any other condition?
    }

    res.reverse
  }

  private def evaluatePosPredicate(item: PosPredSyntax, ctxs: List[Ctx]): List[Either[Ctx, Array[Ctx]]] = {
    val posExpr = item.arg

    var res = List[Either[Ctx, Array[Ctx]]]()
    ctxs foreach {
      case currCtx @ Ctx(arr: java.util.Collection[Any] @unchecked, schema, topLevelField, _) =>
        val elemSchema = avro.getElementType(schema)
        posExpr match {
          case PosSyntax(LiteralSyntax("*"), _, _) =>
            val values = arr.iterator
            val n = arr.size
            val elems = Array.ofDim[Ctx](n)
            var i = 0
            while (values.hasNext) {
              val value = values.next
              elems(i) = Ctx(value, elemSchema, topLevelField, Some(TargetArray(arr, i, schema)))
              i += 1
            }
            res ::= Right(elems)

          case PosSyntax(idxSyn: Syntax, _, _) =>
            // idx
            evaluateExpr(posExpr.idx, currCtx) match {
              case idx: Int =>
                val ix = theIdx(idx, arr.size)
                val value = arr match {
                  case xs: java.util.List[_] =>
                    xs.get(ix)
                  case _ =>
                    val values = arr.iterator
                    var i = 0
                    while (i < ix) {
                      values.next
                      i += 1
                    }
                    values.next
                }
                res ::= Left(Ctx(value, elemSchema, topLevelField, Some(TargetArray(arr, ix, schema))))
              case _ =>
            }

          case PosSyntax(null, fromIdxSyn: Syntax, toIdxSyn: Syntax) =>
            evaluateExpr(fromIdxSyn, currCtx) match {
              case fromIdx: Int =>
                evaluateExpr(toIdxSyn, currCtx) match {
                  case toIdx: Int =>
                    val n = arr.size
                    val from = theIdx(fromIdx, n)
                    val to = theIdx(toIdx, n)
                    val elems = Array.ofDim[Ctx](to - from + 1)
                    arr match {
                      case xs: java.util.List[_] =>
                        var i = from
                        while (i <= to) {
                          val value = xs.get(i)
                          elems(i - from) = Ctx(value, elemSchema, topLevelField, Some(TargetArray(arr, i, schema)))
                          i += 1
                        }
                      case _ =>
                        val values = arr.iterator
                        var i = 0
                        while (values.hasNext && i <= to) {
                          val value = values.next
                          if (i >= from) {
                            elems(i) = Ctx(value, elemSchema, topLevelField, Some(TargetArray(arr, i, schema)))
                          }
                          i += 1
                        }
                    }
                    res ::= Right(elems)

                  case _ =>
                }

              case _ =>
            }

          case PosSyntax(null, fromIdxSyn: Syntax, null) =>
            evaluateExpr(fromIdxSyn, currCtx) match {
              case fromIdx: Int =>
                val n = arr.size
                val from = theIdx(fromIdx, n)
                val elems = Array.ofDim[Ctx](n - from)
                arr match {
                  case xs: java.util.List[_] =>
                    var i = from
                    while (i < n) {
                      elems(i - from) = Ctx(xs.get(i), elemSchema, topLevelField, Some(TargetArray(arr, i, schema)))
                      i += 1
                    }
                  case _ =>
                    val values = arr.iterator
                    var i = 0
                    while (values.hasNext && i < n) {
                      val value = values.next
                      if (i >= from) {
                        elems(i - from) = Ctx(value, elemSchema, topLevelField, Some(TargetArray(arr, i, schema)))
                      }
                      i += 1
                    }
                }
                res ::= Right(elems)

              case _ =>
            }

          case PosSyntax(null, null, toIdxSyn: Syntax) =>
            evaluateExpr(toIdxSyn, currCtx) match {
              case toIdx: Int =>
                val n = arr.size
                val to = theIdx(toIdx, n)
                val elems = Array.ofDim[Ctx](to + 1)
                arr match {
                  case xs: java.util.List[_] =>
                    var i = 0
                    while (i <= to && i < n) {
                      elems(i) = Ctx(xs.get(i), elemSchema, topLevelField, Some(TargetArray(arr, i, schema)))
                      i += 1
                    }
                  case _ =>
                    val values = arr.iterator
                    var i = 0
                    while (values.hasNext && i < n) {
                      val value = values.next
                      if (i <= to) {
                        elems(i) = Ctx(value, elemSchema, topLevelField, Some(TargetArray(arr, i, schema)))
                      }
                      i += 1
                    }
                }
                res ::= Right(elems)

              case _ =>
            }

        }

      case _ => // should be array
    }
    res.reverse
  }

  private def theIdx(i: Int, size: Int) = {
    if (i >= 0) {
      if (i >= size)
        size - 1
      else
        i
    } else {
      size + i
    }
  }

  private def evaluateMapValues(syntax: MapKeysSyntax, ctxs: List[Ctx]): List[Ctx] = {
    val expectKeys = syntax.keys
    var res = List[Ctx]()
    ctxs foreach {
      case Ctx(map: java.util.Map[String, Any] @unchecked, schema, topLevelField, _) =>
        val valueSchema = avro.getValueType(schema)
        // the order of selected map items is not guaranteed due to the implemetation of java.util.Map
        val entries = map.entrySet.iterator
        while (entries.hasNext) {
          val entry = entries.next
          val key = entry.getKey
          expectKeys.collectFirst {
            case Left(expectKey) if expectKey == key        => entry.getValue
            case Right(regex) if regex.matcher(key).matches => entry.getValue
          } foreach { value =>
            res = Ctx(value, valueSchema, topLevelField, Some(TargetMap(map, key, schema))) :: res
          }
        }
      case _ => // should be map
    }

    res.reverse
  }

  private def evaluateExpr(expr: Syntax, ctx: Ctx): Any = {
    expr match {
      case x: PathSyntax =>
        evaluatePath(x, List(ctx), false).head.value

      case x: ComparionExprSyntax =>
        evaluateComparisonExpr(x, ctx)

      case x: MathExprSyntax =>
        evaluateMathExpr(x, ctx)

      case x: LogicalExprSyntax =>
        evaluateLogicalExpr(x, ctx)

      case x: UnaryExprSyntax =>
        evaluateUnaryExpr(x, ctx)

      case x: LiteralSyntax[_] =>
        x.value

      case x: SubstSyntax => // TODO 

      case _              => // should not happen
    }
  }

  private def evaluateComparisonExpr(expr: ComparionExprSyntax, ctx: Ctx) = {
    val dest: Array[Ctx] = Array() // to be dropped
    val leftArg = expr.args(0)
    val rightArg = expr.args(1)

    val v1 = evaluateExpr(leftArg, ctx)
    val v2 = evaluateExpr(rightArg, ctx)

    val isLeftArgPath = leftArg.isInstanceOf[PathSyntax]
    val isRightArgLiteral = rightArg.isInstanceOf[LiteralSyntax[_]]

    binaryOperators(expr.op)(v1, v2)
  }

  private def writeCondition(op: String, v1Expr: Any, v2Expr: Any) = {
    binaryOperators(op)(v1Expr, v2Expr)
  }

  private def evaluateLogicalExpr(expr: LogicalExprSyntax, ctx: Ctx): Boolean = {
    val args = expr.args

    expr.op match {
      case "&&" =>
        args.foldLeft(true) { (acc, arg) =>
          acc && convertToBool(arg, evaluateExpr(arg, ctx))
        }

      case "||" =>
        args.foldLeft(false) { (acc, arg) =>
          acc || convertToBool(arg, evaluateExpr(arg, ctx))
        }
    }
  }

  private def evaluateMathExpr(expr: MathExprSyntax, ctx: Ctx) = {
    val args = expr.args

    val v1 = evaluateExpr(args(0), ctx)
    val v2 = evaluateExpr(args(1), ctx)

    binaryOperators(expr.op)(
      convertToSingleValue(args(0), v1),
      convertToSingleValue(args(1), v2))
  }

  private def evaluateUnaryExpr(expr: UnaryExprSyntax, ctx: Ctx) = {
    val arg = expr.arg
    val value = evaluateExpr(expr.arg, ctx)

    expr.op match {
      case "!" =>
        !convertToBool(arg, value)

      case "-" =>
        value match {
          case x: Int    => -x
          case x: Long   => -x
          case x: Float  => -x
          case x: Double => -x
          case _         => // error
        }
    }
  }

  private def evaluateConcatExpr(expr: ConcatExprSyntax, ctxs: List[Ctx]): List[Ctx] = {
    var res = List[Ctx]()
    val args = expr.args
    val len = args.length
    var i = 0
    while (i < len) {
      val argVar = evaluatePath(args(i).asInstanceOf[PathSyntax], ctxs, false)
      res :::= argVar
      i += 1
    }

    res.reverse
  }

  private def convertToBool(arg: Syntax, value: Any): Boolean = {
    arg match {
      case _: LogicalExprSyntax =>
        value match {
          case x: Boolean => x
          case _          => false
        }

      case _: LiteralSyntax[_] =>
        value match {
          case x: Boolean => x
          case _          => false
        }

      case _: PathSyntax =>
        value match {
          case x: Boolean => x
          case _          => false
        }

      case _ =>
        value match {
          case x: Boolean => x
          case _          => false
        }
    }
  }

  private def convertToSingleValue(arg: Syntax, value: Any): Any = {
    arg match {
      case _: LiteralSyntax[_] => value
      case _: PathSyntax =>
        value match {
          case h :: xs => h
          case _       => value
        }
      case _ => value match {
        case h :: xs => h
        case _       => value
      }
    }
  }

  private def binaryOperators(op: String)(v1: Any, v2: Any) = {
    op match {
      case "===" =>
        v1 == v2

      case "==" =>
        (v1, v2) match {
          case (s1: CharSequence, s2: CharSequence) => s1.toString.toLowerCase == s2.toString.toLowerCase
          case _                                    => v1 == v2
        }

      case "!==" =>
        v1 != v2

      case "!=" =>
        v1 != v2

      case "^==" =>
        (v1, v2) match {
          case (s1: CharSequence, s2: CharSequence) => s1.toString.startsWith(s2.toString)
          case _                                    => false
        }

      case "^=" =>
        (v1, v2) match {
          case (s1: CharSequence, s2: CharSequence) => s1.toString.toLowerCase.startsWith(s2.toString.toLowerCase)
          case _                                    => false
        }

      case "$==" =>
        (v1, v2) match {
          case (s1: CharSequence, s2: CharSequence) => s1.toString.endsWith(s2.toString)
          case _                                    => false
        }

      case "$=" =>
        (v1, v2) match {
          case (s1: CharSequence, s2: CharSequence) => s1.toString.toLowerCase.endsWith(s2.toString.toLowerCase)
          case _                                    => false
        }

      case "*==" =>
        (v1, v2) match {
          case (s1: CharSequence, s2: CharSequence) => s1.toString.contains(s2)
          case _                                    => false
        }

      case "*=" =>
        (v1, v2) match {
          case (s1: CharSequence, s2: CharSequence) => s1.toString.toLowerCase.contains(s2.toString.toLowerCase)
          case _                                    => false
        }

      case ">=" =>
        (v1.asInstanceOf[AnyRef], v2.asInstanceOf[AnyRef]) match {
          case (c1: Number, c2: Number) => c1.doubleValue >= c2.doubleValue
          case _                        => false
        }

      case ">" =>
        (v1.asInstanceOf[AnyRef], v2.asInstanceOf[AnyRef]) match {
          case (c1: Number, c2: Number) => c1.doubleValue > c2.doubleValue
          case _                        => false
        }

      case "<=" =>
        (v1.asInstanceOf[AnyRef], v2.asInstanceOf[AnyRef]) match {
          case (c1: Number, c2: Number) => c1.doubleValue <= c2.doubleValue
          case _                        => false
        }

      case "<" =>
        (v1.asInstanceOf[AnyRef], v2.asInstanceOf[AnyRef]) match {
          case (c1: Number, c2: Number) => c1.doubleValue < c2.doubleValue
          case _                        => false
        }

      case "+" =>
        (v1.asInstanceOf[AnyRef], v2.asInstanceOf[AnyRef]) match {
          case (c1: Number, c2: Number) => c1.doubleValue + c2.doubleValue
          case _                        => Double.NaN
        }

      case "-" =>
        (v1.asInstanceOf[AnyRef], v2.asInstanceOf[AnyRef]) match {
          case (c1: Number, c2: Number) => c1.doubleValue - c2.doubleValue
          case _                        => Double.NaN
        }

      case "*" =>
        (v1.asInstanceOf[AnyRef], v2.asInstanceOf[AnyRef]) match {
          case (c1: Number, c2: Number) => c1.doubleValue * c2.doubleValue
          case _                        => Double.NaN
        }

      case "/" =>
        (v1.asInstanceOf[AnyRef], v2.asInstanceOf[AnyRef]) match {
          case (c1: Number, c2: Number) => c1.doubleValue / c2.doubleValue
          case _                        => Double.NaN
        }

      case "%" =>
        (v1.asInstanceOf[AnyRef], v2.asInstanceOf[AnyRef]) match {
          case (c1: Number, c2: Number) => c1.doubleValue % c2.doubleValue
          case _                        => Double.NaN
        }
    }
  }

}
