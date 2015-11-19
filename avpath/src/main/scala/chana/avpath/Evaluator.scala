package chana.avpath

import chana.avro.FromJson
import chana.avpath.Parser.ComparionExprSyntax
import chana.avpath.Parser.ConcatExprSyntax
import chana.avpath.Parser.LiteralSyntax
import chana.avpath.Parser.LogicalExprSyntax
import chana.avpath.Parser.MapKeysSyntax
import chana.avpath.Parser.MathExprSyntax
import chana.avpath.Parser.ObjPredSyntax
import chana.avpath.Parser.PathSyntax
import chana.avpath.Parser.PosPredSyntax
import chana.avpath.Parser.SelectorSyntax
import chana.avpath.Parser.SubstSyntax
import chana.avpath.Parser.Syntax
import chana.avpath.Parser.UnaryExprSyntax
import chana.avpath.Parser.PosSyntax
import java.nio.ByteBuffer
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.IndexedRecord
import scala.collection.mutable

object Evaluator {
  sealed trait Target
  final case class TargetRecord(record: IndexedRecord, field: Schema.Field) extends Target
  final case class TargetArray(array: java.util.Collection[_], idx: Int, arraySchema: Schema) extends Target
  final case class TargetMap(map: java.util.Map[String, _], key: String, mapSchema: Schema) extends Target

  private object Op {
    case object Select extends Op
    case object Update extends Op
    case object Delete extends Op
    case object Clear extends Op
    case object Insert extends Op
    case object InsertAll extends Op
  }
  private sealed trait Op

  final case class Ctx(value: Any, schema: Schema, topLevelField: Schema.Field, target: Option[Target] = None)

  def select(root: IndexedRecord, ast: PathSyntax): List[Ctx] = {
    evaluatePath(ast, List(Ctx(root, root.getSchema, null)), true)
  }

  def update(root: IndexedRecord, ast: PathSyntax, value: Any): List[Ctx] = {
    operate(root, ast, Op.Update, value, isJsonValue = false)
  }

  def updateJson(root: IndexedRecord, ast: PathSyntax, value: String): List[Ctx] = {
    operate(root, ast, Op.Update, value, isJsonValue = true)
  }

  def insert(root: IndexedRecord, ast: PathSyntax, value: Any): List[Ctx] = {
    operate(root, ast, Op.Insert, value, isJsonValue = false)
  }

  def insertJson(root: IndexedRecord, ast: PathSyntax, value: String): List[Ctx] = {
    operate(root, ast, Op.Insert, value, isJsonValue = true)
  }

  def insertAll(root: IndexedRecord, ast: PathSyntax, values: java.util.Collection[_]): List[Ctx] = {
    operate(root, ast, Op.InsertAll, values, isJsonValue = false)
  }

  def insertAllJson(root: IndexedRecord, ast: PathSyntax, values: String): List[Ctx] = {
    operate(root, ast, Op.InsertAll, values, isJsonValue = true)
  }

  def delete(root: IndexedRecord, ast: PathSyntax): List[Ctx] = {
    operate(root, ast, Op.Delete, null, false)
  }

  def clear(root: IndexedRecord, ast: PathSyntax): List[Ctx] = {
    operate(root, ast, Op.Clear, null, false)
  }

  private def targets(ctxs: List[Ctx]) = ctxs.flatMap(_.target)

  private def operate(root: IndexedRecord, ast: PathSyntax, op: Op, value: Any, isJsonValue: Boolean): List[Ctx] = {
    val ctxs = select(root, ast)

    op match {
      case Op.Select =>

      case Op.Delete =>
        val processingArrs = new mutable.HashMap[java.util.Collection[_], (Schema, List[Int])]()
        targets(ctxs) foreach {
          case _: TargetRecord => // cannot apply delete on record

          case TargetArray(arr, idx, schema) =>
            processingArrs += arr -> (schema, idx :: processingArrs.getOrElse(arr, (schema, List[Int]()))._2)

          case TargetMap(map, key, _) =>
            map.remove(key)
        }

        for ((arr, (schema, toRemoved)) <- processingArrs) {
          arrayRemove(arr, toRemoved)
        }

      case Op.Clear =>
        targets(ctxs) foreach {
          case TargetRecord(rec, field) =>
            rec.get(field.pos) match {
              case arr: java.util.Collection[_] => arr.clear
              case map: java.util.Map[_, _]     => map.clear
              case _                            => // ?
            }

          case _: TargetArray =>
          // why you be here, for Insert, you should op on record's arr field directly

          case _: TargetMap   =>
          // why you be here, for Insert, you should op on record's map field directly
        }

      case Op.Update =>
        targets(ctxs) foreach {
          case TargetRecord(rec, null) =>
            val value1 = if (isJsonValue) FromJson.fromJsonString(value.asInstanceOf[String], rec.getSchema, false) else value
            value1 match {
              case v: IndexedRecord => replace(rec, v)
              case _                => // log.error 
            }

          case TargetRecord(rec, field) =>
            val value1 = if (isJsonValue) FromJson.fromJsonString(value.asInstanceOf[String], field.schema, false) else value
            rec.put(field.pos, value1)

          case TargetArray(arr, idx, arrSchema) =>
            getElementType(arrSchema) foreach { elemSchema =>
              val value1 = if (isJsonValue) FromJson.fromJsonString(value.asInstanceOf[String], elemSchema, false) else value
              (elemSchema.getType, value1) match {
                case (Type.BOOLEAN, v: Boolean)               => arrayUpdate(arr, idx, v)
                case (Type.INT, v: Int)                       => arrayUpdate(arr, idx, v)
                case (Type.LONG, v: Long)                     => arrayUpdate(arr, idx, v)
                case (Type.FLOAT, v: Float)                   => arrayUpdate(arr, idx, v)
                case (Type.DOUBLE, v: Double)                 => arrayUpdate(arr, idx, v)
                case (Type.BYTES, v: ByteBuffer)              => arrayUpdate(arr, idx, v)
                case (Type.STRING, v: CharSequence)           => arrayUpdate(arr, idx, v)
                case (Type.RECORD, v: IndexedRecord)          => arrayUpdate(arr, idx, v)
                case (Type.ENUM, v: GenericEnumSymbol)        => arrayUpdate(arr, idx, v)
                case (Type.FIXED, v: GenericFixed)            => arrayUpdate(arr, idx, v)
                case (Type.ARRAY, v: java.util.Collection[_]) => arrayUpdate(arr, idx, v)
                case (Type.MAP, v: java.util.Map[_, _])       => arrayUpdate(arr, idx, v)
                case _                                        => // todo
              }
            }

          case TargetMap(map, key, mapSchema) =>
            getValueType(mapSchema) foreach { valueSchema =>
              val value1 = if (isJsonValue) FromJson.fromJsonString(value.asInstanceOf[String], valueSchema, false) else value
              map.asInstanceOf[java.util.Map[String, Any]].put(key, value1)
            }
        }

      case Op.Insert =>
        targets(ctxs) foreach {
          case TargetRecord(rec, field) =>
            rec.get(field.pos) match {
              case arr: java.util.Collection[_] =>
                getElementType(field.schema) foreach { elemSchema =>
                  val value1 = if (isJsonValue) FromJson.fromJsonString(value.asInstanceOf[String], elemSchema, false) else value
                  (elemSchema.getType, value1) match {
                    case (Type.BOOLEAN, v: Boolean)               => arrayInsert(arr, v)
                    case (Type.INT, v: Int)                       => arrayInsert(arr, v)
                    case (Type.LONG, v: Long)                     => arrayInsert(arr, v)
                    case (Type.FLOAT, v: Float)                   => arrayInsert(arr, v)
                    case (Type.DOUBLE, v: Double)                 => arrayInsert(arr, v)
                    case (Type.BYTES, v: ByteBuffer)              => arrayInsert(arr, v)
                    case (Type.STRING, v: CharSequence)           => arrayInsert(arr, v)
                    case (Type.RECORD, v: IndexedRecord)          => arrayInsert(arr, v)
                    case (Type.ENUM, v: GenericEnumSymbol)        => arrayInsert(arr, v)
                    case (Type.FIXED, v: GenericFixed)            => arrayInsert(arr, v)
                    case (Type.ARRAY, v: java.util.Collection[_]) => arrayInsert(arr, v)
                    case (Type.MAP, v: java.util.Map[_, _])       => arrayInsert(arr, v)
                    case _                                        => // todo
                  }
                }

              case map: java.util.Map[_, _] =>
                val value1 = if (isJsonValue) FromJson.fromJsonString(value.asInstanceOf[String], field.schema, false) else value
                value1 match {
                  case (k: String, v) =>
                    map.asInstanceOf[java.util.Map[String, Any]].put(k, v)
                  case kvs: java.util.Map[_, _] =>
                    val entries = kvs.entrySet.iterator
                    if (entries.hasNext) {
                      val entry = entries.next // should contains only one entry
                      map.asInstanceOf[java.util.Map[String, Any]].put(entry.getKey.asInstanceOf[String], entry.getValue)
                    }
                  case _ =>
                }

              case _ => // can only insert to array/map field
            }

          case _: TargetArray =>
          // why you be here, for Insert, you should op on record's arr field directly

          case _: TargetMap   =>
          // why you be here, for Insert, you should op on record's map field directly
        }

      case Op.InsertAll =>
        targets(ctxs) foreach {
          case TargetRecord(rec, field) =>
            rec.get(field.pos) match {
              case arr: java.util.Collection[_] =>
                getElementType(field.schema) foreach { elemSchema =>
                  val value1 = if (isJsonValue) FromJson.fromJsonString(value.asInstanceOf[String], field.schema, false) else value
                  value1 match {
                    case xs: java.util.Collection[_] =>
                      val itr = xs.iterator
                      while (itr.hasNext) {
                        (elemSchema.getType, itr.next) match {
                          case (Type.BOOLEAN, v: Boolean)               => arrayInsert(arr, v)
                          case (Type.INT, v: Int)                       => arrayInsert(arr, v)
                          case (Type.LONG, v: Long)                     => arrayInsert(arr, v)
                          case (Type.FLOAT, v: Float)                   => arrayInsert(arr, v)
                          case (Type.DOUBLE, v: Double)                 => arrayInsert(arr, v)
                          case (Type.BYTES, v: ByteBuffer)              => arrayInsert(arr, v)
                          case (Type.STRING, v: CharSequence)           => arrayInsert(arr, v)
                          case (Type.RECORD, v: IndexedRecord)          => arrayInsert(arr, v)
                          case (Type.ENUM, v: GenericEnumSymbol)        => arrayInsert(arr, v)
                          case (Type.FIXED, v: GenericFixed)            => arrayInsert(arr, v)
                          case (Type.ARRAY, v: java.util.Collection[_]) => arrayInsert(arr, v)
                          case (Type.MAP, v: java.util.Map[_, _])       => arrayInsert(arr, v)
                          case _                                        => // todo

                        }
                      }
                    case _ => // ?
                  }
                }

              case map: java.util.Map[_, _] =>
                val value1 = if (isJsonValue) FromJson.fromJsonString(value.asInstanceOf[String], field.schema, false) else value
                value1 match {
                  case xs: java.util.Collection[_] =>
                    val itr = xs.iterator
                    while (itr.hasNext) {
                      itr.next match {
                        case (k: String, v) => map.asInstanceOf[java.util.Map[String, Any]].put(k, v)
                        case _              => // ? 
                      }
                    }

                  case xs: java.util.Map[_, _] =>
                    val itr = xs.entrySet.iterator
                    while (itr.hasNext) {
                      val entry = itr.next
                      entry.getKey match {
                        case k: String => map.asInstanceOf[java.util.Map[String, Any]].put(k, entry.getValue)
                        case _         => // ?
                      }
                    }

                  case _ => // ?
                }
            }

          case _: TargetArray =>
          // why you be here, for Insert, you should op on record's arr field directly

          case _: TargetMap   =>
          // why you be here, for Insert, you should op on record's map field directly
        }
    }

    ctxs
  }

  private def arrayUpdate[T](arr: java.util.Collection[_], idx: Int, value: T) {
    if (idx >= 0) {
      arr match {
        case xs: java.util.List[T] @unchecked =>
          if (idx < xs.size) {
            xs.set(idx, value)
          }
        case _ =>
          val values = arr.iterator
          var i = 0
          val newTail = new java.util.ArrayList[T]()
          newTail.add(value)
          while (values.hasNext) {
            val value = values.next.asInstanceOf[T]
            if (i == idx) {
              arr.remove(value)
            } else if (i > idx) {
              arr.remove(value)
              newTail.add(value)
            }
            i += 1
          }
          arr.asInstanceOf[java.util.Collection[T]].addAll(newTail)
      }
    }
  }

  private def arrayInsert[T](arr: java.util.Collection[_], value: T) {
    arr.asInstanceOf[java.util.Collection[T]].add(value)
  }

  private def arrayRemove[T](arr: java.util.Collection[T], _toRemoved: List[Int]) {
    var toRemoved = _toRemoved.sortBy(-_)
    while (toRemoved != Nil) {
      arr.remove(toRemoved.head)
      toRemoved = toRemoved.tail
    }
  }

  private def replace(dst: IndexedRecord, src: IndexedRecord) {
    if (dst.getSchema == src.getSchema) {
      val fields = dst.getSchema.getFields.iterator
      while (fields.hasNext) {
        val field = fields.next
        val value = src.get(field.pos)
        val tpe = field.schema.getType
        if (value != null && (tpe != Type.ARRAY || !value.asInstanceOf[java.util.Collection[_]].isEmpty) && (tpe != Type.MAP || !value.asInstanceOf[java.util.Map[_, _]].isEmpty)) {
          dst.put(field.pos, value)
        }
      }
    }
  }

  private def evaluatePath(path: PathSyntax, _ctxs: List[Ctx], isTopLevel: Boolean): List[Ctx] = {

    val parts = path.parts
    val len = parts.length
    var i = 0
    var isResArray = true
    var ctxs = _ctxs
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
          case Ctx(arr: java.util.Collection[_], schema, topLevelField, _) =>
            getElementType(schema) foreach { elemType =>
              val values = arr.iterator
              var j = 0
              while (values.hasNext) {
                val value = values.next
                res ::= Ctx(value, elemType, topLevelField, Some(TargetArray(arr, j, schema)))
                j += 1
              }
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

      case Ctx(arr: java.util.Collection[_], schema, topLevelField, _) =>
        getElementType(schema) foreach { elemType =>
          val values = arr.iterator
          var i = 0
          while (values.hasNext) {
            val value = values.next
            val elemCtx = Ctx(value, elemType, topLevelField, Some(TargetArray(arr, i, schema)))
            evaluateExpr(expr.arg, elemCtx) match {
              case true => res ::= elemCtx
              case _    =>
            }
            i += 1
          }
        }

      case Ctx(map: java.util.Map[String, _] @unchecked, schema, topLevelField, _) =>
        getValueType(schema) foreach { elemType =>
          val entries = map.entrySet.iterator
          while (entries.hasNext) {
            val entry = entries.next
            val elemCtx = Ctx(entry.getValue, elemType, topLevelField, Some(TargetMap(map, entry.getKey, schema)))
            evaluateExpr(expr.arg, elemCtx) match {
              case true => res ::= elemCtx
              case _    =>
            }
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
      case currCtx @ Ctx(arr: java.util.Collection[_], schema, topLevelField, _) =>
        getElementType(schema) foreach { elemType =>
          posExpr match {
            case PosSyntax(LiteralSyntax("*"), _, _) =>
              val values = arr.iterator
              val n = arr.size
              val elems = Array.ofDim[Ctx](n)
              var i = 0
              while (values.hasNext) {
                val value = values.next
                elems(i) = Ctx(value, elemType, topLevelField, Some(TargetArray(arr, i, schema)))
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
                  res ::= Left(Ctx(value, elemType, topLevelField, Some(TargetArray(arr, ix, schema))))
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
                            elems(i - from) = Ctx(value, elemType, topLevelField, Some(TargetArray(arr, i, schema)))
                            i += 1
                          }
                        case _ =>
                          val values = arr.iterator
                          var i = 0
                          while (values.hasNext && i <= to) {
                            val value = values.next
                            if (i >= from) {
                              elems(i) = Ctx(value, elemType, topLevelField, Some(TargetArray(arr, i, schema)))
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
                        elems(i - from) = Ctx(xs.get(i), elemType, topLevelField, Some(TargetArray(arr, i, schema)))
                        i += 1
                      }
                    case _ =>
                      val values = arr.iterator
                      var i = 0
                      while (values.hasNext && i < n) {
                        val value = values.next
                        if (i >= from) {
                          elems(i - from) = Ctx(value, elemType, topLevelField, Some(TargetArray(arr, i, schema)))
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
                        elems(i) = Ctx(xs.get(i), elemType, topLevelField, Some(TargetArray(arr, i, schema)))
                        i += 1
                      }
                    case _ =>
                      val values = arr.iterator
                      var i = 0
                      while (values.hasNext && i < n) {
                        val value = values.next
                        if (i <= to) {
                          elems(i) = Ctx(value, elemType, topLevelField, Some(TargetArray(arr, i, schema)))
                        }
                        i += 1
                      }
                  }
                  res ::= Right(elems)

                case _ =>
              }

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
      case Ctx(map: java.util.Map[String, _] @unchecked, schema, topLevelField, _) =>
        getValueType(schema) foreach { valueSchema =>
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

  /**
   * For array
   */
  private def getElementType(schema: Schema): Option[Schema] = {
    schema.getType match {
      case Type.ARRAY =>
        Option(schema.getElementType)
      case Type.UNION =>
        val unions = schema.getTypes.iterator
        var res: Option[Schema] = None
        while (unions.hasNext && res == None) {
          val union = unions.next
          if (union.getType == Type.ARRAY) {
            res = Some(union.getElementType)
          }
        }
        res
      case _ => None
    }
  }

  /**
   * For map
   */
  private def getValueType(schema: Schema): Option[Schema] = {
    schema.getType match {
      case Type.MAP =>
        Option(schema.getValueType)
      case Type.UNION =>
        val unions = schema.getTypes.iterator
        var res: Option[Schema] = None
        while (unions.hasNext && res == None) {
          val union = unions.next
          if (union.getType == Type.MAP) {
            res = Some(union.getValueType)
          }
        }
        res
      case _ => None
    }
  }
}
