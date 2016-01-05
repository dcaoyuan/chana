package chana.xpath

import chana.avro
import chana.avro.Changelog
import chana.avro.Clearlog
import chana.avro.Deletelog
import chana.avro.Insertlog
import chana.avro.UpdateAction
import chana.xpath.nodes._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.IndexedRecord
import scala.collection.mutable

final case class XPathRuntimeException(value: Any, message: String) extends RuntimeException(
  value + " " + message + ". " + value + "'s type is: " + (value match {
    case null      => null
    case x: AnyRef => x.getClass.getName
    case _         => "primary type."
  }))

/**
 * XPath elements cases:
 *   record
 *   record's field
 *   map entry
 *   map entry's value
 *   map entry's value's field
 *   array element
 *   array element's field
 */
sealed trait AvroNode[T] {
  def underlying: T
  def schema: Schema

  private var _xpath = ""
  /**
   * xpath till underlying
   */
  def xpath = _xpath
  def xpath(x: String): this.type = {
    _xpath = x
    this
  }

  private var _field: Option[Schema.Field] = None
  def field = _field
  def field(x: Option[Schema.Field]): this.type = {
    _field = x
    this
  }
}

sealed trait CollectionNode[T, K] extends AvroNode[T] {
  private var _keys: java.util.Collection[K] = null
  def keys: java.util.Collection[K] = _keys
  def keys(xs: java.util.Collection[_]): this.type = {
    _keys = xs.asInstanceOf[java.util.Collection[K]]
    this
  }

}

object RecordNode {
  def apply(record: IndexedRecord, field: Option[Schema.Field]) =
    new RecordNode(record).field(field)

  def unapply(x: RecordNode): Option[(IndexedRecord, Option[Schema.Field])] =
    Some((x.underlying, x.field))
}
final class RecordNode private (val underlying: IndexedRecord) extends AvroNode[IndexedRecord] {
  def schema = underlying.getSchema

  override def toString() = {
    new StringBuilder().append("RecordNode(")
      .append(underlying).append(",")
      .append(field).append(")")
      .toString
  }
}

object ArrayNode {
  def apply(array: java.util.Collection[_], arraySchema: Schema, idxes: java.util.Collection[Int], field: Option[Schema.Field]) =
    new ArrayNode(array, arraySchema).keys(idxes).field(field)

  def unapply(x: ArrayNode): Option[(java.util.Collection[_], Schema, java.util.Collection[Int], Option[Schema.Field])] =
    Some((x.underlying, x.schema, x.keys, x.field))
}
final class ArrayNode private (val underlying: java.util.Collection[_], val schema: Schema) extends CollectionNode[java.util.Collection[_], Int] {
  override def toString() = {
    new StringBuilder().append("ArrayNode(")
      .append(underlying).append(",")
      .append(schema).append(",")
      .append(keys).append(",")
      .append(field).append(")")
      .toString
  }
}

object MapNode {
  def apply(map: java.util.Map[String, _], mapSchema: Schema, keys: java.util.Collection[String], field: Option[Schema.Field]) =
    new MapNode(map, mapSchema).keys(keys).field(field)

  def unapply(x: MapNode): Option[(java.util.Map[String, _], Schema, java.util.Collection[String], Option[Schema.Field])] =
    Some((x.underlying, x.schema, x.keys, x.field))
}
final class MapNode private (val underlying: java.util.Map[String, _], val schema: Schema) extends CollectionNode[java.util.Map[String, _], String] {
  override def toString() = {
    new StringBuilder().append("MapNode(")
      .append(underlying).append(",")
      .append(schema).append(",")
      .append(keys).append(",")
      .append(field).append(")")
      .toString
  }
}

object Context {
  def apply(schema: Schema, target: Any, node: AvroNode[_], value: Any) = new Context(schema, target, node).value(value)
  def unapply(x: Context): Option[(Schema, Any, AvroNode[_])] = Some((x.schema, x.target, x.node))
}
/**
 * Context during evaluating.
 *
 * @param schema  the schema of target
 * @param target  current value/values to be applied on node test
 * @param node    the owner node(or this node's field) of target
 *
 * value the value during evaluating, vairable during this context
 */
final class Context private (val schema: Schema, val target: Any, val node: AvroNode[_]) {
  /**
   * value during evaluating, vairable during this context
   */
  private var _value: Any = _
  def value = _value
  def value(x: Any) = {
    _value = x
    this
  }

  def copy = Context(schema, target, node, value)

  override def toString() = {
    new StringBuilder().append("Context(")
      .append(schema).append(",")
      .append(target).append(",")
      .append(node).append(",")
      .append(value).append(")")
      .toString
  }
}

object XPathEvaluator {

  sealed trait ValueFormat
  case object Value extends ValueFormat
  case object Json extends ValueFormat
  case object Avro extends ValueFormat

  def select(record: IndexedRecord, xpath: Expr): List[Context] = {
    val ctx = Context(record.getSchema, record, RecordNode(record, None), null)
    expr(xpath.expr, xpath.exprs, ctx)
  }

  def update(record: IndexedRecord, xpath: Expr, value: Any): List[UpdateAction] = {
    val ctxs = select(record, xpath)
    opUpdate(ctxs.head, value, Value)
  }

  def updateJson(record: IndexedRecord, xpath: Expr, value: String): List[UpdateAction] = {
    val ctxs = select(record, xpath)
    opUpdate(ctxs.head, value, Json)
  }

  def updateAvro(record: IndexedRecord, xpath: Expr, value: Array[Byte]): List[UpdateAction] = {
    val ctxs = select(record, xpath)
    opUpdate(ctxs.head, value, Avro)
  }

  def insert(record: IndexedRecord, xpath: Expr, value: Any): List[UpdateAction] = {
    val ctxs = select(record, xpath)
    opInsert(ctxs.head, value, Value)
  }

  def insertJson(record: IndexedRecord, xpath: Expr, value: String): List[UpdateAction] = {
    val ctxs = select(record, xpath)
    opInsert(ctxs.head, value, Json)
  }

  def insertAvro(record: IndexedRecord, xpath: Expr, value: Array[Byte]): List[UpdateAction] = {
    val ctxs = select(record, xpath)
    opInsert(ctxs.head, value, Avro)
  }

  def insertAll(record: IndexedRecord, xpath: Expr, values: java.util.Collection[_]): List[UpdateAction] = {
    val ctxs = select(record, xpath)
    opInsertAll(ctxs.head, values, Value)
  }

  def insertAllJson(record: IndexedRecord, xpath: Expr, values: String): List[UpdateAction] = {
    val ctxs = select(record, xpath)
    opInsertAll(ctxs.head, values, Json)
  }

  def insertAllAvro(record: IndexedRecord, xpath: Expr, values: Array[Byte]): List[UpdateAction] = {
    val ctxs = select(record, xpath)
    opInsertAll(ctxs.head, values, Avro)
  }

  def delete(record: IndexedRecord, xpath: Expr): List[UpdateAction] = {
    val ctxs = select(record, xpath)
    opDelete(ctxs.head)
  }

  def clear(record: IndexedRecord, xpath: Expr): List[UpdateAction] = {
    val ctxs = select(record, xpath)
    opClear(ctxs.head)
  }

  private def opUpdate(ctx: Context, value: Any, format: ValueFormat): List[UpdateAction] = {
    var actions = List[UpdateAction]()

    ctx.node match {
      case n @ RecordNode(rec, None) =>
        val value1 = format match {
          case Json  => avro.jsonDecode(value.asInstanceOf[String], rec.getSchema).get
          case Avro  => avro.avroDecode[Any](value.asInstanceOf[Array[Byte]], rec.getSchema).get
          case Value => value
        }
        value1 match {
          case v: IndexedRecord =>
            val prev = new GenericData.Record(rec.asInstanceOf[GenericData.Record], true)
            val rlback = { () => avro.replace(rec, prev) }
            val commit = { () => avro.replace(rec, v) }
            val xpath = n.xpath match {
              case "" => "/"
              case x  => x
            }
            val bytes = if (format == Avro) value.asInstanceOf[Array[Byte]] else avro.avroEncode(value1, rec.getSchema).get
            actions ::= UpdateAction(commit, rlback, Changelog(xpath, value1, bytes))
          case _ => // log.error 
        }

      case n @ RecordNode(rec, Some(field)) =>
        val value1 = format match {
          case Json  => avro.jsonDecode(value.asInstanceOf[String], field.schema).get
          case Avro  => avro.avroDecode[Any](value.asInstanceOf[Array[Byte]], field.schema).get
          case Value => value
        }

        val prev = rec.get(field.pos)
        val rlback = { () => rec.put(field.pos, prev) }
        val commit = { () => rec.put(field.pos, value1) }
        val xpath = n.xpath + "/" + field.name
        val bytes = if (format == Avro) value.asInstanceOf[Array[Byte]] else avro.avroEncode(value1, field.schema).get
        actions ::= UpdateAction(commit, rlback, Changelog(xpath, value1, bytes))

      case n @ ArrayNode(arr: java.util.Collection[Any], arrSchema, idxes, field) =>

        //println(ctx.node)
        val itr = idxes.iterator

        while (itr.hasNext) {
          val idx = itr.next
          val ix = idx - 1
          val target = avro.arraySelect(arr, ix)
          field match {
            case Some(f) =>
              val value1 = format match {
                case Json  => avro.jsonDecode(value.asInstanceOf[String], f.schema).get
                case Avro  => avro.avroDecode[Any](value.asInstanceOf[Array[Byte]], f.schema).get
                case Value => value
              }

              val rec = target.asInstanceOf[IndexedRecord]
              val prev = rec.get(f.pos)
              val rlback = { () => rec.put(f.pos, prev) }
              val commit = { () => rec.put(f.pos, value1) }
              val xpath = n.xpath + "[" + idx + "]/" + f.name
              val bytes = if (format == Avro) value.asInstanceOf[Array[Byte]] else avro.avroEncode(value1, f.schema).get
              actions ::= UpdateAction(commit, rlback, Changelog(xpath, value1, bytes))
            case None =>
              val elemSchema = avro.getElementType(arrSchema)
              val value1 = format match {
                case Json  => avro.jsonDecode(value.asInstanceOf[String], elemSchema).get
                case Avro  => avro.avroDecode[Any](value.asInstanceOf[Array[Byte]], elemSchema).get
                case Value => value
              }

              val rlback = { () => avro.arrayUpdate(arr, ix, target) }
              val commit = { () => avro.arrayUpdate(arr, ix, value1) }
              val xpath = n.xpath + "[" + idx + "]"
              val bytes = if (format == Avro) value.asInstanceOf[Array[Byte]] else avro.avroEncode(value1, elemSchema).get
              actions ::= UpdateAction(commit, rlback, Changelog(xpath, value1, bytes))
          }
        }

      case n @ MapNode(map: java.util.Map[String, Any], mapSchema, keys, field) =>

        //println(ctx.node)
        val itr = keys.iterator

        while (itr.hasNext) {
          val key = itr.next
          val target = map.get(key)
          field match {
            case Some(f) =>
              val value1 = format match {
                case Json  => avro.jsonDecode(value.asInstanceOf[String], f.schema).get
                case Avro  => avro.avroDecode[Any](value.asInstanceOf[Array[Byte]], f.schema).get
                case Value => value
              }

              val rec = target.asInstanceOf[IndexedRecord]
              val prev = rec.get(f.pos)
              val rlback = { () => rec.put(f.pos, prev) }
              val commit = { () => rec.put(f.pos, value1) }
              val xpath = n.xpath + "/@" + key + "/" + f.name
              val bytes = if (format == Avro) value.asInstanceOf[Array[Byte]] else avro.avroEncode(value1, f.schema).get
              actions ::= UpdateAction(commit, rlback, Changelog(xpath, value1, bytes))
            case None =>
              val valueSchema = avro.getValueType(mapSchema)
              val value1 = format match {
                case Json  => avro.jsonDecode(value.asInstanceOf[String], valueSchema).get
                case Avro  => avro.avroDecode[Any](value.asInstanceOf[Array[Byte]], valueSchema).get
                case Value => value
              }

              val rlback = { () => map.put(key, target) }
              val commit = { () => map.put(key, value1) }
              val xpath = n.xpath + "/@" + key
              val bytes = if (format == Avro) value.asInstanceOf[Array[Byte]] else avro.avroEncode(value1, valueSchema).get
              actions ::= UpdateAction(commit, rlback, Changelog(xpath, value1, bytes))

          }
        }
    }
    actions.reverse
  }

  private def opInsert(ctx: Context, value: Any, format: ValueFormat): List[UpdateAction] = {
    var actions = List[UpdateAction]()

    ctx.node match {
      case n @ RecordNode(rec, Some(field)) =>
        rec.get(field.pos) match {
          case arr: java.util.Collection[Any] @unchecked =>
            val elemSchema = avro.getElementType(field.schema)
            val value1 = format match {
              case Json  => avro.jsonDecode(value.asInstanceOf[String], elemSchema).get
              case Avro  => avro.avroDecode[Any](value.asInstanceOf[Array[Byte]], elemSchema).get
              case Value => value
            }

            // There should be only one element
            val rlback = { () => arr.remove(value1) }
            val commit = { () => arr.add(value1) }
            // always convert to a collection inserting
            val vs = java.util.Arrays.asList(value1)
            val xpath = n.xpath + "/" + field.name
            val bytes = avro.avroEncode(vs, field.schema).get
            actions ::= UpdateAction(commit, rlback, Insertlog(xpath, vs, bytes))

          case map: java.util.Map[String, Any] @unchecked =>
            val valueSchema = avro.getValueType(field.schema)
            val value1 = format match {
              case Json  => avro.jsonDecode(value.asInstanceOf[String], field.schema).get
              case Avro  => avro.avroDecode[Any](value.asInstanceOf[Array[Byte]], field.schema).get
              case Value => value
            }
            value1 match {
              case (k: String, v) =>
                val prev = map.get(k)
                val rlback = { () => map.put(k, prev) }
                val commit = { () => map.put(k, v) }
                // always convert to a collection inserting
                val kvs = new java.util.HashMap[String, Any](1)
                kvs.put(k, v)
                val xpath = n.xpath + "/" + field.name
                val bytes = avro.avroEncode(kvs, field.schema).get
                actions ::= UpdateAction(commit, rlback, Insertlog(xpath, v, bytes))

              case kvs: java.util.Map[String, _] @unchecked =>
                val entries = kvs.entrySet.iterator
                // should contain only one entry
                if (entries.hasNext) {
                  val entry = entries.next
                  val k = entry.getKey
                  val v = entry.getValue

                  val prev = map.get(k)
                  val rlback = { () => map.put(entry.getKey, prev) }
                  val commit = { () => map.put(entry.getKey, v) }
                  // always convert to a collection inserting
                  val xpath = n.xpath + "/" + field.name
                  val bytes = avro.avroEncode(kvs, field.schema).get
                  actions ::= UpdateAction(commit, rlback, Insertlog(xpath, kvs, bytes))
                }

              case _ =>
            }

          case _ => // can only insert to array/map field
        }

      case _: ArrayNode =>
      // why are you here, for Insert, you should op on record's arr field directly

      case _: MapNode   =>
      // why are you here, for Insert, you should op on record's map field directly
    }

    actions.reverse
  }

  private def opInsertAll(ctx: Context, value: Any, format: ValueFormat): List[UpdateAction] = {
    var actions = List[UpdateAction]()

    ctx.node match {
      case n @ RecordNode(rec, Some(field)) =>
        rec.get(field.pos) match {
          case arr: java.util.Collection[Any] @unchecked =>
            val value1 = format match {
              case Json  => avro.jsonDecode(value.asInstanceOf[String], field.schema).get
              case Avro  => avro.avroDecode[Any](value.asInstanceOf[Array[Byte]], field.schema).get
              case Value => value
            }
            value1 match {
              case xs: java.util.Collection[Any] @unchecked =>
                val rlback = { () => arr.removeAll(xs) }
                val commit = { () => arr.addAll(xs) }
                val xpath = n.xpath + "/" + field.name
                val bytes = if (format == Avro) value.asInstanceOf[Array[Byte]] else avro.avroEncode(xs, field.schema).get
                actions ::= UpdateAction(commit, rlback, Insertlog(xpath, xs, bytes))

              case _ => // ?
            }

          case map: java.util.Map[String, Any] @unchecked =>
            val value1 = format match {
              case Json  => avro.jsonDecode(value.asInstanceOf[String], field.schema).get
              case Avro  => avro.avroDecode[Any](value.asInstanceOf[Array[Byte]], field.schema).get
              case Value => value
            }
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
                val xpath = n.xpath + "/" + field.name
                val bytes = if (format == Avro) value.asInstanceOf[Array[Byte]] else avro.avroEncode(toPut, field.schema).get
                actions ::= UpdateAction(commit, rlback, Insertlog(xpath, toPut, bytes))

              case xs: java.util.Map[String, Any] @unchecked =>
                val prev = new java.util.HashMap[String, Any]()
                val itr = xs.entrySet.iterator

                while (itr.hasNext) {
                  val entry = itr.next
                  val k = entry.getKey
                  val v = entry.getValue
                  prev.put(k, v)
                }
                val rlback = { () => map.putAll(prev) }
                val commit = { () => map.putAll(xs) }
                val xpath = n.xpath + "/" + field.name
                val bytes = if (format == Avro) value.asInstanceOf[Array[Byte]] else avro.avroEncode(xs, field.schema).get
                actions ::= UpdateAction(commit, rlback, Insertlog(xpath, xs, bytes))

              case _ => // ?
            }
        }

      case _: ArrayNode =>
      // why are you here, for Insert, you should op on record's arr field directly

      case _: MapNode   =>
      // why are you here, for Insert, you should op on record's map field directly
    }

    actions.reverse
  }

  private def opDelete(ctx: Context): List[UpdateAction] = {
    var actions = List[UpdateAction]()

    ctx.node match {
      case _: RecordNode => // cannot apply delete on record

      case n @ ArrayNode(arr: java.util.Collection[Any], arrSchema, idxes, field) =>
        val descendingIdxes = new java.util.ArrayList(idxes)
        java.util.Collections.sort[Int](descendingIdxes, java.util.Collections.reverseOrder())

        // toRlback will be asc sorted
        var prev = descendingIdxes.iterator
        var toRemove = List[Int]()
        var toRlback = List[(Int, Any)]()
        var i = 0
        var arrItr = arr.iterator

        while (prev.hasNext) {
          val ix = prev.next - 1

          while (arrItr.hasNext && i <= ix) {
            if (i == ix) {
              toRemove ::= i
              toRlback ::= (i, arrItr.next)
            } else {
              arrItr.next
              i += 1
            }
          }
        }

        val rlback = { () => avro.arrayInsert(arr, toRlback) }
        val commit = { () => avro.arrayRemove(arr, toRemove) }
        val xpath = n.xpath
        // in case of delete on array, Deletelog just remember the idxes.
        actions ::= UpdateAction(commit, rlback, Deletelog(xpath, descendingIdxes))

      case n @ MapNode(map: java.util.Map[String, Any], mapSchema, keys, field) =>
        val prev = new java.util.HashMap[String, Any]()
        val itr = keys.iterator

        while (itr.hasNext) {
          val k = itr.next
          val v = map.get(k)
          prev.put(k, v)
        }
        val rlback = { () => map.putAll(prev) }
        val commit = { () => map.keySet.removeAll(keys) }
        val xpath = n.xpath
        // in case of delete on map , Deletelog just remember the keys.
        actions ::= UpdateAction(commit, rlback, Deletelog(xpath, keys))
    }

    actions.reverse
  }

  private def opClear(ctx: Context): List[UpdateAction] = {
    var actions = List[UpdateAction]()

    ctx.node match {
      case n @ RecordNode(rec, Some(field)) =>
        rec.get(field.pos) match {
          case arr: java.util.Collection[_] =>
            val prev = new java.util.ArrayList(arr)
            val rlback = { () => arr.addAll(prev) }
            val commit = { () => arr.clear }
            val xpath = n.xpath + "/" + field.name
            actions ::= UpdateAction(commit, rlback, Clearlog(xpath))

          case map: java.util.Map[String, Any] @unchecked =>
            val prev = new java.util.HashMap[String, Any](map)
            val rlback = { () => map.putAll(prev) }
            val commit = { () => map.clear }
            val xpath = n.xpath + "/" + field.name
            actions ::= UpdateAction(commit, rlback, Clearlog(xpath))

          case _ => // ?
        }

      case _: ArrayNode =>
      // why are you here, for Insert, you should op on record's arr field directly

      case _: MapNode   =>
      // why are you here, for Insert, you should op on record's map field directly
    }

    actions.reverse
  }

  // -------------------------------------------------------------------------

  private def _applyPredicateList(preds: List[Context], targetCtx: Context): Context = {
    //println("ctx: " + targetCtx.node + " in predicates: " + preds)
    if (preds.nonEmpty) {
      val applieds = preds map { pred => _applyPredicate(pred, targetCtx) }
      applieds.head // TODO process multiple predicates
    } else {
      targetCtx
    }
  }

  private def _applyPredicate(pred: Context, targetCtx: Context): Context = {
    targetCtx.schema.getType match {
      case Schema.Type.ARRAY =>
        pred.value match {
          case ix: Number =>
            val node = _checkIfApplyOnCollectionField(targetCtx)
            val origKeys = node.keys.iterator

            val arr = targetCtx.target.asInstanceOf[java.util.Collection[Any]].iterator

            val values = new java.util.ArrayList[Any]()
            val keys = new java.util.ArrayList[Any]()
            val idx = ix.intValue
            var i = 1

            while (arr.hasNext && i <= idx) {
              val value = arr.next
              val key = origKeys.next
              if (i == idx) {
                values.add(value)
                keys.add(key)
              }
              i += 1
            }

            //println(ctx.target + ", " + node.keys + ", ix:" + ix + ", elems: " + elems)
            Context(targetCtx.schema, values, node.keys(keys), values)

          case boolOrInts: java.util.Collection[_] @unchecked =>
            val node = _checkIfApplyOnCollectionField(targetCtx)
            val origKeys = node.keys.iterator

            val arr = targetCtx.target.asInstanceOf[java.util.Collection[Any]].iterator

            val values = new java.util.ArrayList[Any]()
            val keys = new java.util.ArrayList[Any]()
            var i = 0
            val conds = boolOrInts.iterator

            while (arr.hasNext && conds.hasNext) {
              val value = arr.next
              val key = origKeys.next
              conds.next match {
                case cond: java.lang.Boolean =>
                  if (cond) {
                    values.add(value)
                    keys.add(key)
                  }
                case cond: Number =>
                  if (i == cond.intValue) {
                    values.add(value)
                    keys.add(key)
                  }
              }
              i += 1
            }

            //println(ctx.target + ", " + node.keys + ", elems: " + elems)
            Context(targetCtx.schema, values, node.keys(keys), values)

          case _ =>
            targetCtx // TODO
        }

      case Schema.Type.MAP => // TODO
        pred.value match {
          case bools: java.util.Collection[Boolean] @unchecked =>
            val node = _checkIfApplyOnCollectionField(targetCtx)
            val origKeys = node.keys.iterator

            val map = targetCtx.target.asInstanceOf[java.util.Map[String, Any]].entrySet.iterator

            val values = new java.util.HashMap[String, Any]()
            val keys = new java.util.ArrayList[Any]()
            val conds = bools.iterator

            while (map.hasNext && conds.hasNext) {
              val entry = map.next
              val key = origKeys.next
              val cond = conds.next
              if (cond) {
                values.put(entry.getKey, entry.getValue)
                keys.add(key)
              }
            }

            //println(ctx.target + ", " + node.keys + ", entries: " + entries)
            Context(targetCtx.schema, values, node.keys(keys), values)

          case _ =>
            targetCtx // TODO
        }

      case tpe =>
        //println("value: " + ctx.target + ", pred: " + pred)
        pred.value match {
          case x: Boolean => if (x) targetCtx else Context(targetCtx.schema, (), targetCtx.node, ())
          case _          => targetCtx
        }
    }

  }

  /**
   * When apply filtering on record's collection field, we know that the
   * node should be transfered to a CollectionNode now.
   */
  private def _checkIfApplyOnCollectionField(ctx: Context): CollectionNode[_, _] = {
    ctx.node match {
      case n: RecordNode =>
        ctx.target match {
          case arr: java.util.Collection[Any] @unchecked =>
            val itr = arr.iterator
            val keys = new java.util.ArrayList[Int]()
            var i = 1

            while (itr.hasNext) {
              itr.next
              keys.add(i)
              i += 1
            }
            val xpath = n.field match {
              case Some(f) => n.xpath + "/" + f.name
              case None    => n.xpath
            }

            ArrayNode(arr, ctx.schema, keys, None).xpath(xpath)

          case map: java.util.Map[String, Any] @unchecked =>
            val xpath = n.field match {
              case Some(f) => n.xpath + "/" + f.name
              case None    => n.xpath
            }

            MapNode(map, ctx.schema, map.keySet, None).xpath(xpath)
        }
      case x: CollectionNode[_, _] => x
    }
  }

  /**
   * Node: Always convert selected collection result to scala list except which
   * is selected by exactly the field name.
   */
  private def _ctxOfNameTest(axis: Axis, testResult: Any, ctx: Context): Context = {
    testResult match {
      case URIQualifiedName(uri, name) => ctx
      case PrefixedName(prefix, local) => ctx
      case UnprefixedName(local) =>
        val schema = ctx.schema

        axis match {
          case Child =>
            ctx.target match {
              case rec: IndexedRecord =>
                val field = avro.getNonNull(schema).getField(local)
                val fieldSchema = avro.getNonNull(field.schema)
                val value = rec.get(field.pos)

                val node = ctx.node match {
                  case n: RecordNode =>
                    val xpath = n.field match {
                      case Some(f) => n.xpath + "/" + f.name
                      case None    => n.xpath
                    }
                    RecordNode(rec, Some(field)).xpath(xpath)
                  case n: MapNode =>
                    RecordNode(rec, Some(field)).xpath(n.xpath)
                  case n: ArrayNode =>
                    RecordNode(rec, Some(field)).xpath(n.xpath)
                }

                Context(fieldSchema, value, node, value)

              case arr: java.util.Collection[IndexedRecord] @unchecked =>
                // we'll transfer Node to record's ArrayNode
                val values = new java.util.ArrayList[Any]()
                val elemSchema = avro.getElementType(schema)
                val field = elemSchema.getField(local)
                val fieldSchema = avro.getNonNull(field.schema)
                val itr = arr.iterator

                while (itr.hasNext) {
                  val elem = itr.next
                  val value = elem.get(field.pos)
                  values.add(value)
                }

                val node = ctx.node match {
                  case n: RecordNode =>
                    val itr = arr.iterator
                    val keys = new java.util.ArrayList[Int]()
                    var i = 1

                    while (itr.hasNext) {
                      itr.next
                      keys.add(i)
                      i += 1
                    }
                    val xpath = n.field match {
                      case Some(f) => n.xpath + "/" + f.name
                      case None    => n.xpath
                    }
                    ArrayNode(arr, ctx.schema, keys, Some(field)).xpath(xpath)

                  case n: CollectionNode[_, _] => n.field(Some(field))
                }

                Context(Schema.createArray(fieldSchema), values, node, values)

              case map: java.util.Map[String, IndexedRecord] @unchecked =>
                val values = new java.util.ArrayList[Any]()
                val valueSchema = avro.getValueType(schema)
                val field = valueSchema.getField(local)
                val fieldSchema = avro.getNonNull(field.schema)
                val itr = map.entrySet.iterator

                while (itr.hasNext) {
                  val entry = itr.next
                  val value = entry.getValue.get(field.pos)
                  values.add(entry.getValue.get(field.pos))
                }

                // we'll transfer Node to record's MapNode
                val node = ctx.node match {
                  case n: RecordNode =>
                    val xpath = n.field match {
                      case Some(f) => n.xpath + "/" + f.name
                      case None    => n.xpath
                    }
                    MapNode(map, ctx.schema, map.keySet, Some(field)).xpath(xpath)

                  case n: CollectionNode[_, _] => n.field(Some(field))
                }

                Context(Schema.createArray(fieldSchema), values, node, values)

              case x => // what happens?
                throw new XPathRuntimeException(x, "try to get child: " + local)
            }

          case Attribute =>
            ctx.target match {
              case map: java.util.Map[String, Any] @unchecked =>
                // ok we're fetching a map value via key
                val values = new java.util.ArrayList[Any]()
                val keys = new java.util.ArrayList[String]()
                val valueSchema = avro.getValueType(schema)
                val value = map.get(local)
                values.add(value)
                keys.add(local)

                val node = ctx.node match {
                  case n: RecordNode =>
                    val xpath = n.field match {
                      case Some(f) => n.xpath + "/" + f.name
                      case None    => n.xpath
                    }
                    MapNode(map, schema, keys, None).xpath(xpath)
                  case n: CollectionNode[_, _] => n.keys(keys)
                }

                // when select upon collection field (map/array), we'll always return collection
                Context(Schema.createArray(valueSchema), values, node, values)

              case x => // what happens?
                throw new XPathRuntimeException(x, "try to get attribute of: " + local)
            }

        }

      case Aster =>
        val schema = ctx.schema

        axis match {
          case Child => ctx // TODO

          case Attribute =>
            //println("target: ", target)
            ctx.target match {
              case map: java.util.Map[String, Any] @unchecked =>
                // ok we're fetching all map values
                val valueSchema = avro.getValueType(schema)
                val values = map.values
                val keys = map.keySet

                val node = ctx.node match {
                  case n: RecordNode =>
                    val xpath = n.field match {
                      case Some(f) => n.xpath + "/" + f.name
                      case None    => n.xpath
                    }
                    MapNode(map, schema, keys, None).xpath(xpath)
                  case n: CollectionNode[_, _] => n.keys(keys)
                }

                // we convert values to an avro array, since it's selected result
                Context(Schema.createArray(valueSchema), values, node, values)

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

  // -------------------------------------------------------------------------

  def paramList(param0: Param, params: List[Param], ctx: Context): List[Any] = {
    param0 :: params map { x => param(x.name, x.typeDecl, ctx) }
  }

  def param(name: EQName, typeDecl: Option[TypeDeclaration], ctx: Context) = {
    // name
    typeDecl map { x => typeDeclaration(x.asType, ctx) }
  }

  def functionBody(expr: EnclosedExpr, ctx: Context) = {
    enclosedExpr(expr.expr, ctx)
  }

  def enclosedExpr(_expr: Expr, ctx: Context) = {
    expr(_expr.expr, _expr.exprs, ctx)
  }

  // TODO should we support multiple exprSingles? which brings List values
  def expr(expr: ExprSingle, exprs: List[ExprSingle], ctx: Context): List[Context] = {
    expr :: exprs map { exprSingle(_, ctx) }
  }

  def exprSingle(expr: ExprSingle, ctx: Context): Context = {
    expr match {
      case ForExpr(forClause, returnExpr)                         => forExpr(forClause, returnExpr, ctx)
      case LetExpr(letClause, returnExpr)                         => letExpr(letClause, returnExpr, ctx)
      case QuantifiedExpr(isEvery, varExpr, varExprs, statisExpr) => quantifiedExpr(isEvery, varExpr, varExprs, statisExpr, ctx)
      case IfExpr(_ifExpr, thenExpr, elseExpr)                    => ifExpr(_ifExpr, thenExpr, elseExpr, ctx)
      case OrExpr(andExpr, andExprs)                              => orExpr(andExpr, andExprs, ctx)
    }
  }

  def forExpr(forClause: SimpleForClause, returnExpr: ExprSingle, ctx: Context) = {
    simpleForClause(forClause.binding, forClause.bindings, ctx)
    exprSingle(returnExpr, ctx)
  }

  def simpleForClause(binding: SimpleForBinding, bindings: List[SimpleForBinding], ctx: Context) = {
    binding :: bindings map { x => simpleForBinding(x.varName, x.inExpr, ctx) }
  }

  def simpleForBinding(_varName: VarName, inExpr: ExprSingle, ctx: Context) = {
    varName(_varName.eqName, ctx)
    exprSingle(inExpr, ctx)
  }

  def letExpr(letClause: SimpleLetClause, returnExpr: ExprSingle, ctx: Context) = {
    simpleLetClause(letClause.binding, letClause.bindings, ctx)
    exprSingle(returnExpr, ctx)
  }

  def simpleLetClause(binding: SimpleLetBinding, bindings: List[SimpleLetBinding], ctx: Context) = {
    binding :: bindings map { x => simpleLetBinding(x.varName, x.boundTo, ctx) }
  }

  def simpleLetBinding(_varName: VarName, boundTo: ExprSingle, ctx: Context) = {
    varName(_varName.eqName, ctx)
    exprSingle(boundTo, ctx)
  }

  def quantifiedExpr(isEvery: Boolean, varExpr: VarInExprSingle, varExprs: List[VarInExprSingle], statisExpr: ExprSingle, ctx: Context) = {
    varExpr :: varExprs map { x => varInExprSingle(x.varName, x.inExpr, ctx) }
    exprSingle(statisExpr, ctx)
  }

  def varInExprSingle(_varName: VarName, inExpr: ExprSingle, ctx: Context) = {
    varName(_varName.eqName, ctx)
    exprSingle(inExpr, ctx)
  }

  def ifExpr(ifExpr: Expr, thenExpr: ExprSingle, elseExpr: ExprSingle, ctx: Context) = {
    expr(ifExpr.expr, ifExpr.exprs, ctx)
    exprSingle(thenExpr, ctx)
    exprSingle(elseExpr, ctx)
  }

  def orExpr(_andExpr: AndExpr, andExprs: List[AndExpr], ctx: Context): Context = {
    val ctx1 = andExpr(_andExpr.compExpr, _andExpr.compExprs, ctx)
    val value0 = ctx1.value

    val ctxs = andExprs map { x => andExpr(x.compExpr, x.compExprs, ctx) }
    val value1 = ctxs.foldLeft(value0) { (acc, x) => XPathFunctions.or(acc, x.value) }
    ctx1.value(value1)
  }

  def andExpr(compExpr: ComparisonExpr, compExprs: List[ComparisonExpr], ctx: Context): Context = {
    val ctx1 = comparisonExpr(compExpr.concExpr, compExpr.compExprPostfix, ctx)
    val value0 = ctx1.value

    val ctxs = compExprs map { x => comparisonExpr(x.concExpr, x.compExprPostfix, ctx) }
    val value1 = ctxs.foldLeft(value0) { (acc, x) => XPathFunctions.and(acc, x.value) }
    ctx1.value(value1)
  }

  def comparisonExpr(concExpr: StringConcatExpr, compExprPostfix: Option[ComparisonExprPostfix], ctx: Context): Context = {
    val ctx1 = stringConcatExpr(concExpr.rangeExpr, concExpr.rangeExprs, ctx)
    val value0 = ctx1.value

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
    ctx1.value(value1)
  }

  /**
   * generalcomp: "=", "!=", "<=", "<", ">=", ">"
   * valuecomp: "eq", "ne", "lt", "le", "gt", "ge"
   * nodecomp: "is", "<<", ">>"
   */
  def comparisonExprPostfix(compOp: CompOperator, concExpr: StringConcatExpr, ctx: Context) = {
    stringConcatExpr(concExpr.rangeExpr, concExpr.rangeExprs, ctx)
  }

  def stringConcatExpr(_rangeExpr: RangeExpr, rangeExprs: List[RangeExpr], ctx: Context) = {
    val ctx1 = rangeExpr(_rangeExpr.addExpr, _rangeExpr.toExpr, ctx)
    val value0 = ctx1.value

    val ctxs = rangeExprs map { x => rangeExpr(x.addExpr, x.toExpr, ctx) }
    val value1 = if (ctxs.nonEmpty) {
      XPathFunctions.strConcat(value0 :: ctxs.map { _.value })
    } else {
      value0
    }
    ctx1.value(value1)
  }

  def rangeExpr(addExpr: AdditiveExpr, toExpr: Option[AdditiveExpr], ctx: Context): Context = {
    val ctx1 = additiveExpr(addExpr.multiExpr, addExpr.prefixedMultiExprs, ctx)
    val value0 = ctx1.value

    val value1 = toExpr.map { x => additiveExpr(x.multiExpr, x.prefixedMultiExprs, ctx) } match {
      case None    => value0
      case Some(x) => XPathFunctions.range(value0, x.value)
    }
    ctx1.value(value1)
  }

  def additiveExpr(multiExpr: MultiplicativeExpr, prefixedMultiExprs: List[MultiplicativeExpr], ctx: Context): Context = {
    val ctx1 = multiplicativeExpr(multiExpr.prefix, multiExpr.unionExpr, multiExpr.prefixedUnionExprs, ctx)
    val v0 = ctx1.value
    val value0 = multiExpr.prefix match {
      case Nop | Plus => v0
      case Minus      => XPathFunctions.neg(v0)
    }

    val ctxs = prefixedMultiExprs map { x => (x.prefix, multiplicativeExpr(x.prefix, x.unionExpr, x.prefixedUnionExprs, ctx)) }
    val value1 = ctxs.foldLeft(value0) {
      case (acc, (Plus, x))  => XPathFunctions.plus(acc, x.value)
      case (acc, (Minus, x)) => XPathFunctions.minus(acc, x.value)
      case _                 => value0
    }
    ctx1.value(value1)
  }

  /**
   * prefix is "", or "+", "-"
   */
  def multiplicativeExpr(prefix: Prefix, _unionExpr: UnionExpr, prefixedUnionExprs: List[UnionExpr], ctx: Context): Context = {
    val ctx1 = unionExpr(_unionExpr.prefix, _unionExpr.intersectExceptExpr, _unionExpr.prefixedIntersectExceptExprs, ctx)
    val value0 = ctx1.value

    val ctxs = prefixedUnionExprs map { x => (x.prefix, unionExpr(x.prefix, x.intersectExceptExpr, x.prefixedIntersectExceptExprs, ctx)) }
    val value1 = ctxs.foldLeft(value0) {
      case (acc, (Aster, x)) => XPathFunctions.multiply(acc, x.value)
      case (acc, (Div, x))   => XPathFunctions.divide(acc, x.value)
      case (acc, (IDiv, x))  => XPathFunctions.idivide(acc, x.value)
      case (acc, (Mod, x))   => XPathFunctions.mod(acc, x.value)
      case _                 => value0
    }
    ctx1.value(value1)
  }

  /**
   * prefix is "", or "*", "div", "idiv", "mod"
   */
  def unionExpr(prefix: Prefix, _intersectExceptExpr: IntersectExceptExpr, prefixedIntersectExceptExprs: List[IntersectExceptExpr], ctx: Context): Context = {
    val ctx1 = intersectExceptExpr(_intersectExceptExpr.prefix, _intersectExceptExpr.instanceOfExpr, _intersectExceptExpr.prefixedInstanceOfExprs, ctx)
    val value0 = ctx1.value

    val ctxs = prefixedIntersectExceptExprs map { x => (x.prefix, intersectExceptExpr(x.prefix, x.instanceOfExpr, x.prefixedInstanceOfExprs, ctx)) }
    val value1 = ctxs.foldLeft(value0) {
      case (acc, (Union, x)) => value0 // TODO
      case _                 => value0
    }
    ctx1.value(value1)
  }

  /**
   * prefix is "", or "union", "|". The union and | operators are equivalent
   */
  def intersectExceptExpr(prefix: Prefix, _instanceOfExpr: InstanceofExpr, prefixedInstanceOfExprs: List[InstanceofExpr], ctx: Context): Context = {
    val ctx1 = instanceofExpr(_instanceOfExpr.prefix, _instanceOfExpr.treatExpr, _instanceOfExpr.ofType, ctx)
    val value0 = ctx1.value

    val ctxs = prefixedInstanceOfExprs map { x => (x.prefix, instanceofExpr(x.prefix, x.treatExpr, x.ofType, ctx)) }
    val value1 = ctxs.foldLeft(value0) {
      case (acc, (Intersect, x)) => value0 // TODO
      case (acc, (Except, x))    => value0 // TODO
      case _                     => value0
    }
    ctx1.value(value1)
  }

  /**
   * prefix is "", or "intersect", "except"
   */
  def instanceofExpr(prefix: Prefix, _treatExpr: TreatExpr, ofType: Option[SequenceType], ctx: Context): Context = {
    val ctx1 = treatExpr(_treatExpr.castableExpr, _treatExpr.asType, ctx)
    val value = ctx1.value

    val value1 = ofType match {
      case Some(x) =>
        val tpe = sequenceType(x, ctx1)
        value // TODO
      case None => value
    }
    ctx1.value(value1)
  }

  def treatExpr(_castableExpr: CastableExpr, asType: Option[SequenceType], ctx: Context): Context = {
    val ctx1 = castableExpr(_castableExpr.castExpr, _castableExpr.asType, ctx)
    val value = ctx1.value

    val value1 = asType match {
      case Some(x) =>
        val tpe = sequenceType(x, ctx1)
        value // TODO
      case None => value
    }
    ctx1.value(value1)
  }

  def castableExpr(_castExpr: CastExpr, asType: Option[SingleType], ctx: Context): Context = {
    val ctx1 = castExpr(_castExpr.unaryExpr, _castExpr.asType, ctx)
    val value = ctx1.value

    val value1 = asType match {
      case Some(x) =>
        val tpe = singleType(x.name, x.withQuestionMark, ctx1)
        value // TODO
      case None => value
    }
    ctx1.value(value1)
  }

  def castExpr(_unaryExpr: UnaryExpr, asType: Option[SingleType], ctx: Context): Context = {
    val ctx1 = unaryExpr(_unaryExpr.prefix, _unaryExpr.valueExpr, ctx)
    val v = ctx1.value
    val value = _unaryExpr.prefix match {
      case Nop | Plus => v
      case Minus      => XPathFunctions.neg(v)
    }

    val value1 = asType match {
      case Some(x) =>
        val tpe = singleType(x.name, x.withQuestionMark, ctx1)
        value // TODO
      case None => value
    }
    ctx1.value(value1)
  }

  /**
   * prefix is "", or "-", "+"
   */
  def unaryExpr(prefix: Prefix, _valueExpr: ValueExpr, ctx: Context): Context = {
    valueExpr(_valueExpr.simpleMapExpr, ctx)
  }

  /**
   * TODO, fetch value here or later?
   */
  def valueExpr(_simpleMapExpr: SimpleMapExpr, ctx: Context) = {
    simpleMapExpr(_simpleMapExpr.pathExpr, _simpleMapExpr.exclamExprs, ctx)
  }

  def simpleMapExpr(_pathExpr: PathExpr, exclamExprs: List[PathExpr], ctx: Context): Context = {
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
  def pathExpr(prefix: Prefix, relativeExpr: Option[RelativePathExpr], ctx: Context): Context = {
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

  def relativePathExpr(_stepExpr: StepExpr, prefixedStepExprs: List[StepExpr], ctx: Context): Context = {
    val path0 = stepExpr(_stepExpr.prefix, _stepExpr.expr, ctx)
    prefixedStepExprs.foldLeft(path0) { (acc, x) => stepExpr(x.prefix, x.expr, acc) }
  }

  /**
   * prefix is "" or "/" or "//"
   */
  def stepExpr(prefix: Prefix, expr: Either[PostfixExpr, AxisStep], ctx: Context): Context = {
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
            Context(null, value0, null, value0) // TODO
        }
    }
  }

  def reverseAxisStep(step: ReverseStep, predicates: PredicateList, ctx: Context): Context = {
    val nodeTestCtx = nodeTest(step.axis, step.nodeTest, ctx)
    // We should separate the node test ctx and predicate ctx, the node test ctx will
    // be applied by predicates, and predicate ctx will use it as the beginning
    // ctx. So, we'll rely on predicateList to make a new copy for each predicate 
    // evaluating
    val preds = predicateList(predicates.predicates, nodeTestCtx)
    _applyPredicateList(preds, nodeTestCtx)
  }

  def forwardAxisStep(step: ForwardStep, predicates: PredicateList, ctx: Context): Context = {
    val nodeTestCtx = nodeTest(step.axis, step.nodeTest, ctx)
    // We should separate the node test ctx and predicate ctx, the node test ctx will
    // be applied by predicates, and predicate ctx will use it as the beginning
    // ctx. So, we'll rely on predicateList to make a new copy for each predicate 
    // evaluating
    val preds = predicateList(predicates.predicates, nodeTestCtx)
    _applyPredicateList(preds, nodeTestCtx)
  }

  def nodeTest(axis: Axis, test: NodeTest, ctx: Context): Context = {
    test match {
      case x: NameTest =>
        val res = nameTest(x, ctx)
        _ctxOfNameTest(axis, res, ctx)
      case x: KindTest =>
        kindTest(x, ctx) // TODO
        ctx
    }
  }

  def nameTest(test: NameTest, ctx: Context) = {
    test match {
      case name: EQName       => name
      case wildcard: Wildcard => wildcard
    }
  }

  def kindTest(test: KindTest, ctx: Context) = {
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

  def argumentList(args: List[Argument], ctx: Context): List[Context] = {
    args map {
      case expr: ExprSingle    => exprSingle(expr, ctx)
      case ArgumentPlaceholder => ctx.value(ArgumentPlaceholder)
    }
  }

  /**
   * Should make a new copy of ctx for each predicate, since we are in new ctx
   * during each predicate evaluating.
   */
  def predicateList(predicates: List[Predicate], ctx: Context): List[Context] = {
    predicates map { x => predicate(x.expr, ctx.copy) }
  }

  /**
   * predicate is composed by 'expr' which may have multiple values, but actually
   * it's rare?  So we just pick the head one.
   */
  def predicate(_expr: Expr, ctx: Context): Context = {
    expr(_expr.expr, _expr.exprs, ctx).head
  }

  def lookup(_keySpecifier: KeySpecifier, ctx: Context) = {
    keySpecifier(_keySpecifier, ctx)
  }

  def keySpecifier(key: AnyRef, ctx: Context) = {
    key match {
      case ncName: String          => ncName
      case v: java.lang.Integer    => v
      case ParenthesizedExpr(expr) => parenthesizedExpr(expr, ctx)
      case Aster                   => Aster
      case x                       => throw new XPathRuntimeException(x, "could not to be a KeySpecifier.")
    }
  }

  def arrowPostfix(arrowFunction: ArrowFunctionSpecifier, args: ArgumentList, ctx: Context) = {
    arrowFunction match {
      case name: EQName            =>
      case VarRef(varName)         =>
      case ParenthesizedExpr(expr) => parenthesizedExpr(expr, ctx)
    }
  }

  def primaryExpr(expr: PrimaryExpr, ctx: Context): Any = {
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
  def literal(v: Any, ctx: Context): AnyRef = {
    v.asInstanceOf[AnyRef]
  }

  def varRef(_varName: VarName, ctx: Context) = {
    varName(_varName.eqName, ctx)
  }

  def varName(eqName: EQName, ctx: Context): EQName = {
    eqName
  }

  def parenthesizedExpr(_expr: Option[Expr], ctx: Context): Option[List[Context]] = {
    _expr map { x => expr(x.expr, x.exprs, ctx) }
  }

  def functionCall(name: EQName, _args: ArgumentList, ctx: Context): Any = {
    val fnName = name match {
      case UnprefixedName(local)       => local
      case PrefixedName(prefix, local) => local
      case URIQualifiedName(uri, name) => name
    }
    // args will be evaluated to value previously 
    val args = argumentList(_args.args, ctx) map (_.value)

    // functions are always applied on ctx.target
    fnName match {
      case "last"     => XPathFunctions.last(_getArrayOfNode(ctx.node))
      case "position" => XPathFunctions.position(_getArrayOfNode(ctx.node))

      case "name"     => XPathFunctions.name(_getMapOfNode(ctx.node))

      case "not"      => XPathFunctions.not(args.head)
      case "true"     => true
      case "false"    => false

      case _          => throw new XPathRuntimeException(fnName, "is not a supported function")
    }
  }

  private def _getArrayOfNode(node: AvroNode[_]): java.util.Collection[_] = {
    node match {
      case RecordNode(rec, Some(field)) => rec.get(field.pos).asInstanceOf[java.util.Collection[_]]
      case ArrayNode(arr, _, _, _)      => arr
      case _                            => throw new XPathRuntimeException(node, "does not contain array target")
    }
  }

  private def _getMapOfNode(node: AvroNode[_]): java.util.Map[String, _] = {
    node match {
      case RecordNode(rec, Some(field)) => rec.get(field.pos).asInstanceOf[java.util.Map[String, _]]
      case MapNode(map, _, _, _)        => map
      case _                            => throw new XPathRuntimeException(node, "does not contain map target")
    }
  }

  def namedFunctionRef(name: EQName, index: Int, ctx: Context) = {
    // TODO
  }

  def inlineFunctionExpr(params: Option[ParamList], asType: Option[SequenceType], _functionBody: FunctionBody, ctx: Context) = {
    params match {
      case Some(x) => paramList(x.param, x.params, ctx)
      case None    => Nil
    }
    asType map { sequenceType(_, ctx) }
    functionBody(_functionBody.expr, ctx)
  }

  def mapConstructor(entrys: List[MapConstructorEntry], ctx: Context) = {
    entrys map { x => mapConstructorEntry(x.key, x.value, ctx) }
  }

  def mapConstructorEntry(key: MapKeyExpr, value: MapValueExpr, ctx: Context): Any = {
    exprSingle(key.expr, ctx)
    exprSingle(value.expr, ctx)
  }

  def arrayConstructor(constructor: ArrayConstructor, ctx: Context): Any = {
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

  def unaryLookup(key: KeySpecifier, ctx: Context) = {
    keySpecifier(key, ctx)
  }

  def singleType(name: SimpleTypeName, withQuestionMark: Boolean, ctx: Context) = {
    simpleTypeName(name.name, ctx)
  }

  def typeDeclaration(asType: SequenceType, ctx: Context) = {
    sequenceType(asType, ctx)
  }

  def sequenceType(tpe: SequenceType, ctx: Context) = {
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

  def itemType(tpe: ItemType, ctx: Context): Any = {
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
  def documentTest(elemTest: Option[KindTest], ctx: Context): Any = {
    elemTest map { x => kindTest(x, ctx) }
  }

  def piTest(name: Option[String], ctx: Context) = {
    name match {
      case Some(x) =>
      case None    =>
    }
  }

  def attributeTest_Name(name: AttribNameOrWildcard, typeName: Option[TypeName], ctx: Context) = {
    // TODO optional type name
    name match {
      case Left(x)  => attributeName(x.name, ctx)
      case Right(x) => // Aster - QName
    }
  }

  def schemaAttributeTest(attrDecl: AttributeDeclaration, ctx: Context) = {
    attributeDeclaration(attrDecl.name, ctx)
  }

  def attributeDeclaration(name: AttributeName, ctx: Context) = {
    attributeName(name.name, ctx)
  }

  def elementTest_Name(name: ElementNameOrWildcard, typeName: Option[TypeName], withQuestionMark: Boolean, ctx: Context) = {
    name match {
      case Left(x)  => elementName(x.name, ctx)
      case Right(x) => // Aster - QName
    }
  }

  def schemaElementTest(elemDecl: ElementDeclaration, ctx: Context) = {
    elementDeclaration(elemDecl.name, ctx)
  }

  def elementDeclaration(name: ElementName, ctx: Context) = {
    elementName(name.name, ctx)
  }

  def attributeName(name: EQName, ctx: Context): EQName = {
    name
  }

  def elementName(name: EQName, ctx: Context): EQName = {
    name
  }

  def simpleTypeName(name: TypeName, ctx: Context): EQName = {
    typeName(name.name, ctx)
  }

  def typeName(name: EQName, ctx: Context): EQName = {
    name
  }

  def functionTest(test: FunctionTest, ctx: Context) = {
    test match {
      case AnyFunctionTest                  =>
      case TypedFunctionTest(types, asType) => typedFunctionTest(types, asType, ctx)
    }
  }

  def typedFunctionTest(types: List[SequenceType], asType: SequenceType, ctx: Context): Any = {
    types map { sequenceType(_, ctx) }
    sequenceType(asType, ctx)
  }

  def mapTest(test: MapTest, ctx: Context) = {
    test match {
      case AnyMapTest                =>
      case TypedMapTest(tpe, asType) => typedMapTest(tpe, asType, ctx)
    }
  }

  def typedMapTest(tpe: AtomicOrUnionType, asType: SequenceType, ctx: Context) = {
    //tpe.eqName
    sequenceType(asType, ctx)
  }

  def arrayTest(test: ArrayTest, ctx: Context) = {
    test match {
      case AnyArrayTest        =>
      case TypedArrayTest(tpe) => typedArrayTest(tpe, ctx)
    }
  }

  def typedArrayTest(tpe: SequenceType, ctx: Context) = {
    sequenceType(tpe, ctx)
  }

}
