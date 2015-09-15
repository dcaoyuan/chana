package chana.jpql

import chana.jpql.nodes._
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneId
import java.time.temporal.Temporal
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8

case class JPQLRuntimeException(value: Any, message: String)
  extends RuntimeException(
    (value match {
      case null      => null
      case x: AnyRef => x.getClass.getName
      case _         => value
    }) + " " + message + ":" + value)

object JPQLFunctions {

  def plus(left: Any, right: Any): Number = {
    (left, right) match {
      case (x: java.lang.Double, y: Number)  => x + y.doubleValue
      case (x: Number, y: java.lang.Double)  => x.doubleValue + y
      case (x: java.lang.Float, y: Number)   => x + y.floatValue
      case (x: Number, y: java.lang.Float)   => x.floatValue + y
      case (x: java.lang.Long, y: Number)    => x + y.longValue
      case (x: Number, y: java.lang.Long)    => x.longValue + y
      case (x: java.lang.Integer, y: Number) => x + y.intValue
      case (x: Number, y: java.lang.Integer) => x.intValue + y
      case x                                 => throw JPQLRuntimeException(x, "is not pair of number")
    }
  }

  def minus(left: Any, right: Any): Number = {
    (left, right) match {
      case (x: java.lang.Double, y: Number)  => x - y.doubleValue
      case (x: Number, y: java.lang.Double)  => x.doubleValue - y
      case (x: java.lang.Float, y: Number)   => x - y.floatValue
      case (x: Number, y: java.lang.Float)   => x.floatValue - y
      case (x: java.lang.Long, y: Number)    => x - y.longValue
      case (x: Number, y: java.lang.Long)    => x.longValue - y
      case (x: java.lang.Integer, y: Number) => x - y.intValue
      case (x: Number, y: java.lang.Integer) => x.intValue - y
      case x                                 => throw JPQLRuntimeException(x, "is not pair of number")
    }
  }

  def multiply(left: Any, right: Any): Number = {
    (left, right) match {
      case (x: java.lang.Double, y: Number)  => x * y.doubleValue
      case (x: Number, y: java.lang.Double)  => x.doubleValue * y
      case (x: java.lang.Float, y: Number)   => x * y.floatValue
      case (x: Number, y: java.lang.Float)   => x.floatValue * y
      case (x: java.lang.Long, y: Number)    => x * y.longValue
      case (x: Number, y: java.lang.Long)    => x.longValue * y
      case (x: java.lang.Integer, y: Number) => x * y.intValue
      case (x: Number, y: java.lang.Integer) => x.intValue * y
      case x                                 => throw JPQLRuntimeException(x, "is not pair of number")
    }
  }

  def divide(left: Any, right: Any): Number = {
    (left, right) match {
      case (x: java.lang.Double, y: Number)  => x / y.doubleValue
      case (x: Number, y: java.lang.Double)  => x.doubleValue / y
      case (x: java.lang.Float, y: Number)   => x / y.floatValue
      case (x: Number, y: java.lang.Float)   => x.floatValue / y
      case (x: java.lang.Long, y: Number)    => x / y.longValue
      case (x: Number, y: java.lang.Long)    => x.longValue / y
      case (x: java.lang.Integer, y: Number) => x / y.intValue
      case (x: Number, y: java.lang.Integer) => x.intValue / y
      case x                                 => throw JPQLRuntimeException(x, "is not pair of number")
    }
  }

  def neg(v: Any): Number = {
    v match {
      case x: java.lang.Double  => -x
      case x: java.lang.Float   => -x
      case x: java.lang.Long    => -x
      case x: java.lang.Integer => -x
      case x                    => throw JPQLRuntimeException(x, "is not a number")
    }
  }

  def abs(v: Any): Number = {
    v match {
      case x: java.lang.Integer => math.abs(x)
      case x: java.lang.Long    => math.abs(x)
      case x: java.lang.Float   => math.abs(x)
      case x: java.lang.Double  => math.abs(x)
      case x                    => throw JPQLRuntimeException(x, "is not a number")
    }
  }

  def eq(left: Any, right: Any) = {
    (left, right) match {
      case (x: Number, y: Number) => x == y
      case (x: CharSequence, y: CharSequence) => x == y
      case (x: LocalTime, y: LocalTime) => !(x.isAfter(y) || x.isBefore(y)) // ??
      case (x: LocalDate, y: LocalDate) => x.isEqual(y)
      case (x: LocalDateTime, y: LocalDateTime) => x.isEqual(y)
      case (x: Temporal, y: Temporal) => x == y
      case (x: java.lang.Boolean, y: java.lang.Boolean) => x == y
      case x => throw JPQLRuntimeException(x, "can not be applied EQ")
    }
  }

  def ne(left: Any, right: Any) = {
    (left, right) match {
      case (x: Number, y: Number) => x != y
      case (x: CharSequence, y: CharSequence) => x != y
      case (x: LocalTime, y: LocalTime) => x.isAfter(y) || x.isBefore(y)
      case (x: LocalDate, y: LocalDate) => !x.isEqual(y)
      case (x: LocalDateTime, y: LocalDateTime) => !x.isEqual(y)
      case (x: Temporal, y: Temporal) => x != y
      case (x: java.lang.Boolean, y: java.lang.Boolean) => x != y
      case x => throw JPQLRuntimeException(x, "can not be applied NE")
    }
  }

  def gt(left: Any, right: Any) = {
    (left, right) match {
      case (x: Number, y: Number)               => x.doubleValue > y.doubleValue
      case (x: LocalTime, y: LocalTime)         => x.isAfter(y)
      case (x: LocalDate, y: LocalDate)         => x.isAfter(y)
      case (x: LocalDateTime, y: LocalDateTime) => x.isAfter(y)
      case x                                    => throw JPQLRuntimeException(x, "can not be applied GT")
    }
  }

  def ge(left: Any, right: Any) = {
    (left, right) match {
      case (x: Number, y: Number)               => x.doubleValue >= y.doubleValue
      case (x: LocalTime, y: LocalTime)         => x.isAfter(y) || !x.isBefore(y)
      case (x: LocalDate, y: LocalDate)         => x.isAfter(y) || x.isEqual(y)
      case (x: LocalDateTime, y: LocalDateTime) => x.isAfter(y) || x.isEqual(y)
      case x                                    => throw JPQLRuntimeException(x, "can not be applied GE")
    }
  }

  def lt(left: Any, right: Any) = {
    (left, right) match {
      case (x: Number, y: Number)               => x.doubleValue < y.doubleValue
      case (x: LocalTime, y: LocalTime)         => x.isBefore(y)
      case (x: LocalDate, y: LocalDate)         => x.isBefore(y)
      case (x: LocalDateTime, y: LocalDateTime) => x.isBefore(y)
      case x                                    => throw JPQLRuntimeException(x, "can not be applied LT")
    }
  }

  def le(left: Any, right: Any) = {
    (left, right) match {
      case (x: Number, y: Number)               => x.doubleValue <= y.doubleValue
      case (x: LocalTime, y: LocalTime)         => x.isBefore(y) || !x.isAfter(y)
      case (x: LocalDate, y: LocalDate)         => x.isBefore(y) || x.isEqual(y)
      case (x: LocalDateTime, y: LocalDateTime) => x.isBefore(y) || x.isEqual(y)
      case x                                    => throw JPQLRuntimeException(x, "can not be applied LE")
    }
  }

  def strLike(str: String, expr: String, escape: Option[String]): Boolean = {
    val likeExpr = expr.toLowerCase.replace(".", "\\.").replace("?", ".").replace("%", ".*")
    str.toLowerCase.matches(likeExpr)
  }

  def between(base: Any, min: Any, max: Any) = {
    (base, min, max) match {
      case (x: Number, min: Number, max: Number) =>
        x.doubleValue >= min.doubleValue && x.doubleValue <= max.doubleValue
      case (x: LocalTime, min: LocalTime, max: LocalTime) =>
        (x.isAfter(min) || !x.isBefore(min)) && (x.isBefore(max) || !x.isAfter(max))
      case (x: LocalDate, min: LocalDate, max: LocalDate) =>
        (x.isAfter(min) || x.isEqual(min)) && (x.isBefore(max) || x.isEqual(max))
      case (x: LocalDateTime, min: LocalDateTime, max: LocalDateTime) =>
        (x.isAfter(min) || x.isEqual(min)) && (x.isBefore(max) || x.isEqual(max))
      case x => throw JPQLRuntimeException(x, "can not be appled BETWEEN")
    }
  }

  def currentTime() = LocalTime.now()
  def currentDate() = LocalDate.now()
  def currentDateTime() = LocalDateTime.now()

  final class IndexedList(underlying: java.util.List[_], joinSpec: JoinSpec = INNER_JOIN) {
    lazy val indexToElement = {
      var map = Map[Int, Any]()
      var i = 1
      var itr = underlying.iterator
      while (itr.hasNext) {
        map += (i -> itr.next)
        i += 1
      }
      map
    }

    def apply(i: Int) = indexToElement(i)

    def EQ(x: Int) = {
      val javaIdx = x - 1
      val xs = new java.util.LinkedList[Any]()
      xs.add(underlying.get(javaIdx))
      xs
    }

    def NE(x: Int) = {
      val javaIdx = x - 1
      val xs = new java.util.LinkedList[Any]()
      var i = 0
      val itr = underlying.iterator
      while (itr.hasNext) {
        if (i != javaIdx) {
          xs.add(itr.next)
        } else {
          itr.next
        }
        i += 1
      }
      xs
    }

    def GT(x: Int) = {
      val javaIdx = x - 1
      val xs = new java.util.LinkedList[Any]()
      var i = 0
      val itr = underlying.iterator
      while (itr.hasNext) {
        if (i > javaIdx) {
          xs.add(itr.next)
        } else {
          itr.next
        }
        i += 1
      }
      xs
    }

    def GE(x: Int) = {
      val javaIdx = x - 1
      val xs = new java.util.LinkedList[Any]()
      var i = 0
      val itr = underlying.iterator
      while (itr.hasNext) {
        if (i >= javaIdx) {
          xs.add(itr.next)
        } else {
          itr.next
        }
        i += 1
      }
      xs
    }

    def LT(x: Int) = {
      val javaIdx = x - 1
      val xs = new java.util.LinkedList[Any]()
      var i = 0
      val itr = underlying.iterator
      while (itr.hasNext && i < javaIdx) {
        xs.add(itr.next)
        i += 1
      }
      xs
    }

    def LE(x: Int) = {
      val javaIdx = x - 1
      val xs = new java.util.LinkedList[Any]()
      var i = 0
      val itr = underlying.iterator
      while (itr.hasNext && i <= javaIdx) {
        xs.add(itr.next)
        i += 1
      }
      xs
    }

    def BETWEEN(min: Int, max: Int) = {
      val javaMin = min - 1
      val javaMax = max - 1
      val xs = new java.util.LinkedList[Any]()
      var i = 0
      val itr = underlying.iterator
      while (itr.hasNext && i <= javaMax) {
        if (i >= javaMin) {
          xs.add(itr.next)
        } else {
          itr.next
        }
        i += 1
      }
      xs
    }
  }
}

object JPQLEvaluator {

  def keyOf(qual: String, attrPaths: List[String]) = {
    val key = new StringBuilder(qual)
    var paths = attrPaths
    while (paths.nonEmpty) {
      key.append(".").append(paths.head)
      paths = paths.tail
    }
    key.toString
  }

  val timeZone = ZoneId.systemDefault
}

class JPQLEvaluator extends JPQLVisitor {

  protected var asToEntity = Map[String, String]()
  protected var asToJoin = Map[String, Any]()
  private var asToItem = Map[String, Any]()
  private var asToCollectionMember = Map[String, Any]()

  protected var selectObjects = List[Any]()
  protected var selectMapEntries = List[Any]()
  protected var selectNewInstances = List[Any]()

  protected var selectedItems = List[Any]()
  protected var selectIsDistinct: Boolean = _
  protected var isToCollect: Boolean = _
  protected var isJoin: Boolean = _

  /**
   * For simple test
   */
  private[jpql] def simpleEval(root: Statement, record: Any): List[Any] = {
    root match {
      case SelectStatement(select, from, where, groupby, having, orderby) =>
        fromClause(from, record)

        val whereCond = where.fold(true) { x => whereClause(x, record) }
        if (whereCond) {
          selectClause(select, record)

          groupby.fold(List[Any]()) { x => groupbyClause(x, record) }

          having.fold(true) { x => havingClause(x, record) }
          orderby.fold(List[Any]()) { x => orderbyClause(x, record) }

          selectedItems.reverse
        } else {
          List()
        }

      case UpdateStatement(update, set, where) => List() // NOT YET
      case DeleteStatement(delete, where)      => List() // NOT YET
    }
  }

  final def entityOf(as: String): Option[String] = asToEntity.get(as)

  def valueOf(qual: String, attrPaths: List[String], record: Any): Any = {
    // TODO in case of record does not contain schema, get entityNames from DistributedSchemaBoard?
    record match {
      case rec: GenericRecord =>
        val EntityName = rec.getSchema.getName.toLowerCase
        asToEntity.get(qual) match {
          case Some(EntityName) =>
            var paths = attrPaths
            var curr: Any = rec
            while (paths.nonEmpty) {
              curr match {
                case x: GenericRecord =>
                  val path = paths.head
                  paths = paths.tail
                  curr = x.get(path)
                case null => throw JPQLRuntimeException(curr, "is null when fetch its attribute: " + paths)
                case _    => throw JPQLRuntimeException(curr, "is not a record when fetch its attribute: " + paths)
              }
            }
            if (curr.isInstanceOf[Utf8]) {
              curr.toString
            } else {
              curr
            }
          case _ => throw JPQLRuntimeException(qual, "is not an AS alias of entity: " + EntityName)
        }
      case _ => throw JPQLRuntimeException(record, "is not an avro record")
    }
  }

  override def updateClause(updateClause: UpdateClause, record: Any) = {
    val entityName = updateClause.entityName.ident
    updateClause.as foreach { x =>
      x.ident
    }
  }

  override def setClause(setClause: SetClause, record: Any) = {
    val assign = setAssignClause(setClause.assign, record)
    setClause.assigns foreach { x =>
      setAssignClause(x, record)
    }
  }

  override def setAssignClause(assign: SetAssignClause, record: Any) = {
    val target = setAssignTarget(assign.target, record)
    val value = newValue(assign.value, record)
  }

  override def setAssignTarget(target: SetAssignTarget, record: Any) = {
    target.path match {
      case Left(x)  => pathExpr(x, record)
      case Right(x) => attribute(x, record)
    }
  }

  override def newValue(expr: NewValue, record: Any) = {
    scalarExpr(expr.v, record)
  }

  override def deleteClause(deleteClause: DeleteClause, record: Any) = {
    val from = deleteClause.from.ident
    deleteClause.as foreach { x =>
      x.ident
    }
  }

  override def selectClause(select: SelectClause, record: Any): Unit = {
    isToCollect = true
    selectIsDistinct = select.isDistinct
    selectItem(select.item, record)
    select.items foreach { x => selectItem(x, record) }
    isToCollect = false
  }

  override def selectItem(item: SelectItem, record: Any): Any = {
    val item1 = selectExpr(item.expr, record)
    item.as foreach { x => asToItem += (x.ident -> item1) }
    item1
  }

  override def selectExpr(expr: SelectExpr, record: Any) = {
    expr match {
      case SelectExpr_AggregateExpr(expr)   => selectedItems ::= aggregateExpr(expr, record)
      case SelectExpr_ScalarExpr(expr)      => selectedItems ::= scalarExpr(expr, record)
      case SelectExpr_OBJECT(expr)          => selectObjects ::= varAccessOrTypeConstant(expr, record)
      case SelectExpr_ConstructorExpr(expr) => selectNewInstances ::= constructorExpr(expr, record)
      case SelectExpr_MapEntryExpr(expr)    => selectMapEntries ::= mapEntryExpr(expr, record)
    }
  }

  // SELECT ENTRY(e.contactInfo) from Employee e
  override def mapEntryExpr(expr: MapEntryExpr, record: Any): Any = {
    varAccessOrTypeConstant(expr.entry, record)
  }

  override def pathExprOrVarAccess(expr: PathExprOrVarAccess, record: Any): Any = {
    val qual = qualIdentVar(expr.qual, record)
    val paths = expr.attributes map { x => attribute(x, record) }
    valueOf(qual, paths, record)
  }

  // SELECT e from Employee e join e.contactInfo c where KEY(c) = 'Email' and VALUE(c) = 'joe@gmail.com'
  override def qualIdentVar(qual: QualIdentVar, record: Any): String = {
    qual match {
      case QualIdentVar_VarAccessOrTypeConstant(v) => varAccessOrTypeConstant(v, record)
      case QualIdentVar_KEY(v)                     => varAccessOrTypeConstant(v, record)
      case QualIdentVar_VALUE(v)                   => varAccessOrTypeConstant(v, record)
    }
  }

  override def aggregateExpr(expr: AggregateExpr, record: Any) = {
    expr match {
      case AggregateExpr_AVG(isDistinct, expr) =>
        scalarExpr(expr, record)
      case AggregateExpr_MAX(isDistinct, expr) =>
        scalarExpr(expr, record)
      case AggregateExpr_MIN(isDistinct, expr) =>
        scalarExpr(expr, record)
      case AggregateExpr_SUM(isDistinct, expr) =>
        scalarExpr(expr, record)
      case AggregateExpr_COUNT(isDistinct, expr) =>
        scalarExpr(expr, record)
    }
  }

  override def constructorExpr(expr: ConstructorExpr, record: Any) = {
    val fullname = constructorName(expr.name, record)
    val args = constructorItem(expr.arg, record) :: (expr.args map { x => constructorItem(x, record) })
    null // NOT YET 
  }

  override def constructorName(name: ConstructorName, record: Any): String = {
    val fullname = new StringBuilder(name.id.ident)
    name.ids foreach fullname.append(".").append
    fullname.toString
  }

  override def constructorItem(item: ConstructorItem, record: Any) = {
    item match {
      case ConstructorItem_ScalarExpr(expr)    => scalarExpr(expr, record)
      case ConstructorItem_AggregateExpr(expr) => aggregateExpr(expr, record) // TODO aggregate here!?
    }
  }

  override def fromClause(from: FromClause, record: Any) = {
    identVarDecl(from.from, record)
    from.froms foreach {
      case Left(x)  => identVarDecl(x, record)
      case Right(x) => collectionMemberDecl(x, record)
    }
  }

  override def identVarDecl(ident: IdentVarDecl, record: Any) = {
    rangeVarDecl(ident.range, record)
    ident.joins foreach { x =>
      join(x, record)
    }
  }

  override def rangeVarDecl(range: RangeVarDecl, record: Any): Unit = {
    asToEntity += (range.as.ident.toLowerCase -> range.entityName.ident.toLowerCase)
  }

  /**
   *  The JOIN clause allows any of the object's relationships to be joined into
   *  the query so they can be used in the WHERE clause. JOIN does not mean the
   *  relationships will be fetched, unless the FETCH option is included.
   */
  override def join(join: Join, record: Any) = {
    isJoin = true
    join match {
      case Join_General(spec, expr, as, cond) =>
        spec match {
          case JOIN            =>
          case LEFT_JOIN       =>
          case LEFT_OUTER_JOIN =>
          case INNER_JOIN      =>
        }
        joinAssocPathExpr(expr, record)
        asToJoin += (as.ident -> expr)
        cond match {
          case Some(x) => joinCond(x, record)
          case None    =>
        }
      case Join_TREAT(spec, expr, exprAs, as, cond) =>
        spec match {
          case JOIN            =>
          case LEFT_JOIN       =>
          case LEFT_OUTER_JOIN =>
          case INNER_JOIN      =>
        }
        joinAssocPathExpr(expr, record)
        asToJoin += (as.ident -> expr)
        cond match {
          case Some(x) => joinCond(x, record)
          case None    =>
        }
      case Join_FETCH(spec, expr, alias, cond) =>
        spec match {
          case JOIN            =>
          case LEFT_JOIN       =>
          case LEFT_OUTER_JOIN =>
          case INNER_JOIN      =>
        }
        joinAssocPathExpr(expr, record)
        alias foreach { x => asToJoin += (x.ident -> expr) }
        cond match {
          case Some(x) => joinCond(x, record)
          case None    =>
        }
    }
    isJoin = false
  }

  override def joinCond(joinCond: JoinCond, record: Any) = {
    condExpr(joinCond.expr, record)
  }

  override def collectionMemberDecl(expr: CollectionMemberDecl, record: Any): Unit = {
    val member = collectionValuedPathExpr(expr.in, record)
    asToCollectionMember += (expr.as.ident -> member)
  }

  override def collectionValuedPathExpr(expr: CollectionValuedPathExpr, record: Any) = {
    pathExpr(expr.path, record)
  }

  override def assocPathExpr(expr: AssocPathExpr, record: Any) = {
    pathExpr(expr.path, record)
  }

  override def joinAssocPathExpr(expr: JoinAssocPathExpr, record: Any) = {
    val qual = qualIdentVar(expr.qualId, record)
    val paths = expr.attrbutes map { x => attribute(x, record) }
  }

  override def singleValuedPathExpr(expr: SingleValuedPathExpr, record: Any) = {
    pathExpr(expr.path, record)
  }

  override def stateFieldPathExpr(expr: StateFieldPathExpr, record: Any) = {
    pathExpr(expr.path, record)
  }

  override def pathExpr(expr: PathExpr, record: Any): Any = {
    val qual = qualIdentVar(expr.qual, record)
    val paths = expr.attributes map { x => attribute(x, record) }
    valueOf(qual, paths, record)
  }

  override def attribute(attr: Attribute, record: Any): String = {
    attr.name
  }

  override def varAccessOrTypeConstant(expr: VarAccessOrTypeConstant, record: Any): String = {
    expr.id.ident
  }

  override def whereClause(where: WhereClause, record: Any): Boolean = {
    condExpr(where.expr, record)
  }

  override def condExpr(expr: CondExpr, record: Any): Boolean = {
    expr.orTerms.foldLeft(condTerm(expr.term, record)) { (acc, orTerm) =>
      acc || condTerm(orTerm, record)
    }
  }

  override def condTerm(term: CondTerm, record: Any): Boolean = {
    term.andFactors.foldLeft(condFactor(term.factor, record)) { (acc, andFactor) =>
      acc && condFactor(andFactor, record)
    }
  }

  override def condFactor(factor: CondFactor, record: Any): Boolean = {
    val res = factor.expr match {
      case Left(x)  => condPrimary(x, record)
      case Right(x) => existsExpr(x, record)
    }

    factor.not ^ res
  }

  override def condPrimary(primary: CondPrimary, record: Any): Boolean = {
    primary match {
      case CondPrimary_CondExpr(expr)       => condExpr(expr, record)
      case CondPrimary_SimpleCondExpr(expr) => simpleCondExpr(expr, record)
    }
  }

  override def simpleCondExpr(expr: SimpleCondExpr, record: Any): Boolean = {
    val base = expr.expr match {
      case Left(x)  => arithExpr(x, record)
      case Right(x) => nonArithScalarExpr(x, record)
    }

    // simpleCondExprRem
    expr.rem match {
      case SimpleCondExprRem_ComparisonExpr(expr) =>
        // comparisionExpr
        val operand = comparsionExprRightOperand(expr.operand, record)
        expr.op match {
          case EQ => JPQLFunctions.eq(base, operand)
          case NE => JPQLFunctions.ne(base, operand)
          case GT => JPQLFunctions.gt(base, operand)
          case GE => JPQLFunctions.ge(base, operand)
          case LT => JPQLFunctions.lt(base, operand)
          case LE => JPQLFunctions.le(base, operand)
        }

      case SimpleCondExprRem_CondWithNotExpr(not, expr) =>
        // condWithNotExpr
        val res = expr match {
          case CondWithNotExpr_BetweenExpr(expr) =>
            val minMax = betweenExpr(expr, record)
            JPQLFunctions.between(base, minMax._1, minMax._2)

          case CondWithNotExpr_LikeExpr(expr) =>
            base match {
              case x: CharSequence =>
                val like = likeExpr(expr, record)
                JPQLFunctions.strLike(x.toString, like._1, like._2)
              case x => throw JPQLRuntimeException(x, "is not a string")
            }

          case CondWithNotExpr_InExpr(expr) =>
            inExpr(expr, record)

          case CondWithNotExpr_CollectionMemberExpr(expr) =>
            collectionMemberExpr(expr, record)
        }

        not ^ res

      case SimpleCondExprRem_IsExpr(not, expr) =>
        // isExpr
        val res = expr match {
          case IsNullExpr =>
            base match {
              case x: AnyRef => x eq null
              case _         => false // TODO
            }
          case IsEmptyExpr =>
            base match {
              case xs: java.util.Collection[_] => xs.isEmpty
              case xs: scala.collection.Seq[_] => xs.isEmpty
            }
        }

        not ^ res
    }
  }

  override def betweenExpr(expr: BetweenExpr, record: Any) = {
    (scalarOrSubselectExpr(expr.min, record), scalarOrSubselectExpr(expr.max, record))
  }

  override def inExpr(expr: InExpr, record: Any): Boolean = {
    expr match {
      case InExpr_InputParam(expr) =>
        inputParam(expr, record)
      case InExpr_ScalarOrSubselectExpr(expr, exprs) =>
        scalarOrSubselectExpr(expr, record)
        exprs map { x => scalarOrSubselectExpr(x, record) }
      case InExpr_Subquery(expr: Subquery) =>
        subquery(expr, record)
    }
    true
    // TODO
  }

  override def likeExpr(expr: LikeExpr, record: Any): (String, Option[String]) = {
    scalarOrSubselectExpr(expr.like, record) match {
      case like: CharSequence =>
        val escape = expr.escape match {
          case Some(x) =>
            scalarExpr(x.expr, record) match {
              case c: CharSequence => Some(c.toString)
              case x               => throw JPQLRuntimeException(x, "is not a string")
            }
          case None => None
        }
        (like.toString, escape)
      case x => throw JPQLRuntimeException(x, "is not a string")
    }
  }

  override def collectionMemberExpr(expr: CollectionMemberExpr, record: Any): Boolean = {
    collectionValuedPathExpr(expr.of, record)
    true // TODO
  }

  override def existsExpr(expr: ExistsExpr, record: Any): Boolean = {
    subquery(expr.subquery, record)
    true // TODO
  }

  override def comparsionExprRightOperand(expr: ComparsionExprRightOperand, record: Any) = {
    expr match {
      case ComparsionExprRightOperand_ArithExpr(expr)          => arithExpr(expr, record)
      case ComparsionExprRightOperand_NonArithScalarExpr(expr) => nonArithScalarExpr(expr, record)
      case ComparsionExprRightOperand_AnyOrAllExpr(expr)       => anyOrAllExpr(expr, record)
    }
  }

  override def arithExpr(expr: ArithExpr, record: Any) = {
    expr.expr match {
      case Left(expr)  => simpleArithExpr(expr, record)
      case Right(expr) => subquery(expr, record)
    }
  }

  override def simpleArithExpr(expr: SimpleArithExpr, record: Any): Any = {
    expr.rightTerms.foldLeft(arithTerm(expr.term, record)) {
      case (acc, ArithTerm_Plus(term))  => JPQLFunctions.plus(acc, arithTerm(term, record))
      case (acc, ArithTerm_Minus(term)) => JPQLFunctions.minus(acc, arithTerm(term, record))
    }
  }

  override def arithTerm(term: ArithTerm, record: Any): Any = {
    term.rightFactors.foldLeft(arithFactor(term.factor, record)) {
      case (acc, ArithFactor_Multiply(factor)) => JPQLFunctions.multiply(acc, arithFactor(factor, record))
      case (acc, ArithFactor_Divide(factor))   => JPQLFunctions.divide(acc, arithFactor(factor, record))
    }
  }

  override def arithFactor(factor: ArithFactor, record: Any): Any = {
    plusOrMinusPrimary(factor.primary, record)
  }

  override def plusOrMinusPrimary(primary: PlusOrMinusPrimary, record: Any): Any = {
    primary match {
      case ArithPrimary_Plus(primary)  => arithPrimary(primary, record)
      case ArithPrimary_Minus(primary) => JPQLFunctions.neg(arithPrimary(primary, record))
    }
  }

  override def arithPrimary(primary: ArithPrimary, record: Any) = {
    primary match {
      case ArithPrimary_PathExprOrVarAccess(expr)    => pathExprOrVarAccess(expr, record)
      case ArithPrimary_InputParam(expr)             => inputParam(expr, record)
      case ArithPrimary_CaseExpr(expr)               => caseExpr(expr, record)
      case ArithPrimary_FuncsReturningNumerics(expr) => funcsReturningNumerics(expr, record)
      case ArithPrimary_SimpleArithExpr(expr)        => simpleArithExpr(expr, record)
      case ArithPrimary_LiteralNumeric(expr)         => expr
    }
  }

  override def scalarExpr(expr: ScalarExpr, record: Any): Any = {
    expr match {
      case ScalarExpr_SimpleArithExpr(expr)    => simpleArithExpr(expr, record)
      case ScalarExpr_NonArithScalarExpr(expr) => nonArithScalarExpr(expr, record)
    }
  }

  override def scalarOrSubselectExpr(expr: ScalarOrSubselectExpr, record: Any) = {
    expr match {
      case ScalarOrSubselectExpr_ArithExpr(expr)          => arithExpr(expr, record)
      case ScalarOrSubselectExpr_NonArithScalarExpr(expr) => nonArithScalarExpr(expr, record)
    }
  }

  override def nonArithScalarExpr(expr: NonArithScalarExpr, record: Any): Any = {
    expr match {
      case NonArithScalarExpr_FuncsReturningDatetime(expr) => funcsReturningDatetime(expr, record)
      case NonArithScalarExpr_FuncsReturningStrings(expr)  => funcsReturningStrings(expr, record)
      case NonArithScalarExpr_LiteralString(expr)          => expr
      case NonArithScalarExpr_LiteralBoolean(expr)         => expr
      case NonArithScalarExpr_LiteralTemporal(expr)        => expr
      case NonArithScalarExpr_EntityTypeExpr(expr)         => entityTypeExpr(expr, record)
    }
  }

  override def anyOrAllExpr(expr: AnyOrAllExpr, record: Any) = {
    val subq = subquery(expr.subquery, record)
    expr.anyOrAll match {
      case ALL  =>
      case ANY  =>
      case SOME =>
    }
  }

  override def entityTypeExpr(expr: EntityTypeExpr, record: Any) = {
    typeDiscriminator(expr.typeDis, record)
  }

  override def typeDiscriminator(expr: TypeDiscriminator, record: Any) = {
    expr.expr match {
      case Left(expr1)  => varOrSingleValuedPath(expr1, record)
      case Right(expr1) => inputParam(expr1, record)
    }
  }

  override def caseExpr(expr: CaseExpr, record: Any): Any = {
    expr match {
      case CaseExpr_SimpleCaseExpr(expr) =>
        simpleCaseExpr(expr, record)
      case CaseExpr_GeneralCaseExpr(expr) =>
        generalCaseExpr(expr, record)
      case CaseExpr_CoalesceExpr(expr) =>
        coalesceExpr(expr, record)
      case CaseExpr_NullifExpr(expr) =>
        nullifExpr(expr, record)
    }
  }

  override def simpleCaseExpr(expr: SimpleCaseExpr, record: Any) = {
    caseOperand(expr.caseOperand, record)
    simpleWhenClause(expr.when, record)
    expr.whens foreach { when =>
      simpleWhenClause(when, record)
    }
    val elseExpr = scalarExpr(expr.elseExpr, record)
  }

  override def generalCaseExpr(expr: GeneralCaseExpr, record: Any) = {
    whenClause(expr.when, record)
    expr.whens foreach { when =>
      whenClause(when, record)
    }
    val elseExpr = scalarExpr(expr.elseExpr, record)
  }

  override def coalesceExpr(expr: CoalesceExpr, record: Any) = {
    scalarExpr(expr.expr, record)
    expr.exprs map { x => scalarExpr(x, record) }
  }

  override def nullifExpr(expr: NullifExpr, record: Any) = {
    val left = scalarExpr(expr.leftExpr, record)
    val right = scalarExpr(expr.rightExpr, record)
    left // TODO
  }

  override def caseOperand(expr: CaseOperand, record: Any) = {
    expr.expr match {
      case Left(x)  => stateFieldPathExpr(x, record)
      case Right(x) => typeDiscriminator(x, record)
    }
  }

  override def whenClause(whenClause: WhenClause, record: Any) = {
    val when = condExpr(whenClause.when, record)
    val thenExpr = scalarExpr(whenClause.thenExpr, record)
  }

  override def simpleWhenClause(whenClause: SimpleWhenClause, record: Any) = {
    val when = scalarExpr(whenClause.when, record)
    val thenExpr = scalarExpr(whenClause.thenExpr, record)
  }

  override def varOrSingleValuedPath(expr: VarOrSingleValuedPath, record: Any) = {
    expr.expr match {
      case Left(x)  => singleValuedPathExpr(x, record)
      case Right(x) => varAccessOrTypeConstant(x, record)
    }
  }

  override def stringPrimary(expr: StringPrimary, record: Any): Either[String, Any => String] = {
    expr match {
      case StringPrimary_LiteralString(expr) => Left(expr)
      case StringPrimary_FuncsReturningStrings(expr) =>
        try {
          Left(funcsReturningStrings(expr, record))
        } catch {
          case ex: Throwable => throw ex
        }
      case StringPrimary_InputParam(expr) =>
        val param = inputParam(expr, record)
        Right(param => "") // TODO
      case StringPrimary_StateFieldPathExpr(expr) =>
        pathExpr(expr.path, record) match {
          case x: CharSequence => Left(x.toString)
          case x               => throw JPQLRuntimeException(x, "is not a StringPrimary")
        }
    }
  }

  override def inputParam(expr: InputParam, record: Any) = {
    expr match {
      case InputParam_Named(name)   => name
      case InputParam_Position(pos) => pos
    }
  }

  override def funcsReturningNumerics(expr: FuncsReturningNumerics, record: Any): Any = {
    expr match {
      case Abs(expr) =>
        val v = simpleArithExpr(expr, record)
        JPQLFunctions.abs(v)

      case Length(expr) =>
        scalarExpr(expr, record) match {
          case x: CharSequence => x.length
          case x               => throw JPQLRuntimeException(x, "is not a string")
        }

      case Mod(expr, divisorExpr) =>
        scalarExpr(expr, record) match {
          case dividend: Number =>
            scalarExpr(divisorExpr, record) match {
              case divisor: Number => dividend.intValue % divisor.intValue
              case x               => throw JPQLRuntimeException(x, "divisor is not a number")
            }
          case x => throw JPQLRuntimeException(x, "dividend is not a number")
        }

      case Locate(expr, searchExpr, startExpr) =>
        scalarExpr(expr, record) match {
          case base: CharSequence =>
            scalarExpr(searchExpr, record) match {
              case searchStr: CharSequence =>
                val start = startExpr match {
                  case Some(exprx) =>
                    scalarExpr(exprx, record) match {
                      case x: java.lang.Integer => x - 1
                      case x                    => throw JPQLRuntimeException(x, "start is not an integer")
                    }
                  case None => 0
                }
                base.toString.indexOf(searchStr.toString, start)
              case x => throw JPQLRuntimeException(x, "is not a string")
            }
          case x => throw JPQLRuntimeException(x, "is not a string")
        }

      case Size(expr) =>
        collectionValuedPathExpr(expr, record)
        // todo return size of elements of the collection member TODO
        0

      case Sqrt(expr) =>
        scalarExpr(expr, record) match {
          case x: Number => math.sqrt(x.doubleValue)
          case x         => throw JPQLRuntimeException(x, "is not a number")
        }

      // Select p from Employee e join e.projects p where e.id = :id and INDEX(p) = 1
      case Index(expr) =>
        val field = varAccessOrTypeConstant(expr, record)
        valueOf(field, Nil, record) match {
          case xs: java.util.List[_] => new JPQLFunctions.IndexedList(xs)
          case x                     => throw JPQLRuntimeException(x, "is not a indexed list membet")
        }

      case Func(name, args) =>
        // try to call function: name(as: _*) TODO
        val as = args map { x => newValue(x, record) }
        0
    }
  }

  override def funcsReturningDatetime(expr: FuncsReturningDatetime, record: Any): Temporal = {
    expr match {
      case CURRENT_DATE      => JPQLFunctions.currentDate()
      case CURRENT_TIME      => JPQLFunctions.currentTime()
      case CURRENT_TIMESTAMP => JPQLFunctions.currentDateTime()
    }
  }

  override def funcsReturningStrings(expr: FuncsReturningStrings, record: Any): String = {
    expr match {
      case Concat(expr, exprs: List[ScalarExpr]) =>
        scalarExpr(expr, record) match {
          case base: CharSequence =>
            (exprs map { x => scalarExpr(x, record) }).foldLeft(new StringBuilder(base.toString)) {
              case (sb, x: CharSequence) => sb.append(x)
              case x                     => throw JPQLRuntimeException(x, "is not a string")
            }.toString
          case x => throw JPQLRuntimeException(x, "is not a string")
        }

      case Substring(expr, startExpr, lengthExpr: Option[ScalarExpr]) =>
        scalarExpr(expr, record) match {
          case base: CharSequence =>
            scalarExpr(startExpr, record) match {
              case start: Number =>
                val end = (lengthExpr map { x => scalarExpr(x, record) }) match {
                  case Some(length: Number) => start.intValue + length.intValue - 1
                  case _                    => base.length - 1
                }
                base.toString.substring(start.intValue - 1, end)
              case x => throw JPQLRuntimeException(x, "is not a number")
            }
          case x => throw JPQLRuntimeException(x, "is not a string")
        }

      case Trim(trimSpec: Option[TrimSpec], trimChar: Option[TrimChar], from) =>
        val base = stringPrimary(from, record) match {
          case Left(x)  => x
          case Right(x) => x("") // TODO
        }
        val trimC = trimChar match {
          case Some(TrimChar_String(char)) => char
          case Some(TrimChar_InputParam(param)) =>
            inputParam(param, record)
            "" // TODO
          case None => ""
        }
        trimSpec match {
          case Some(BOTH) | None => base.replaceAll("^" + trimC + "|" + trimC + "$", "")
          case Some(LEADING)     => base.replaceAll("^" + trimC, "")
          case Some(TRAILING)    => base.replaceAll(trimC + "$", "")
        }

      case Upper(expr) =>
        scalarExpr(expr, record) match {
          case base: CharSequence => base.toString.toUpperCase
          case x                  => throw JPQLRuntimeException(x, "is not a string")
        }

      case Lower(expr) =>
        scalarExpr(expr, record) match {
          case base: CharSequence => base.toString.toLowerCase
          case x                  => throw JPQLRuntimeException(x, "is not a string")
        }
    }
  }

  override def subquery(subquery: Subquery, record: Any) = {
    val select = simpleSelectClause(subquery.select, record)
    val from = subqueryFromClause(subquery.from, record)
    val where = subquery.where match {
      case Some(x) => whereClause(x, record)
      case None    => true
    }
    subquery.groupby match {
      case Some(x: GroupbyClause) =>
      case None                   =>
    }
    subquery.having match {
      case Some(x: HavingClause) =>
      case None                  =>
    }
  }

  override def simpleSelectClause(select: SimpleSelectClause, record: Any) = {
    val isDistinct = select.isDistinct
    simpleSelectExpr(select.expr, record)
  }

  override def simpleSelectExpr(expr: SimpleSelectExpr, record: Any) = {
    expr match {
      case SimpleSelectExpr_SingleValuedPathExpr(expr)    => singleValuedPathExpr(expr, record)
      case SimpleSelectExpr_AggregateExpr(expr)           => aggregateExpr(expr, record)
      case SimpleSelectExpr_VarAccessOrTypeConstant(expr) => varAccessOrTypeConstant(expr, record)
    }
  }

  override def subqueryFromClause(fromClause: SubqueryFromClause, record: Any) = {
    val from = subselectIdentVarDecl(fromClause.from, record)
    val froms = fromClause.froms map {
      case Left(x)  => subselectIdentVarDecl(x, record)
      case Right(x) => collectionMemberDecl(x, record)
    }
  }

  override def subselectIdentVarDecl(ident: SubselectIdentVarDecl, record: Any) = {
    ident match {
      case SubselectIdentVarDecl_IdentVarDecl(expr) =>
        identVarDecl(expr, record)
      case SubselectIdentVarDecl_AssocPathExpr(expr, as) =>
        assocPathExpr(expr, record)
        as.ident
      case SubselectIdentVarDecl_CollectionMemberDecl(expr) =>
        collectionMemberDecl(expr, record)
    }
  }

  override def orderbyClause(orderbyClause: OrderbyClause, record: Any): List[Any] = {
    isToCollect = true
    val items = orderbyItem(orderbyClause.orderby, record) :: (orderbyClause.orderbys map { x => orderbyItem(x, record) })
    isToCollect = false
    items
  }

  override def orderbyItem(item: OrderbyItem, record: Any): Any = {
    val orderingItem = item.expr match {
      case Left(x)  => simpleArithExpr(x, record)
      case Right(x) => scalarExpr(x, record)
    }
    orderingItem match {
      case x: CharSequence  => (item.isAsc, x)
      case x: Number        => if (item.isAsc) x else JPQLFunctions.neg(x)
      case x: LocalTime     => (if (item.isAsc) 1 else -1) * (x.toNanoOfDay)
      case x: LocalDate     => (if (item.isAsc) 1 else -1) * (x.getYear * 12 * 31 + x.getMonthValue * 12 + x.getDayOfMonth)
      case x: LocalDateTime => (if (item.isAsc) 1 else -1) * (x.atZone(JPQLEvaluator.timeZone).toInstant.toEpochMilli)
      case x                => throw JPQLRuntimeException(x, "can not be applied order")
    }
  }

  override def groupbyClause(groupbyClause: GroupbyClause, record: Any): List[Any] = {
    isToCollect = true
    val groupbys = scalarExpr(groupbyClause.expr, record) :: (groupbyClause.exprs map { x => scalarExpr(x, record) })
    isToCollect = false
    groupbys
  }

  override def havingClause(having: HavingClause, record: Any): Boolean = {
    isToCollect = true
    val cond = condExpr(having.condExpr, record)
    isToCollect = false
    cond
  }

}

