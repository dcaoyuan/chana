package chana.jpql

import chana.jpql.nodes._
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.temporal.Temporal
import org.apache.avro.generic.GenericData.Record

case class JPQLRuntimeException(value: Any, message: String)
  extends RuntimeException(value.getClass.getName + " " + message + ":" + value)

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
}

class JPQLEvaluator(root: Statement, record: Record) {

  private var asToEntity = Map[String, String]()
  private var asToItem = Map[String, Any]()
  private var asToCollectionMember = Map[String, Any]()

  private var selectIsDistinct = false
  private var selectAggregates = List[Any]()
  private var selectScalars = List[Any]()
  private var selectObjects = List[Any]()
  private var selectMapEntries = List[Any]()
  private var selectNewInstances = List[Any]()

  final def entityOf(as: String): Option[String] = asToEntity.get(as)

  final def valueOf(_paths: List[String]) = {
    var paths = _paths
    var current: Any = record
    while (paths.nonEmpty) {
      val path = paths.head
      current = record.get(path)
      paths = paths.tail
    }
    current
  }

  def visit() = {
    root match {
      case SelectStatement(select, from, where, groupby, having, orderby) =>
        fromClause(from)
        selectClause(select)
        selectScalars = selectScalars.reverse

        val res = where match {
          case Some(x) =>
            if (whereClause(x)) {
              selectScalars
            } else {
              List()
            }
          case None =>
            selectScalars
        }

        groupby match {
          case Some(x) => groupbyClause(x)
          case None    =>
        }
        having match {
          case Some(x) => havingClause(x)
          case None    =>
        }
        orderby match {
          case Some(x) => orderbyClause(x)
          case None    =>
        }

        res
      case UpdateStatement(update, set, where) => // NOT YET
      case DeleteStatement(delete, where)      => // NOT YET
    }
  }

  def updateClause(updateClause: UpdateClause) = {
    val entityName = updateClause.entityName.ident
    updateClause.as foreach { x =>
      x.ident
    }
  }

  def setClause(setClause: SetClause) = {
    val assign = setAssignClause(setClause.assign)
    setClause.assigns foreach { x =>
      setAssignClause(x)
    }
  }

  def setAssignClause(assign: SetAssignClause) = {
    val target = setAssignTarget(assign.target)
    val value = newValue(assign.value)
  }

  def setAssignTarget(target: SetAssignTarget) = {
    target.path match {
      case Left(x)  => pathExpr(x)
      case Right(x) => attribute(x)
    }
  }

  def newValue(expr: NewValue) = {
    scalarExpr(expr.v)
  }

  def deleteClause(deleteClause: DeleteClause) = {
    val from = deleteClause.from.ident
    deleteClause.as foreach { x =>
      x.ident
    }
  }

  def selectClause(select: SelectClause): Unit = {
    selectIsDistinct = select.isDistinct
    selectItem(select.item)
    select.items foreach selectItem
  }

  def selectItem(item: SelectItem) = {
    val item1 = selectExpr(item.expr)
    item.as foreach { x => asToItem += (x.ident -> item1) }
    item1
  }

  def selectExpr(expr: SelectExpr) = {
    expr match {
      case SelectExpr_AggregateExpr(expr)   => selectAggregates ::= aggregateExpr(expr)
      case SelectExpr_ScalarExpr(expr)      => selectScalars ::= scalarExpr(expr)
      case SelectExpr_OBJECT(expr)          => selectObjects ::= varAccessOrTypeConstant(expr)
      case SelectExpr_ConstructorExpr(expr) => selectNewInstances ::= constructorExpr(expr)
      case SelectExpr_MapEntryExpr(expr)    => selectMapEntries ::= mapEntryExpr(expr)
    }
  }

  def mapEntryExpr(expr: MapEntryExpr): Any = {
    varAccessOrTypeConstant(expr.entry)
  }

  def pathExprOrVarAccess(expr: PathExprOrVarAccess): Any = {
    val field = qualIdentVar(expr.qual)
    val paths = field :: (expr.attributes map attribute)
    valueOf(paths)
  }

  def qualIdentVar(qual: QualIdentVar) = {
    qual match {
      case QualIdentVar_VarAccessOrTypeConstant(v) => varAccessOrTypeConstant(v)
      case QualIdentVar_KEY(v)                     => varAccessOrTypeConstant(v)
      case QualIdentVar_VALUE(v)                   => varAccessOrTypeConstant(v)
    }
  }

  def aggregateExpr(expr: AggregateExpr) = {
    expr match {
      case AggregateExpr_AVG(isDistinct, expr) =>
        scalarExpr(expr)
      case AggregateExpr_MAX(isDistinct, expr) =>
        scalarExpr(expr)
      case AggregateExpr_MIN(isDistinct, expr) =>
        scalarExpr(expr)
      case AggregateExpr_SUM(isDistinct, expr) =>
        scalarExpr(expr)
      case AggregateExpr_COUNT(isDistinct, expr) =>
        scalarExpr(expr)
    }
  }

  def constructorExpr(expr: ConstructorExpr) = {
    val fullname = constructorName(expr.name)
    val args = constructorItem(expr.arg) :: (expr.args map constructorItem)
    null // NOT YET 
  }

  def constructorName(name: ConstructorName): String = {
    val fullname = new StringBuilder(name.id.ident)
    name.ids foreach fullname.append(".").append
    fullname.toString
  }

  def constructorItem(item: ConstructorItem) = {
    item match {
      case ConstructorItem_ScalarExpr(expr)    => scalarExpr(expr)
      case ConstructorItem_AggregateExpr(expr) => aggregateExpr(expr) // TODO aggregate here!?
    }
  }

  def fromClause(from: FromClause) = {
    identVarDecl(from.from)
    from.froms foreach {
      case Left(x)  => identVarDecl(x)
      case Right(x) => collectionMemberDecl(x)
    }
  }

  def identVarDecl(ident: IdentVarDecl) = {
    rangeVarDecl(ident.range)
    ident.joins foreach { x =>
      join(x)
    }
  }

  def rangeVarDecl(range: RangeVarDecl): Unit = {
    asToEntity += (range.as.ident -> range.entityName.ident)
  }

  def join(join: Join) = {
    join match {
      case Join_General(spec, expr, as, cond) =>
        spec match {
          case JOIN            =>
          case LEFT_JOIN       =>
          case LEFT_OUTER_JOIN =>
          case INNER_JOIN      =>
        }
        joinAssocPathExpr(expr)
        val as_ = as.ident
        cond match {
          case Some(x) => joinCond(x)
          case None    =>
        }
      case Join_TREAT(spec, expr, exprAs, as, cond) =>
        spec match {
          case JOIN            =>
          case LEFT_JOIN       =>
          case LEFT_OUTER_JOIN =>
          case INNER_JOIN      =>
        }
        joinAssocPathExpr(expr)
        val as_ = as.ident
        cond match {
          case Some(x) => joinCond(x)
          case None    =>
        }
      case Join_FETCH(spec, expr, alias, cond) =>
        spec match {
          case JOIN            =>
          case LEFT_JOIN       =>
          case LEFT_OUTER_JOIN =>
          case INNER_JOIN      =>
        }
        joinAssocPathExpr(expr)
        alias foreach { x =>
          x.ident
        }
        cond match {
          case Some(x) => joinCond(x)
          case None    =>
        }
    }
  }

  def joinCond(joinCond: JoinCond) = {
    condExpr(joinCond.expr)
  }

  def collectionMemberDecl(expr: CollectionMemberDecl): Unit = {
    val member = collectionValuedPathExpr(expr.in)
    asToCollectionMember += (expr.as.ident -> member)
  }

  def collectionValuedPathExpr(expr: CollectionValuedPathExpr) = {
    pathExpr(expr.path)
  }

  def assocPathExpr(expr: AssocPathExpr) = {
    pathExpr(expr.path)
  }

  def joinAssocPathExpr(expr: JoinAssocPathExpr) = {
    val qualId = qualIdentVar(expr.qualId)
    val attrbutes = expr.attrbutes map attribute
  }

  def singleValuedPathExpr(expr: SingleValuedPathExpr) = {
    pathExpr(expr.path)
  }

  def stateFieldPathExpr(expr: StateFieldPathExpr) = {
    pathExpr(expr.path)
  }

  def pathExpr(expr: PathExpr): Any = {
    val fieldName = qualIdentVar(expr.qual)
    var field = record.get(fieldName)
    var attrs = expr.attributes map attribute
    while (attrs.nonEmpty && (field ne null)) {
      field.asInstanceOf[Record].get(attrs.head)
      attrs = attrs.tail
    }
    field
  }

  def attribute(attr: Attribute): String = {
    attr.name
  }

  def varAccessOrTypeConstant(expr: VarAccessOrTypeConstant): String = {
    expr.id.ident
  }

  def whereClause(where: WhereClause): Boolean = {
    condExpr(where.expr)
  }

  def condExpr(expr: CondExpr): Boolean = {
    expr.orTerms.foldLeft(condTerm(expr.term)) { (res, orTerm) =>
      res || condTerm(orTerm)
    }
  }

  def condTerm(term: CondTerm): Boolean = {
    term.andFactors.foldLeft(condFactor(term.factor)) { (res, andFactor) =>
      res && condFactor(andFactor)
    }
  }

  def condFactor(factor: CondFactor): Boolean = {
    val res = factor.expr match {
      case Left(x)  => condPrimary(x)
      case Right(x) => existsExpr(x)
    }

    if (factor.not) !res else res
  }

  def condPrimary(primary: CondPrimary): Boolean = {
    primary match {
      case CondPrimary_CondExpr(expr)       => condExpr(expr)
      case CondPrimary_SimpleCondExpr(expr) => simpleCondExpr(expr)
    }
  }

  def simpleCondExpr(expr: SimpleCondExpr): Boolean = {
    val base = expr.expr match {
      case Left(x)  => arithExpr(x)
      case Right(x) => nonArithScalarExpr(x)
    }

    // simpleCondExprRem
    expr.rem match {
      case SimpleCondExprRem_ComparisonExpr(expr) =>
        // comparisionExpr
        val operand = comparsionExprRightOperand(expr.operand)
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
            val minMax = betweenExpr(expr)
            JPQLFunctions.between(base, minMax._1, minMax._2)

          case CondWithNotExpr_LikeExpr(expr) =>
            base match {
              case x: CharSequence =>
                val like = likeExpr(expr)
                JPQLFunctions.strLike(x.toString, like._1, like._2)
              case x => throw JPQLRuntimeException(x, "is not a string")
            }

          case CondWithNotExpr_InExpr(expr) =>
            inExpr(expr)

          case CondWithNotExpr_CollectionMemberExpr(expr) =>
            collectionMemberExpr(expr)
        }

        if (not) !res else res

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

        if (not) !res else res
    }
  }

  def betweenExpr(expr: BetweenExpr) = {
    (scalarOrSubselectExpr(expr.min), scalarOrSubselectExpr(expr.max))
  }

  def inExpr(expr: InExpr): Boolean = {
    expr match {
      case InExpr_InputParam(expr) =>
        inputParam(expr)
      case InExpr_ScalarOrSubselectExpr(expr, exprs) =>
        scalarOrSubselectExpr(expr)
        exprs map scalarOrSubselectExpr
      case InExpr_Subquery(expr: Subquery) =>
        subquery(expr)
    }
    true
    // TODO
  }

  def likeExpr(expr: LikeExpr) = {
    scalarOrSubselectExpr(expr.like) match {
      case like: CharSequence =>
        val escape = expr.escape match {
          case Some(x) =>
            scalarExpr(x.expr) match {
              case c: CharSequence => Some(c.toString)
              case x               => throw JPQLRuntimeException(x, "is not a string")
            }
          case None => None
        }
        (like.toString, escape)
      case x => throw JPQLRuntimeException(x, "is not a string")
    }
  }

  def collectionMemberExpr(expr: CollectionMemberExpr): Boolean = {
    collectionValuedPathExpr(expr.of)
    true // TODO
  }

  def existsExpr(expr: ExistsExpr): Boolean = {
    subquery(expr.subquery)
    true // TODO
  }

  def comparsionExprRightOperand(expr: ComparsionExprRightOperand) = {
    expr match {
      case ComparsionExprRightOperand_ArithExpr(expr)          => arithExpr(expr)
      case ComparsionExprRightOperand_NonArithScalarExpr(expr) => nonArithScalarExpr(expr)
      case ComparsionExprRightOperand_AnyOrAllExpr(expr)       => anyOrAllExpr(expr)
    }
  }

  def arithExpr(expr: ArithExpr) = {
    expr.expr match {
      case Left(expr)  => simpleArithExpr(expr)
      case Right(expr) => subquery(expr)
    }
  }

  def simpleArithExpr(expr: SimpleArithExpr): Any = {
    expr.rightTerms.foldLeft(arithTerm(expr.term)) {
      case (acc, ArithTerm_Plus(term))  => JPQLFunctions.plus(acc, arithTerm(term))
      case (acc, ArithTerm_Minus(term)) => JPQLFunctions.minus(acc, arithTerm(term))
    }
  }

  def arithTerm(term: ArithTerm): Any = {
    term.rightFactors.foldLeft(arithFactor(term.factor)) {
      case (acc, ArithFactor_Multiply(factor)) => JPQLFunctions.multiply(acc, arithFactor(factor))
      case (acc, ArithFactor_Divide(factor))   => JPQLFunctions.divide(acc, arithFactor(factor))
    }
  }

  def arithFactor(factor: ArithFactor): Any = {
    plusOrMinusPrimary(factor.primary)
  }

  def plusOrMinusPrimary(primary: PlusOrMinusPrimary): Any = {
    primary match {
      case ArithPrimary_Plus(primary)  => arithPrimary(primary)
      case ArithPrimary_Minus(primary) => JPQLFunctions.neg(arithPrimary(primary))
    }
  }

  def arithPrimary(primary: ArithPrimary) = {
    primary match {
      case ArithPrimary_PathExprOrVarAccess(expr)    => pathExprOrVarAccess(expr)
      case ArithPrimary_InputParam(expr)             => inputParam(expr)
      case ArithPrimary_CaseExpr(expr)               => caseExpr(expr)
      case ArithPrimary_FuncsReturningNumerics(expr) => funcsReturningNumerics(expr)
      case ArithPrimary_SimpleArithExpr(expr)        => simpleArithExpr(expr)
      case ArithPrimary_LiteralNumeric(expr)         => expr
    }
  }

  def scalarExpr(expr: ScalarExpr): Any = {
    expr match {
      case ScalarExpr_SimpleArithExpr(expr)    => simpleArithExpr(expr)
      case ScalarExpr_NonArithScalarExpr(expr) => nonArithScalarExpr(expr)
    }
  }

  def scalarOrSubselectExpr(expr: ScalarOrSubselectExpr) = {
    expr match {
      case ScalarOrSubselectExpr_ArithExpr(expr)          => arithExpr(expr)
      case ScalarOrSubselectExpr_NonArithScalarExpr(expr) => nonArithScalarExpr(expr)
    }
  }

  def nonArithScalarExpr(expr: NonArithScalarExpr): Any = {
    expr match {
      case NonArithScalarExpr_FuncsReturningDatetime(expr) => funcsReturningDatetime(expr)
      case NonArithScalarExpr_FuncsReturningStrings(expr)  => funcsReturningStrings(expr)
      case NonArithScalarExpr_LiteralString(expr)          => expr
      case NonArithScalarExpr_LiteralBoolean(expr)         => expr
      case NonArithScalarExpr_LiteralTemporal(expr)        => expr
      case NonArithScalarExpr_EntityTypeExpr(expr)         => entityTypeExpr(expr)
    }
  }

  def anyOrAllExpr(expr: AnyOrAllExpr) = {
    val subq = subquery(expr.subquery)
    expr.anyOrAll match {
      case ALL  =>
      case ANY  =>
      case SOME =>
    }
  }

  def entityTypeExpr(expr: EntityTypeExpr) = {
    typeDiscriminator(expr.typeDis)
  }

  def typeDiscriminator(expr: TypeDiscriminator) = {
    expr.expr match {
      case Left(expr1)  => varOrSingleValuedPath(expr1)
      case Right(expr1) => inputParam(expr1)
    }
  }

  def caseExpr(expr: CaseExpr): Any = {
    expr match {
      case CaseExpr_SimpleCaseExpr(expr) =>
        simpleCaseExpr(expr)
      case CaseExpr_GeneralCaseExpr(expr) =>
        generalCaseExpr(expr)
      case CaseExpr_CoalesceExpr(expr) =>
        coalesceExpr(expr)
      case CaseExpr_NullifExpr(expr) =>
        nullifExpr(expr)
    }
  }

  def simpleCaseExpr(expr: SimpleCaseExpr) = {
    caseOperand(expr.caseOperand)
    simpleWhenClause(expr.when)
    expr.whens foreach { when =>
      simpleWhenClause(when)
    }
    val elseExpr = scalarExpr(expr.elseExpr)
  }

  def generalCaseExpr(expr: GeneralCaseExpr) = {
    whenClause(expr.when)
    expr.whens foreach { when =>
      whenClause(when)
    }
    val elseExpr = scalarExpr(expr.elseExpr)
  }

  def coalesceExpr(expr: CoalesceExpr) = {
    scalarExpr(expr.expr)
    expr.exprs map scalarExpr
  }

  def nullifExpr(expr: NullifExpr) = {
    val left = scalarExpr(expr.leftExpr)
    val right = scalarExpr(expr.rightExpr)
    left // TODO
  }

  def caseOperand(expr: CaseOperand) = {
    expr.expr match {
      case Left(x)  => stateFieldPathExpr(x)
      case Right(x) => typeDiscriminator(x)
    }
  }

  def whenClause(whenClause: WhenClause) = {
    val when = condExpr(whenClause.when)
    val thenExpr = scalarExpr(whenClause.thenExpr)
  }

  def simpleWhenClause(whenClause: SimpleWhenClause) = {
    val when = scalarExpr(whenClause.when)
    val thenExpr = scalarExpr(whenClause.thenExpr)
  }

  def varOrSingleValuedPath(expr: VarOrSingleValuedPath) = {
    expr.expr match {
      case Left(x)  => singleValuedPathExpr(x)
      case Right(x) => varAccessOrTypeConstant(x)
    }
  }

  def stringPrimary(expr: StringPrimary): Either[String, Any => String] = {
    expr match {
      case StringPrimary_LiteralString(expr) => Left(expr)
      case StringPrimary_FuncsReturningStrings(expr) =>
        try {
          Left(funcsReturningStrings(expr))
        } catch {
          case ex: Throwable => throw ex
        }
      case StringPrimary_InputParam(expr) =>
        val param = inputParam(expr)
        Right(param => "") // TODO
      case StringPrimary_StateFieldPathExpr(expr) =>
        pathExpr(expr.path) match {
          case x: CharSequence => Left(x.toString)
          case x               => throw JPQLRuntimeException(x, "is not a StringPrimary")
        }
    }
  }

  def inputParam(expr: InputParam) = {
    expr match {
      case InputParam_Named(name)   => name
      case InputParam_Position(pos) => pos
    }
  }

  def funcsReturningNumerics(expr: FuncsReturningNumerics): Number = {
    expr match {
      case Abs(expr) =>
        val v = simpleArithExpr(expr)
        JPQLFunctions.abs(v)

      case Length(expr) =>
        scalarExpr(expr) match {
          case x: CharSequence => x.length
          case x               => throw JPQLRuntimeException(x, "is not a string")
        }

      case Mod(expr, divisorExpr) =>
        scalarExpr(expr) match {
          case dividend: Number =>
            scalarExpr(divisorExpr) match {
              case divisor: Number => dividend.intValue % divisor.intValue
              case x               => throw JPQLRuntimeException(x, "divisor is not a number")
            }
          case x => throw JPQLRuntimeException(x, "dividend is not a number")
        }

      case Locate(expr, searchExpr, startExpr) =>
        scalarExpr(expr) match {
          case base: CharSequence =>
            scalarExpr(searchExpr) match {
              case searchStr: CharSequence =>
                val start = startExpr match {
                  case Some(exprx) =>
                    scalarExpr(exprx) match {
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
        collectionValuedPathExpr(expr)
        // todo return size of elements of the collection member TODO
        0

      case Sqrt(expr) =>
        scalarExpr(expr) match {
          case x: Number => math.sqrt(x.doubleValue)
          case x         => throw JPQLRuntimeException(x, "is not a number")
        }

      case Index(expr) =>
        varAccessOrTypeConstant(expr)
        // TODO
        0

      case Func(name, args) =>
        // try to call function: name(as: _*) TODO
        val as = args map newValue
        0
    }
  }

  def funcsReturningDatetime(expr: FuncsReturningDatetime): Temporal = {
    expr match {
      case CURRENT_DATE      => JPQLFunctions.currentDate()
      case CURRENT_TIME      => JPQLFunctions.currentTime()
      case CURRENT_TIMESTAMP => JPQLFunctions.currentDateTime()
    }
  }

  def funcsReturningStrings(expr: FuncsReturningStrings): String = {
    expr match {
      case Concat(expr, exprs: List[ScalarExpr]) =>
        scalarExpr(expr) match {
          case base: CharSequence =>
            (exprs map scalarExpr).foldLeft(new StringBuilder(base.toString)) {
              case (sb, x: CharSequence) => sb.append(x)
              case x                     => throw JPQLRuntimeException(x, "is not a string")
            }.toString
          case x => throw JPQLRuntimeException(x, "is not a string")
        }

      case Substring(expr, startExpr, lengthExpr: Option[ScalarExpr]) =>
        scalarExpr(expr) match {
          case base: CharSequence =>
            scalarExpr(startExpr) match {
              case start: Number =>
                val end = (lengthExpr map scalarExpr) match {
                  case Some(length: Number) => start.intValue + length.intValue - 1
                  case _                    => base.length - 1
                }
                base.toString.substring(start.intValue - 1, end)
              case x => throw JPQLRuntimeException(x, "is not a number")
            }
          case x => throw JPQLRuntimeException(x, "is not a string")
        }

      case Trim(trimSpec: Option[TrimSpec], trimChar: Option[TrimChar], from) =>
        val base = stringPrimary(from) match {
          case Left(x)  => x
          case Right(x) => x("") // TODO
        }
        val trimC = trimChar match {
          case Some(TrimChar_String(char)) => char
          case Some(TrimChar_InputParam(param)) =>
            inputParam(param)
            "" // TODO
          case None => ""
        }
        trimSpec match {
          case Some(BOTH) | None => base.replaceAll("^" + trimC + "|" + trimC + "$", "")
          case Some(LEADING)     => base.replaceAll("^" + trimC, "")
          case Some(TRAILING)    => base.replaceAll(trimC + "$", "")
        }

      case Upper(expr) =>
        scalarExpr(expr) match {
          case base: CharSequence => base.toString.toUpperCase
          case x                  => throw JPQLRuntimeException(x, "is not a string")
        }

      case Lower(expr) =>
        scalarExpr(expr) match {
          case base: CharSequence => base.toString.toLowerCase
          case x                  => throw JPQLRuntimeException(x, "is not a string")
        }
    }
  }

  def subquery(subquery: Subquery) = {
    val select = simpleSelectClause(subquery.select)
    val from = subqueryFromClause(subquery.from)
    val where = subquery.where match {
      case Some(x) => whereClause(x)
      case None    =>
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

  def simpleSelectClause(select: SimpleSelectClause) = {
    val isDistinct = select.isDistinct
    simpleSelectExpr(select.expr)
  }

  def simpleSelectExpr(expr: SimpleSelectExpr) = {
    expr match {
      case SimpleSelectExpr_SingleValuedPathExpr(expr)    => singleValuedPathExpr(expr)
      case SimpleSelectExpr_AggregateExpr(expr)           => aggregateExpr(expr)
      case SimpleSelectExpr_VarAccessOrTypeConstant(expr) => varAccessOrTypeConstant(expr)
    }
  }

  def subqueryFromClause(fromClause: SubqueryFromClause) = {
    val from = subselectIdentVarDecl(fromClause.from)
    val froms = fromClause.froms map {
      case Left(x)  => subselectIdentVarDecl(x)
      case Right(x) => collectionMemberDecl(x)
    }
  }

  def subselectIdentVarDecl(ident: SubselectIdentVarDecl) = {
    ident match {
      case SubselectIdentVarDecl_IdentVarDecl(expr) =>
        identVarDecl(expr)
      case SubselectIdentVarDecl_AssocPathExpr(expr, as) =>
        assocPathExpr(expr)
        as.ident
      case SubselectIdentVarDecl_CollectionMemberDecl(expr) =>
        collectionMemberDecl(expr)
    }
  }

  def orderbyClause(orderbyClause: OrderbyClause) = {
    val orderby = orderbyItem(orderbyClause.orderby)
    orderbyClause.orderbys map orderbyItem
  }

  def orderbyItem(item: OrderbyItem) = {
    item.expr match {
      case Left(x)  => simpleArithExpr(x)
      case Right(x) => scalarExpr(x)
    }

    item.isAsc
  }

  def groupbyClause(groupbyClause: GroupbyClause) = {
    val expr = scalarExpr(groupbyClause.expr)
    val exprs = groupbyClause.exprs map scalarExpr
  }

  def havingClause(having: HavingClause) = {
    condExpr(having.condExpr)
  }

}

