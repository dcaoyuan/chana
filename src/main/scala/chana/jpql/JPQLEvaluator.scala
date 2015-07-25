package chana.jpql

import chana.jpql.nodes._
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.temporal.Temporal
import org.apache.avro.generic.GenericData.Record

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
      case _                                 => throw new RuntimeException("not a number")
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
      case _                                 => throw new RuntimeException("not a number")
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
      case _                                 => throw new RuntimeException("not a number")
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
      case _                                 => throw new RuntimeException("not a number")
    }
  }

  def neg(v: Any): Number = {
    v match {
      case x: java.lang.Double  => -x
      case x: java.lang.Float   => -x
      case x: java.lang.Long    => -x
      case x: java.lang.Integer => -x
      case _                    => throw new RuntimeException("not a number")
    }
  }

  def abs(v: Any): Number = {
    v match {
      case x: java.lang.Integer => math.abs(x)
      case x: java.lang.Long    => math.abs(x)
      case x: java.lang.Float   => math.abs(x)
      case x: java.lang.Double  => math.abs(x)
      case _                    => throw new RuntimeException("not a number")
    }
  }

  def eq(left: Any, right: Any) = {
    (left, right) match {
      case (x: Number, y: Number) => x == y
      case (x: String, y: String) => x == y
      case (x: LocalTime, y: LocalTime) => !(x.isAfter(y) || x.isBefore(y)) // ??
      case (x: LocalDate, y: LocalDate) => x.isEqual(y)
      case (x: LocalDateTime, y: LocalDateTime) => x.isEqual(y)
      case (x: Temporal, y: Temporal) => x == y
      case (x: java.lang.Boolean, y: java.lang.Boolean) => x == y
      case _ => throw new RuntimeException("can not apply EQ")
    }
  }

  def ne(left: Any, right: Any) = {
    (left, right) match {
      case (x: Number, y: Number) => x != y
      case (x: String, y: String) => x != y
      case (x: LocalTime, y: LocalTime) => x.isAfter(y) || x.isBefore(y)
      case (x: LocalDate, y: LocalDate) => !x.isEqual(y)
      case (x: LocalDateTime, y: LocalDateTime) => !x.isEqual(y)
      case (x: Temporal, y: Temporal) => x != y
      case (x: java.lang.Boolean, y: java.lang.Boolean) => x != y
      case _ => throw new RuntimeException("can not apply NE")
    }
  }

  def gt(left: Any, right: Any) = {
    (left, right) match {
      case (x: Number, y: Number)               => x.doubleValue > y.doubleValue
      case (x: LocalTime, y: LocalTime)         => x.isAfter(y)
      case (x: LocalDate, y: LocalDate)         => x.isAfter(y)
      case (x: LocalDateTime, y: LocalDateTime) => x.isAfter(y)
      case _                                    => throw new RuntimeException("can not apply GT")
    }
  }

  def ge(left: Any, right: Any) = {
    (left, right) match {
      case (x: Number, y: Number)               => x.doubleValue >= y.doubleValue
      case (x: LocalTime, y: LocalTime)         => x.isAfter(y) || !x.isBefore(y)
      case (x: LocalDate, y: LocalDate)         => x.isAfter(y) || x.isEqual(y)
      case (x: LocalDateTime, y: LocalDateTime) => x.isAfter(y) || x.isEqual(y)
      case _                                    => throw new RuntimeException("can not apply GE")
    }
  }

  def lt(left: Any, right: Any) = {
    (left, right) match {
      case (x: Number, y: Number)               => x.doubleValue < y.doubleValue
      case (x: LocalTime, y: LocalTime)         => x.isBefore(y)
      case (x: LocalDate, y: LocalDate)         => x.isBefore(y)
      case (x: LocalDateTime, y: LocalDateTime) => x.isBefore(y)
      case _                                    => throw new RuntimeException("can not apply LT")
    }
  }
  def le(left: Any, right: Any) = {
    (left, right) match {
      case (x: Number, y: Number)               => x.doubleValue <= y.doubleValue
      case (x: LocalTime, y: LocalTime)         => x.isBefore(y) || !x.isAfter(y)
      case (x: LocalDate, y: LocalDate)         => x.isBefore(y) || x.isEqual(y)
      case (x: LocalDateTime, y: LocalDateTime) => x.isBefore(y) || x.isEqual(y)
      case _                                    => throw new RuntimeException("can not apply LE")
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
      case _ => throw new RuntimeException("can not apply between")
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
        visitFromClause(from)
        visitSelectClause(select)
        val res = where match {
          case Some(x) =>
            if (visitWhereClause(x)) {
              selectScalars
            } else {
              List()
            }
          case None =>
            selectScalars
        }
        groupby match {
          case Some(x) => visitGroupbyClause(x)
          case None    =>
        }
        having match {
          case Some(x) => visitHavingClause(x)
          case None    =>
        }
        orderby match {
          case Some(x) => visitOrderbyClause(x)
          case None    =>
        }
        res
      case UpdateStatement(update, set, where) => // NOT YET
      case DeleteStatement(delete, where)      => // NOT YET
    }
  }

  def visitUpdateClause(updateClause: UpdateClause) = {
    val entityName = updateClause.entityName.ident
    updateClause.as foreach { x =>
      x.ident
    }
  }

  def visitSetClause(setClause: SetClause) = {
    val assign = visitSetAssignClause(setClause.assign)
    setClause.assigns foreach { x =>
      visitSetAssignClause(x)
    }
  }

  def visitSetAssignClause(assign: SetAssignClause) = {
    val target = visitSetAssignTarget(assign.target)
    val value = visitNewValue(assign.value)
  }

  def visitSetAssignTarget(target: SetAssignTarget) = {
    target.path match {
      case Left(x)  => visitPathExpr(x)
      case Right(x) => visitAttribute(x)
    }
  }

  def visitNewValue(expr: NewValue) = {
    visitScalarExpr(expr.v)
  }

  def visitDeleteClause(deleteClause: DeleteClause) = {
    val from = deleteClause.from.ident
    deleteClause.as foreach { x =>
      x.ident
    }
  }

  def visitSelectClause(select: SelectClause): Unit = {
    selectIsDistinct = select.isDistinct
    visitSelectItem(select.item)
    select.items foreach visitSelectItem
  }

  def visitSelectItem(item: SelectItem) = {
    val item1 = visitSelectExpr(item.expr)
    item.as foreach { x => asToItem += (x.ident -> item1) }
    item1
  }

  def visitSelectExpr(expr: SelectExpr) = {
    expr match {
      case SelectExpr_AggregateExpr(expr)   => selectAggregates ::= visitAggregateExpr(expr)
      case SelectExpr_ScalarExpr(expr)      => selectScalars ::= visitScalarExpr(expr)
      case SelectExpr_OBJECT(expr)          => selectObjects ::= visitVarAccessOrTypeConstant(expr)
      case SelectExpr_ConstructorExpr(expr) => selectNewInstances ::= visitConstructorExpr(expr)
      case SelectExpr_MapEntryExpr(expr)    => selectMapEntries ::= visitMapEntryExpr(expr)
    }
  }

  def visitMapEntryExpr(expr: MapEntryExpr): Any = {
    visitVarAccessOrTypeConstant(expr.entry)
  }

  def visitPathExprOrVarAccess(expr: PathExprOrVarAccess): Any = {
    val field = visitQualIdentVar(expr.qual)
    val paths = field :: (expr.attributes map visitAttribute)
    valueOf(paths)
  }

  def visitQualIdentVar(qual: QualIdentVar) = {
    qual match {
      case QualIdentVar_VarAccessOrTypeConstant(v) => visitVarAccessOrTypeConstant(v)
      case QualIdentVar_KEY(v)                     => visitVarAccessOrTypeConstant(v)
      case QualIdentVar_VALUE(v)                   => visitVarAccessOrTypeConstant(v)
    }
  }

  def visitAggregateExpr(expr: AggregateExpr) = {
    expr match {
      case AggregateExpr_AVG(isDistinct, expr) =>
        visitScalarExpr(expr)
      case AggregateExpr_MAX(isDistinct, expr) =>
        visitScalarExpr(expr)
      case AggregateExpr_MIN(isDistinct, expr) =>
        visitScalarExpr(expr)
      case AggregateExpr_SUM(isDistinct, expr) =>
        visitScalarExpr(expr)
      case AggregateExpr_COUNT(isDistinct, expr) =>
        visitScalarExpr(expr)
    }
  }

  def visitConstructorExpr(expr: ConstructorExpr) = {
    val fullname = visitConstructorName(expr.name)
    val args = visitConstructorItem(expr.arg) :: (expr.args map visitConstructorItem)
    null // NOT YET 
  }

  def visitConstructorName(name: ConstructorName): String = {
    val fullname = new StringBuilder(name.id.ident)
    name.ids foreach fullname.append(".").append
    fullname.toString
  }

  def visitConstructorItem(item: ConstructorItem) = {
    item match {
      case ConstructorItem_ScalarExpr(expr)    => visitScalarExpr(expr)
      case ConstructorItem_AggregateExpr(expr) => visitAggregateExpr(expr) // TODO aggregate here!?
    }
  }

  def visitFromClause(from: FromClause) = {
    visitIdentVarDecl(from.from)
    from.froms foreach {
      case Left(x)  => visitIdentVarDecl(x)
      case Right(x) => visitCollectionMemberDecl(x)
    }
  }

  def visitIdentVarDecl(ident: IdentVarDecl) = {
    visitRangeVarDecl(ident.range)
    ident.joins foreach { join =>
      visitJoin(join)
    }
  }

  def visitRangeVarDecl(range: RangeVarDecl): Unit = {
    asToEntity += (range.as.ident -> range.entityName.ident)
  }

  def visitJoin(join: Join) = {
    join match {
      case Join_General(joinSpec, expr, as, joinCond) =>
        joinSpec match {
          case JOIN            =>
          case LEFT_JOIN       =>
          case LEFT_OUTER_JOIN =>
          case INNER_JOIN      =>
        }
        visitJoinAssocPathExpr(expr)
        val as_ = as.ident
        joinCond match {
          case Some(x) => visitJoinCond(x)
          case None    =>
        }
      case Join_TREAT(joinSpec, expr, exprAs, as, joinCond) =>
        joinSpec match {
          case JOIN            =>
          case LEFT_JOIN       =>
          case LEFT_OUTER_JOIN =>
          case INNER_JOIN      =>
        }
        visitJoinAssocPathExpr(expr)
        val as_ = as.ident
        joinCond match {
          case Some(x) => visitJoinCond(x)
          case None    =>
        }
      case Join_FETCH(joinSpec, expr, alias, joinCond) =>
        joinSpec match {
          case JOIN            =>
          case LEFT_JOIN       =>
          case LEFT_OUTER_JOIN =>
          case INNER_JOIN      =>
        }
        visitJoinAssocPathExpr(expr)
        alias foreach { x =>
          x.ident
        }
        joinCond match {
          case Some(x) => visitJoinCond(x)
          case None    =>
        }
    }
  }

  def visitJoinCond(joinCond: JoinCond) = {
    visitCondExpr(joinCond.expr)
  }

  def visitCollectionMemberDecl(expr: CollectionMemberDecl): Unit = {
    val member = visitCollectionValuedPathExpr(expr.in)
    asToCollectionMember += (expr.as.ident -> member)
  }

  def visitCollectionValuedPathExpr(expr: CollectionValuedPathExpr) = {
    visitPathExpr(expr.path)
  }

  def visitAssocPathExpr(expr: AssocPathExpr) = {
    visitPathExpr(expr.path)
  }

  def visitJoinAssocPathExpr(expr: JoinAssocPathExpr) = {
    val qualId = visitQualIdentVar(expr.qualId)
    val attrbutes = expr.attrbutes map visitAttribute
  }

  def visitSingleValuedPathExpr(expr: SingleValuedPathExpr) = {
    visitPathExpr(expr.path)
  }

  def visitStateFieldPathExpr(expr: StateFieldPathExpr) = {
    visitPathExpr(expr.path)
  }

  def visitPathExpr(expr: PathExpr): Any = {
    val fieldName = visitQualIdentVar(expr.qual)
    var field = record.get(fieldName)
    var attrs = expr.attributes map visitAttribute
    while (attrs.nonEmpty && (field ne null)) {
      field.asInstanceOf[Record].get(attrs.head)
      attrs = attrs.tail
    }
    field
  }

  def visitAttribute(attr: Attribute): String = {
    attr.name
  }

  def visitVarAccessOrTypeConstant(expr: VarAccessOrTypeConstant): String = {
    expr.id.ident
  }

  def visitWhereClause(where: WhereClause): Boolean = {
    visitCondExpr(where.expr)
  }

  def visitCondExpr(expr: CondExpr): Boolean = {
    expr.orTerms.foldLeft(visitCondTerm(expr.term)) { (res, orTerm) =>
      res || visitCondTerm(orTerm)
    }
  }

  def visitCondTerm(term: CondTerm): Boolean = {
    term.andFactors.foldLeft(visitCondFactor(term.factor)) { (res, andFactor) =>
      res && visitCondFactor(andFactor)
    }
  }

  def visitCondFactor(factor: CondFactor): Boolean = {
    val res = factor.expr match {
      case Left(x)  => visitCondPrimary(x)
      case Right(x) => visitExistsExpr(x)
    }
    factor.isNot && res
  }

  def visitCondPrimary(primary: CondPrimary): Boolean = {
    primary match {
      case CondPrimary_CondExpr(expr)       => visitCondExpr(expr)
      case CondPrimary_SimpleCondExpr(expr) => visitSimpleCondExpr(expr)
    }
  }

  def visitSimpleCondExpr(expr: SimpleCondExpr): Boolean = {
    val base = expr.expr match {
      case Left(x)  => visitArithExpr(x)
      case Right(x) => visitNonArithScalarExpr(x)
    }

    // visitSimpleCondExprRem
    expr.rem match {
      case SimpleCondExprRem_ComparisonExpr(expr) =>
        // visitComparisionExpr
        val operand = visitComparsionExprRightOperand(expr.operand)
        expr.op match {
          case EQ => JPQLFunctions.eq(base, operand)
          case NE => JPQLFunctions.ne(base, operand)
          case GT => JPQLFunctions.gt(base, operand)
          case GE => JPQLFunctions.ge(base, operand)
          case LT => JPQLFunctions.lt(base, operand)
          case LE => JPQLFunctions.le(base, operand)
        }

      case SimpleCondExprRem_CondWithNotExpr(isNot, expr) =>
        // visitCondWithNotExpr
        expr match {
          case CondWithNotExpr_BetweenExpr(expr) =>
            val minMax = visitBetweenExpr(expr)
            JPQLFunctions.between(base, minMax._1, minMax._2)

          case CondWithNotExpr_LikeExpr(expr) =>
            base match {
              case x: String =>
                val like = visitLikeExpr(expr)
                JPQLFunctions.strLike(x, like._1, like._2)
            }

          case CondWithNotExpr_InExpr(expr) =>
            visitInExpr(expr)
            true // TODO

          case CondWithNotExpr_CollectionMemberExpr(expr) =>
            visitCollectionMemberExpr(expr)
            true // TODO
        }

      case SimpleCondExprRem_IsExpr(isNot, expr) =>
        // visitIsExpr
        expr match {
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
    }
  }

  def visitBetweenExpr(expr: BetweenExpr) = {
    (visitScalarOrSubselectExpr(expr.min), visitScalarOrSubselectExpr(expr.max))
  }

  def visitInExpr(expr: InExpr) = {
    expr match {
      case InExpr_InputParam(expr) =>
        visitInputParam(expr)
      case InExpr_ScalarOrSubselectExpr(expr, exprs) =>
        visitScalarOrSubselectExpr(expr)
        exprs map visitScalarOrSubselectExpr
      case InExpr_Subquery(expr: Subquery) =>
        visitSubquery(expr)
    }
  }

  def visitLikeExpr(expr: LikeExpr) = {
    visitScalarOrSubselectExpr(expr.like) match {
      case like: String =>
        val escape = expr.escape match {
          case Some(x) =>
            visitScalarExpr(x.expr) match {
              case c: String => Some(c)
              case _         => throw new RuntimeException("not a string")
            }
          case None => None
        }
        (like, escape)
      case _ => throw new RuntimeException("not a string")
    }
  }

  def visitCollectionMemberExpr(expr: CollectionMemberExpr) = {
    visitCollectionValuedPathExpr(expr.of)
  }

  def visitExistsExpr(expr: ExistsExpr): Boolean = {
    visitSubquery(expr.subquery)
    true // TODO
  }

  def visitComparsionExprRightOperand(expr: ComparsionExprRightOperand) = {
    expr match {
      case ComparsionExprRightOperand_ArithExpr(expr)          => visitArithExpr(expr)
      case ComparsionExprRightOperand_NonArithScalarExpr(expr) => visitNonArithScalarExpr(expr)
      case ComparsionExprRightOperand_AnyOrAllExpr(expr)       => visitAnyOrAllExpr(expr)
    }
  }

  def visitArithExpr(expr: ArithExpr) = {
    expr.expr match {
      case Left(expr)  => visitSimpleArithExpr(expr)
      case Right(expr) => visitSubquery(expr)
    }
  }

  def visitSimpleArithExpr(expr: SimpleArithExpr): Any = {
    expr.rightTerms.foldLeft(visitArithTerm(expr.term)) {
      case (acc, ArithTerm_Plus(term))  => JPQLFunctions.plus(acc, visitArithTerm(term))
      case (acc, ArithTerm_Minus(term)) => JPQLFunctions.minus(acc, visitArithTerm(term))
    }
  }

  def visitArithTerm(term: ArithTerm): Any = {
    term.rightFactors.foldLeft(visitArithFactor(term.factor)) {
      case (acc, ArithFactor_Multiply(factor)) => JPQLFunctions.multiply(acc, visitArithFactor(factor))
      case (acc, ArithFactor_Divide(factor))   => JPQLFunctions.divide(acc, visitArithFactor(factor))
    }
  }

  def visitArithFactor(factor: ArithFactor): Any = {
    visitPlusOrMinusPrimary(factor.primary)
  }

  def visitPlusOrMinusPrimary(primary: PlusOrMinusPrimary): Any = {
    primary match {
      case ArithPrimary_Plus(primary)  => visitArithPrimary(primary)
      case ArithPrimary_Minus(primary) => JPQLFunctions.neg(visitArithPrimary(primary))
    }
  }

  def visitArithPrimary(primary: ArithPrimary) = {
    primary match {
      case ArithPrimary_PathExprOrVarAccess(expr)    => visitPathExprOrVarAccess(expr)
      case ArithPrimary_InputParam(expr)             => visitInputParam(expr)
      case ArithPrimary_CaseExpr(expr)               => visitCaseExpr(expr)
      case ArithPrimary_FuncsReturningNumerics(expr) => visitFuncsReturningNumerics(expr)
      case ArithPrimary_SimpleArithExpr(expr)        => visitSimpleArithExpr(expr)
      case ArithPrimary_LiteralNumeric(expr)         => expr
    }
  }

  def visitScalarExpr(expr: ScalarExpr): Any = {
    expr match {
      case ScalarExpr_SimpleArithExpr(expr)    => visitSimpleArithExpr(expr)
      case ScalarExpr_NonArithScalarExpr(expr) => visitNonArithScalarExpr(expr)
    }
  }

  def visitScalarOrSubselectExpr(expr: ScalarOrSubselectExpr) = {
    expr match {
      case ScalarOrSubselectExpr_ArithExpr(expr)          => visitArithExpr(expr)
      case ScalarOrSubselectExpr_NonArithScalarExpr(expr) => visitNonArithScalarExpr(expr)
    }
  }

  def visitNonArithScalarExpr(expr: NonArithScalarExpr): Any = {
    expr match {
      case NonArithScalarExpr_FuncsReturningDatetime(expr) => visitFuncsReturningDatetime(expr)
      case NonArithScalarExpr_FuncsReturningStrings(expr)  => visitFuncsReturningStrings(expr)
      case NonArithScalarExpr_LiteralString(expr)          => expr
      case NonArithScalarExpr_LiteralBoolean(expr)         => expr
      case NonArithScalarExpr_LiteralTemporal(expr)        => expr
      case NonArithScalarExpr_EntityTypeExpr(expr)         => visitEntityTypeExpr(expr)
    }
  }

  def visitAnyOrAllExpr(expr: AnyOrAllExpr) = {
    val subquery = visitSubquery(expr.subquery)
    expr.anyOrAll match {
      case ALL  =>
      case ANY  =>
      case SOME =>
    }
  }

  def visitEntityTypeExpr(expr: EntityTypeExpr) = {
    visitTypeDiscriminator(expr.typeDis)
  }

  def visitTypeDiscriminator(expr: TypeDiscriminator) = {
    expr.expr match {
      case Left(expr1)  => visitVarOrSingleValuedPath(expr1)
      case Right(expr1) => visitInputParam(expr1)
    }
  }

  def visitCaseExpr(expr: CaseExpr): Any = {
    expr match {
      case CaseExpr_SimpleCaseExpr(expr) =>
        visitSimpleCaseExpr(expr)
      case CaseExpr_GeneralCaseExpr(expr) =>
        visitGeneralCaseExpr(expr)
      case CaseExpr_CoalesceExpr(expr) =>
        visitCoalesceExpr(expr)
      case CaseExpr_NullifExpr(expr) =>
        visitNullifExpr(expr)
    }
  }

  def visitSimpleCaseExpr(expr: SimpleCaseExpr) = {
    visitCaseOperand(expr.caseOperand)
    visitSimpleWhenClause(expr.when)
    expr.whens foreach { when =>
      visitSimpleWhenClause(when)
    }
    val elseExpr = visitScalarExpr(expr.elseExpr)
  }

  def visitGeneralCaseExpr(expr: GeneralCaseExpr) = {
    visitWhenClause(expr.when)
    expr.whens foreach { when =>
      visitWhenClause(when)
    }
    val elseExpr = visitScalarExpr(expr.elseExpr)
  }

  def visitCoalesceExpr(expr: CoalesceExpr) = {
    visitScalarExpr(expr.expr)
    expr.exprs map visitScalarExpr
  }

  def visitNullifExpr(expr: NullifExpr) = {
    val left = visitScalarExpr(expr.leftExpr)
    val right = visitScalarExpr(expr.rightExpr)
    left // TODO
  }

  def visitCaseOperand(expr: CaseOperand) = {
    expr.expr match {
      case Left(x)  => visitStateFieldPathExpr(x)
      case Right(x) => visitTypeDiscriminator(x)
    }
  }

  def visitWhenClause(whenClause: WhenClause) = {
    val when = visitCondExpr(whenClause.when)
    val thenExpr = visitScalarExpr(whenClause.thenExpr)
  }

  def visitSimpleWhenClause(whenClause: SimpleWhenClause) = {
    val when = visitScalarExpr(whenClause.when)
    val thenExpr = visitScalarExpr(whenClause.thenExpr)
  }

  def visitVarOrSingleValuedPath(expr: VarOrSingleValuedPath) = {
    expr.expr match {
      case Left(x)  => visitSingleValuedPathExpr(x)
      case Right(x) => visitVarAccessOrTypeConstant(x)
    }
  }

  def visitStringPrimary(expr: StringPrimary): Either[String, Any => String] = {
    expr match {
      case StringPrimary_LiteralString(expr) => Left(expr)
      case StringPrimary_FuncsReturningStrings(expr) =>
        try {
          Left(visitFuncsReturningStrings(expr))
        } catch {
          case ex: Throwable => throw ex
        }
      case StringPrimary_InputParam(expr) =>
        val param = visitInputParam(expr)
        Right(param => "") // TODO
      case StringPrimary_StateFieldPathExpr(expr) =>
        visitPathExpr(expr.path) match {
          case x: String => Left(x)
          case _         => throw new RuntimeException("not a StringPrimary")
        }
    }
  }

  def visitInputParam(expr: InputParam) = {
    expr match {
      case InputParam_Named(name)   => name
      case InputParam_Position(pos) => pos
    }
  }

  def visitFuncsReturningNumerics(expr: FuncsReturningNumerics): Number = {
    expr match {
      case Abs(expr) =>
        val v = visitSimpleArithExpr(expr)
        JPQLFunctions.abs(v)

      case Length(expr) =>
        visitScalarExpr(expr) match {
          case x: java.lang.CharSequence => x.length
          case _                         => throw new RuntimeException("not a string")
        }

      case Mod(expr, divisorExpr) =>
        visitScalarExpr(expr) match {
          case dividend: Number =>
            visitScalarExpr(divisorExpr) match {
              case divisor: Number => dividend.intValue % divisor.intValue
              case _               => throw new RuntimeException("divisor not a number")
            }
          case _ => throw new RuntimeException("dividend not a number")
        }

      case Locate(expr, searchExpr, startExpr) =>
        visitScalarExpr(expr) match {
          case base: String =>
            visitScalarExpr(searchExpr) match {
              case searchStr: String =>
                val start = startExpr match {
                  case Some(exprx) =>
                    visitScalarExpr(exprx) match {
                      case x: java.lang.Integer => x - 1
                      case _                    => throw new RuntimeException("start is not an integer")
                    }
                  case None => 0
                }
                base.indexOf(searchStr, start)
              case _ => throw new RuntimeException("not a string")
            }
          case _ => throw new RuntimeException("not a string")
        }

      case Size(expr) =>
        visitCollectionValuedPathExpr(expr)
        // todo return size of elements of the collection member TODO
        0

      case Sqrt(expr) =>
        visitScalarExpr(expr) match {
          case x: Number => math.sqrt(x.doubleValue)
          case _         => throw new RuntimeException("not a number")
        }

      case Index(expr) =>
        visitVarAccessOrTypeConstant(expr)
        // TODO
        0

      case Func(name, args) =>
        // try to call function: name(as: _*) TODO
        val as = args map visitNewValue
        0
    }
  }

  def visitFuncsReturningDatetime(expr: FuncsReturningDatetime): Temporal = {
    expr match {
      case CURRENT_DATE      => JPQLFunctions.currentDate()
      case CURRENT_TIME      => JPQLFunctions.currentTime()
      case CURRENT_TIMESTAMP => JPQLFunctions.currentDateTime()
    }
  }

  def visitFuncsReturningStrings(expr: FuncsReturningStrings): String = {
    expr match {
      case Concat(expr, exprs: List[ScalarExpr]) =>
        visitScalarExpr(expr) match {
          case base: String =>
            (exprs map visitScalarExpr).foldLeft(new StringBuilder(base)) {
              case (sb, x: String) => sb.append(x)
              case _               => throw new RuntimeException("not a string")
            }.toString
          case _ => throw new RuntimeException("not a string")
        }

      case Substring(expr, startExpr, lengthExpr: Option[ScalarExpr]) =>
        visitScalarExpr(expr) match {
          case base: String =>
            visitScalarExpr(startExpr) match {
              case start: Number =>
                val end = (lengthExpr map visitScalarExpr) match {
                  case Some(length: Number) => start.intValue + length.intValue
                  case _                    => base.length
                }
                base.substring(start.intValue - 1, end)
              case _ => throw new RuntimeException("not a number")
            }
          case _ => throw new RuntimeException("not a string")
        }

      case Trim(trimSpec: Option[TrimSpec], trimChar: Option[TrimChar], from) =>
        val base = visitStringPrimary(from) match {
          case Left(x)  => x
          case Right(x) => x("") // TODO
        }
        val trimC = trimChar match {
          case Some(TrimChar_String(char)) => char
          case Some(TrimChar_InputParam(param)) =>
            visitInputParam(param)
            "" // TODO
          case None => ""
        }
        trimSpec match {
          case Some(BOTH) | None => base.replaceAll("^" + trimC + "|" + trimC + "$", "")
          case Some(LEADING)     => base.replaceAll("^" + trimC, "")
          case Some(TRAILING)    => base.replaceAll(trimC + "$", "")
        }

      case Upper(expr) =>
        visitScalarExpr(expr) match {
          case base: String => base.toUpperCase
          case _            => throw new RuntimeException("not a string")
        }

      case Lower(expr) =>
        visitScalarExpr(expr) match {
          case base: String => base.toLowerCase
          case _            => throw new RuntimeException("not a string")
        }
    }
  }

  def visitSubquery(subquery: Subquery) = {
    val select = visitSimpleSelectClause(subquery.select)
    val from = visitSubqueryFromClause(subquery.from)
    val where = subquery.where match {
      case Some(x) => visitWhereClause(x)
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

  def visitSimpleSelectClause(select: SimpleSelectClause) = {
    val isDistinct = select.isDistinct
    visitSimpleSelectExpr(select.expr)
  }

  def visitSimpleSelectExpr(expr: SimpleSelectExpr) = {
    expr match {
      case SimpleSelectExpr_SingleValuedPathExpr(expr)    => visitSingleValuedPathExpr(expr)
      case SimpleSelectExpr_AggregateExpr(expr)           => visitAggregateExpr(expr)
      case SimpleSelectExpr_VarAccessOrTypeConstant(expr) => visitVarAccessOrTypeConstant(expr)
    }
  }

  def visitSubqueryFromClause(fromClause: SubqueryFromClause) = {
    val from = visitSubselectIdentVarDecl(fromClause.from)
    val froms = fromClause.froms map {
      case Left(x)  => visitSubselectIdentVarDecl(x)
      case Right(x) => visitCollectionMemberDecl(x)
    }
  }

  def visitSubselectIdentVarDecl(ident: SubselectIdentVarDecl) = {
    ident match {
      case SubselectIdentVarDecl_IdentVarDecl(expr) =>
        visitIdentVarDecl(expr)
      case SubselectIdentVarDecl_AssocPathExpr(expr, as) =>
        visitAssocPathExpr(expr)
        as.ident
      case SubselectIdentVarDecl_CollectionMemberDecl(expr) =>
        visitCollectionMemberDecl(expr)
    }
  }

  def visitOrderbyClause(orderbyClause: OrderbyClause) = {
    val orderby = visitOrderbyItem(orderbyClause.orderby)
    orderbyClause.orderbys map visitOrderbyItem
  }

  def visitOrderbyItem(item: OrderbyItem) = {
    item.expr match {
      case Left(x)  => visitSimpleArithExpr(x)
      case Right(x) => visitScalarExpr(x)
    }

    item.isAsc
  }

  def visitGroupbyClause(groupbyClause: GroupbyClause) = {
    val expr = visitScalarExpr(groupbyClause.expr)
    val exprs = groupbyClause.exprs map visitScalarExpr
  }

  def visitHavingClause(having: HavingClause) = {
    visitCondExpr(having.condExpr)
  }

}

