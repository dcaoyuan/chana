package chana.jpql

import chana.jpql.nodes._
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime

class JPQLVisitor {

  def updateClause(updateClause: UpdateClause, record: Any): Any = {
    updateClause.as foreach { x => }
  }

  def setClause(setClause: SetClause, record: Any): Any = {
    setAssignClause(setClause.assign, record)
    setClause.assigns foreach { x => setAssignClause(x, record) }
  }

  def setAssignClause(assign: SetAssignClause, record: Any): Any = {
    setAssignTarget(assign.target, record)
    newValue(assign.value, record)
  }

  def setAssignTarget(target: SetAssignTarget, record: Any): Any = {
    target.path match {
      case Left(x)  => pathExpr(x, record)
      case Right(x) => attribute(x, record)
    }
  }

  def newValue(expr: NewValue, record: Any): Any = {
    scalarExpr(expr.v, record)
  }

  def deleteClause(deleteClause: DeleteClause, record: Any): Any = {
    deleteClause.as foreach { x => x.ident }
  }

  def selectClause(select: SelectClause, record: Any): Any = {
    selectItem(select.item, record)
    select.items foreach { x => selectItem(x, record) }
  }

  def selectItem(item: SelectItem, record: Any): Any = {
    selectExpr(item.expr, record)
    item.as foreach { x => x.ident }
  }

  def selectExpr(expr: SelectExpr, record: Any): Any = {
    expr match {
      case SelectExpr_AggregateExpr(expr)   => aggregateExpr(expr, record)
      case SelectExpr_ScalarExpr(expr)      => scalarExpr(expr, record)
      case SelectExpr_OBJECT(expr)          => varAccessOrTypeConstant(expr, record)
      case SelectExpr_ConstructorExpr(expr) => constructorExpr(expr, record)
      case SelectExpr_MapEntryExpr(expr)    => mapEntryExpr(expr, record)
    }
  }

  // SELECT ENTRY(e.contactInfo) from Employee e
  def mapEntryExpr(expr: MapEntryExpr, record: Any): Any = {
    varAccessOrTypeConstant(expr.entry, record)
  }

  def pathExprOrVarAccess(expr: PathExprOrVarAccess, record: Any): Any = {
    qualIdentVar(expr.qual, record)
    expr.attributes map { x => attribute(x, record) }
  }

  // SELECT e from Employee e join e.contactInfo c where KEY(c) = 'Email' and VALUE(c) = 'joe@gmail.com'
  def qualIdentVar(qual: QualIdentVar, record: Any): Any = {
    qual match {
      case QualIdentVar_VarAccessOrTypeConstant(v) => varAccessOrTypeConstant(v, record)
      case QualIdentVar_KEY(v)                     => varAccessOrTypeConstant(v, record)
      case QualIdentVar_VALUE(v)                   => varAccessOrTypeConstant(v, record)
    }
  }

  def aggregateExpr(expr: AggregateExpr, record: Any): Any = {
    expr match {
      case AggregateExpr_AVG(isDistinct, expr)   => scalarExpr(expr, record)
      case AggregateExpr_MAX(isDistinct, expr)   => scalarExpr(expr, record)
      case AggregateExpr_MIN(isDistinct, expr)   => scalarExpr(expr, record)
      case AggregateExpr_SUM(isDistinct, expr)   => scalarExpr(expr, record)
      case AggregateExpr_COUNT(isDistinct, expr) => scalarExpr(expr, record)
    }
  }

  def constructorExpr(expr: ConstructorExpr, record: Any): Any = {
    constructorName(expr.name, record)
    constructorItem(expr.arg, record) :: (expr.args map { x => constructorItem(x, record) })
  }

  def constructorName(name: ConstructorName, record: Any): Any = {
    val fullname = new StringBuilder(name.id.ident)
    name.ids foreach fullname.append(".").append
    fullname.toString
  }

  def constructorItem(item: ConstructorItem, record: Any): Any = {
    item match {
      case ConstructorItem_ScalarExpr(expr)    => scalarExpr(expr, record)
      case ConstructorItem_AggregateExpr(expr) => aggregateExpr(expr, record) // TODO aggregate here!?
    }
  }

  def fromClause(from: FromClause, record: Any): Any = {
    identVarDecl(from.from, record)
    from.froms foreach {
      case Left(x)  => identVarDecl(x, record)
      case Right(x) => collectionMemberDecl(x, record)
    }
  }

  def identVarDecl(ident: IdentVarDecl, record: Any): Any = {
    rangeVarDecl(ident.range, record)
    ident.joins foreach { x => join(x, record) }
  }

  def rangeVarDecl(range: RangeVarDecl, record: Any): Any = {
    (range.as.ident.toLowerCase -> range.entityName.ident.toLowerCase)
  }

  /**
   *  The JOIN clause allows any of the object's relationships to be joined into
   *  the query so they can be used in the WHERE clause. JOIN does not mean the
   *  relationships will be fetched, unless the FETCH option is included.
   */
  def join(join: Join, record: Any): Any = {
    join match {
      case Join_General(spec, expr, as, cond) =>
        spec match {
          case JOIN            =>
          case LEFT_JOIN       =>
          case LEFT_OUTER_JOIN =>
          case INNER_JOIN      =>
        }
        joinAssocPathExpr(expr, record)
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
        cond match {
          case Some(x) => joinCond(x, record)
          case None    =>
        }
    }
  }

  def joinCond(joinCond: JoinCond, record: Any): Any = {
    condExpr(joinCond.expr, record)
  }

  def collectionMemberDecl(expr: CollectionMemberDecl, record: Any): Any = {
    val member = collectionValuedPathExpr(expr.in, record)
    (expr.as.ident -> member)
  }

  def collectionValuedPathExpr(expr: CollectionValuedPathExpr, record: Any): Any = {
    pathExpr(expr.path, record)
  }

  def assocPathExpr(expr: AssocPathExpr, record: Any): Any = {
    pathExpr(expr.path, record)
  }

  def joinAssocPathExpr(expr: JoinAssocPathExpr, record: Any): Any = {
    qualIdentVar(expr.qualId, record)
    expr.attrbutes map { x => attribute(x, record) }
  }

  def singleValuedPathExpr(expr: SingleValuedPathExpr, record: Any): Any = {
    pathExpr(expr.path, record)
  }

  def stateFieldPathExpr(expr: StateFieldPathExpr, record: Any): Any = {
    pathExpr(expr.path, record)
  }

  def pathExpr(expr: PathExpr, record: Any): Any = {
    qualIdentVar(expr.qual, record)
    expr.attributes map { x => attribute(x, record) }
  }

  def attribute(attr: Attribute, record: Any): String = {
    attr.name
  }

  def varAccessOrTypeConstant(expr: VarAccessOrTypeConstant, record: Any): String = {
    expr.id.ident
  }

  def whereClause(where: WhereClause, record: Any): Any = {
    condExpr(where.expr, record)
  }

  def condExpr(expr: CondExpr, record: Any): Any = {
    expr.orTerms.foldLeft(condTerm(expr.term, record)) { (acc, orTerm) =>
      condTerm(orTerm, record)
    }
  }

  def condTerm(term: CondTerm, record: Any): Any = {
    term.andFactors.foldLeft(condFactor(term.factor, record)) { (acc, andFactor) =>
      condFactor(andFactor, record)
    }
  }

  def condFactor(factor: CondFactor, record: Any): Any = {
    factor.expr match {
      case Left(x)  => condPrimary(x, record)
      case Right(x) => existsExpr(x, record)
    }
    factor.not
  }

  def condPrimary(primary: CondPrimary, record: Any): Any = {
    primary match {
      case CondPrimary_CondExpr(expr)       => condExpr(expr, record)
      case CondPrimary_SimpleCondExpr(expr) => simpleCondExpr(expr, record)
    }
  }

  def simpleCondExpr(expr: SimpleCondExpr, record: Any): Any = {
    expr.expr match {
      case Left(x)  => arithExpr(x, record)
      case Right(x) => nonArithScalarExpr(x, record)
    }

    // simpleCondExprRem
    expr.rem match {
      case SimpleCondExprRem_ComparisonExpr(expr) =>
        // comparisionExpr
        val operand = comparsionExprRightOperand(expr.operand, record)
        expr.op
      case SimpleCondExprRem_CondWithNotExpr(not, expr) =>
        // condWithNotExpr
        expr match {
          case CondWithNotExpr_BetweenExpr(expr)          => betweenExpr(expr, record)
          case CondWithNotExpr_LikeExpr(expr)             => likeExpr(expr, record)
          case CondWithNotExpr_InExpr(expr)               => inExpr(expr, record)
          case CondWithNotExpr_CollectionMemberExpr(expr) => collectionMemberExpr(expr, record)
        }
      case SimpleCondExprRem_IsExpr(not, expr) =>
        // isExpr
        expr match {
          case IsNullExpr  =>
          case IsEmptyExpr =>
        }
    }
  }

  def betweenExpr(expr: BetweenExpr, record: Any): Any = {
    scalarOrSubselectExpr(expr.min, record)
    scalarOrSubselectExpr(expr.max, record)
  }

  def inExpr(expr: InExpr, record: Any): Any = {
    expr match {
      case InExpr_InputParam(expr) => inputParam(expr, record)
      case InExpr_ScalarOrSubselectExpr(expr, exprs) =>
        scalarOrSubselectExpr(expr, record)
        exprs map { x => scalarOrSubselectExpr(x, record) }
      case InExpr_Subquery(expr: Subquery) => subquery(expr, record)
    }
  }

  def likeExpr(expr: LikeExpr, record: Any): Any = {
    scalarOrSubselectExpr(expr.like, record) match {
      case like: CharSequence =>
        expr.escape match {
          case Some(x) =>
            scalarExpr(x.expr, record) match {
              case c: CharSequence =>
              case x               => throw JPQLRuntimeException(x, "is not a string")
            }
          case None => None
        }
      case x => throw JPQLRuntimeException(x, "is not a string")
    }
  }

  def collectionMemberExpr(expr: CollectionMemberExpr, record: Any): Any = {
    collectionValuedPathExpr(expr.of, record)
  }

  def existsExpr(expr: ExistsExpr, record: Any): Any = {
    subquery(expr.subquery, record)
  }

  def comparsionExprRightOperand(expr: ComparsionExprRightOperand, record: Any): Any = {
    expr match {
      case ComparsionExprRightOperand_ArithExpr(expr)          => arithExpr(expr, record)
      case ComparsionExprRightOperand_NonArithScalarExpr(expr) => nonArithScalarExpr(expr, record)
      case ComparsionExprRightOperand_AnyOrAllExpr(expr)       => anyOrAllExpr(expr, record)
    }
  }

  def arithExpr(expr: ArithExpr, record: Any): Any = {
    expr.expr match {
      case Left(expr)  => simpleArithExpr(expr, record)
      case Right(expr) => subquery(expr, record)
    }
  }

  def simpleArithExpr(expr: SimpleArithExpr, record: Any): Any = {
    expr.rightTerms.foldLeft(arithTerm(expr.term, record)) {
      case (acc, ArithTerm_Plus(term))  => JPQLFunctions.plus(acc, arithTerm(term, record))
      case (acc, ArithTerm_Minus(term)) => JPQLFunctions.minus(acc, arithTerm(term, record))
    }
  }

  def arithTerm(term: ArithTerm, record: Any): Any = {
    term.rightFactors.foldLeft(arithFactor(term.factor, record)) {
      case (acc, ArithFactor_Multiply(factor)) => JPQLFunctions.multiply(acc, arithFactor(factor, record))
      case (acc, ArithFactor_Divide(factor))   => JPQLFunctions.divide(acc, arithFactor(factor, record))
    }
  }

  def arithFactor(factor: ArithFactor, record: Any): Any = {
    plusOrMinusPrimary(factor.primary, record)
  }

  def plusOrMinusPrimary(primary: PlusOrMinusPrimary, record: Any): Any = {
    primary match {
      case ArithPrimary_Plus(primary)  => arithPrimary(primary, record)
      case ArithPrimary_Minus(primary) => arithPrimary(primary, record)
    }
  }

  def arithPrimary(primary: ArithPrimary, record: Any): Any = {
    primary match {
      case ArithPrimary_PathExprOrVarAccess(expr)    => pathExprOrVarAccess(expr, record)
      case ArithPrimary_InputParam(expr)             => inputParam(expr, record)
      case ArithPrimary_CaseExpr(expr)               => caseExpr(expr, record)
      case ArithPrimary_FuncsReturningNumerics(expr) => funcsReturningNumerics(expr, record)
      case ArithPrimary_SimpleArithExpr(expr)        => simpleArithExpr(expr, record)
      case ArithPrimary_LiteralNumeric(expr)         => expr
    }
  }

  def scalarExpr(expr: ScalarExpr, record: Any): Any = {
    expr match {
      case ScalarExpr_SimpleArithExpr(expr)    => simpleArithExpr(expr, record)
      case ScalarExpr_NonArithScalarExpr(expr) => nonArithScalarExpr(expr, record)
    }
  }

  def scalarOrSubselectExpr(expr: ScalarOrSubselectExpr, record: Any): Any = {
    expr match {
      case ScalarOrSubselectExpr_ArithExpr(expr)          => arithExpr(expr, record)
      case ScalarOrSubselectExpr_NonArithScalarExpr(expr) => nonArithScalarExpr(expr, record)
    }
  }

  def nonArithScalarExpr(expr: NonArithScalarExpr, record: Any): Any = {
    expr match {
      case NonArithScalarExpr_FuncsReturningDatetime(expr) => funcsReturningDatetime(expr, record)
      case NonArithScalarExpr_FuncsReturningStrings(expr)  => funcsReturningStrings(expr, record)
      case NonArithScalarExpr_LiteralString(expr)          => expr
      case NonArithScalarExpr_LiteralBoolean(expr)         => expr
      case NonArithScalarExpr_LiteralTemporal(expr)        => expr
      case NonArithScalarExpr_EntityTypeExpr(expr)         => entityTypeExpr(expr, record)
    }
  }

  def anyOrAllExpr(expr: AnyOrAllExpr, record: Any): Any = {
    val subq = subquery(expr.subquery, record)
    expr.anyOrAll match {
      case ALL  =>
      case ANY  =>
      case SOME =>
    }
  }

  def entityTypeExpr(expr: EntityTypeExpr, record: Any): Any = {
    typeDiscriminator(expr.typeDis, record)
  }

  def typeDiscriminator(expr: TypeDiscriminator, record: Any): Any = {
    expr.expr match {
      case Left(expr1)  => varOrSingleValuedPath(expr1, record)
      case Right(expr1) => inputParam(expr1, record)
    }
  }

  def caseExpr(expr: CaseExpr, record: Any): Any = {
    expr match {
      case CaseExpr_SimpleCaseExpr(expr)  => simpleCaseExpr(expr, record)
      case CaseExpr_GeneralCaseExpr(expr) => generalCaseExpr(expr, record)
      case CaseExpr_CoalesceExpr(expr)    => coalesceExpr(expr, record)
      case CaseExpr_NullifExpr(expr)      => nullifExpr(expr, record)
    }
  }

  def simpleCaseExpr(expr: SimpleCaseExpr, record: Any): Any = {
    caseOperand(expr.caseOperand, record)
    simpleWhenClause(expr.when, record)
    expr.whens foreach { when => simpleWhenClause(when, record) }
    scalarExpr(expr.elseExpr, record)
  }

  def generalCaseExpr(expr: GeneralCaseExpr, record: Any): Any = {
    whenClause(expr.when, record)
    expr.whens foreach { when => whenClause(when, record) }
    scalarExpr(expr.elseExpr, record)
  }

  def coalesceExpr(expr: CoalesceExpr, record: Any): Any = {
    scalarExpr(expr.expr, record)
    expr.exprs map { x => scalarExpr(x, record) }
  }

  def nullifExpr(expr: NullifExpr, record: Any): Any = {
    scalarExpr(expr.leftExpr, record)
    scalarExpr(expr.rightExpr, record)
  }

  def caseOperand(expr: CaseOperand, record: Any): Any = {
    expr.expr match {
      case Left(x)  => stateFieldPathExpr(x, record)
      case Right(x) => typeDiscriminator(x, record)
    }
  }

  def whenClause(whenClause: WhenClause, record: Any): Any = {
    condExpr(whenClause.when, record)
    scalarExpr(whenClause.thenExpr, record)
  }

  def simpleWhenClause(whenClause: SimpleWhenClause, record: Any): Any = {
    scalarExpr(whenClause.when, record)
    scalarExpr(whenClause.thenExpr, record)
  }

  def varOrSingleValuedPath(expr: VarOrSingleValuedPath, record: Any): Any = {
    expr.expr match {
      case Left(x)  => singleValuedPathExpr(x, record)
      case Right(x) => varAccessOrTypeConstant(x, record)
    }
  }

  def stringPrimary(expr: StringPrimary, record: Any): Any = {
    expr match {
      case StringPrimary_LiteralString(expr) => expr
      case StringPrimary_FuncsReturningStrings(expr) =>
        try {
          funcsReturningStrings(expr, record)
        } catch {
          case ex: Throwable => throw ex
        }
      case StringPrimary_InputParam(expr) =>
        inputParam(expr, record)
      case StringPrimary_StateFieldPathExpr(expr) =>
        pathExpr(expr.path, record) match {
          case x: CharSequence =>
          case x               => throw JPQLRuntimeException(x, "is not a StringPrimary")
        }
    }
  }

  def inputParam(expr: InputParam, record: Any): Any = {
    expr match {
      case InputParam_Named(name)   => name
      case InputParam_Position(pos) => pos
    }
  }

  def funcsReturningNumerics(expr: FuncsReturningNumerics, record: Any): Any = {
    expr match {
      case Abs(expr) =>
        simpleArithExpr(expr, record)

      case Length(expr) =>
        scalarExpr(expr, record) match {
          case x: CharSequence =>
          case x               => throw JPQLRuntimeException(x, "is not a string")
        }

      case Mod(expr, divisorExpr) =>
        scalarExpr(expr, record) match {
          case dividend: Number =>
            scalarExpr(divisorExpr, record) match {
              case divisor: Number =>
              case x               => throw JPQLRuntimeException(x, "divisor is not a number")
            }
          case x => throw JPQLRuntimeException(x, "dividend is not a number")
        }

      case Locate(expr, searchExpr, startExpr) =>
        scalarExpr(expr, record) match {
          case base: CharSequence =>
            scalarExpr(searchExpr, record) match {
              case searchStr: CharSequence =>
                startExpr match {
                  case Some(exprx) =>
                    scalarExpr(exprx, record) match {
                      case x: java.lang.Integer =>
                      case x                    => throw JPQLRuntimeException(x, "start is not an integer")
                    }
                  case None =>
                }
              case x => throw JPQLRuntimeException(x, "is not a string")
            }
          case x => throw JPQLRuntimeException(x, "is not a string")
        }

      case Size(expr) =>
        collectionValuedPathExpr(expr, record)

      case Sqrt(expr) =>
        scalarExpr(expr, record) match {
          case x: Number =>
          case x         => throw JPQLRuntimeException(x, "is not a number")
        }

      // Select p from Employee e join e.projects p where e.id = :id and INDEX(p) = 1
      case Index(expr) =>
        varAccessOrTypeConstant(expr, record)

      case Func(name, args) =>
        args map { x => newValue(x, record) }
    }
  }

  def funcsReturningDatetime(expr: FuncsReturningDatetime, record: Any): Any = {
    expr match {
      case CURRENT_DATE      =>
      case CURRENT_TIME      =>
      case CURRENT_TIMESTAMP =>
    }
  }

  def funcsReturningStrings(expr: FuncsReturningStrings, record: Any): Any = {
    expr match {
      case Concat(expr, exprs: List[ScalarExpr]) =>
        scalarExpr(expr, record) match {
          case base: CharSequence =>
            (exprs map { x => scalarExpr(x, record) }).foldLeft() {
              case (sb, x: CharSequence) =>
              case x                     => throw JPQLRuntimeException(x, "is not a string")
            }
          case x => throw JPQLRuntimeException(x, "is not a string")
        }

      case Substring(expr, startExpr, lengthExpr: Option[ScalarExpr]) =>
        scalarExpr(expr, record) match {
          case base: CharSequence =>
            scalarExpr(startExpr, record) match {
              case start: Number =>
                (lengthExpr map { x => scalarExpr(x, record) }) match {
                  case Some(length: Number) =>
                  case _                    =>
                }
              case x => throw JPQLRuntimeException(x, "is not a number")
            }
          case x => throw JPQLRuntimeException(x, "is not a string")
        }

      case Trim(trimSpec: Option[TrimSpec], trimChar: Option[TrimChar], from) =>
        stringPrimary(from, record)
        trimChar match {
          case Some(TrimChar_String(char))      =>
          case Some(TrimChar_InputParam(param)) => inputParam(param, record)
          case None                             =>
        }
        trimSpec match {
          case Some(BOTH) | None =>
          case Some(LEADING)     =>
          case Some(TRAILING)    =>
        }

      case Upper(expr) =>
        scalarExpr(expr, record) match {
          case base: CharSequence =>
          case x                  => throw JPQLRuntimeException(x, "is not a string")
        }

      case Lower(expr) =>
        scalarExpr(expr, record) match {
          case base: CharSequence =>
          case x                  => throw JPQLRuntimeException(x, "is not a string")
        }
    }
  }

  def subquery(subquery: Subquery, record: Any): Any = {
    simpleSelectClause(subquery.select, record)
    subqueryFromClause(subquery.from, record)
    subquery.where match {
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

  def simpleSelectClause(select: SimpleSelectClause, record: Any): Any = {
    val isDistinct = select.isDistinct
    simpleSelectExpr(select.expr, record)
  }

  def simpleSelectExpr(expr: SimpleSelectExpr, record: Any): Any = {
    expr match {
      case SimpleSelectExpr_SingleValuedPathExpr(expr)    => singleValuedPathExpr(expr, record)
      case SimpleSelectExpr_AggregateExpr(expr)           => aggregateExpr(expr, record)
      case SimpleSelectExpr_VarAccessOrTypeConstant(expr) => varAccessOrTypeConstant(expr, record)
    }
  }

  def subqueryFromClause(fromClause: SubqueryFromClause, record: Any): Any = {
    subselectIdentVarDecl(fromClause.from, record)
    fromClause.froms map {
      case Left(x)  => subselectIdentVarDecl(x, record)
      case Right(x) => collectionMemberDecl(x, record)
    }
  }

  def subselectIdentVarDecl(ident: SubselectIdentVarDecl, record: Any): Any = {
    ident match {
      case SubselectIdentVarDecl_IdentVarDecl(expr)         => identVarDecl(expr, record)
      case SubselectIdentVarDecl_AssocPathExpr(expr, as)    => assocPathExpr(expr, record)
      case SubselectIdentVarDecl_CollectionMemberDecl(expr) => collectionMemberDecl(expr, record)
    }
  }

  def orderbyClause(orderbyClause: OrderbyClause, record: Any): Any = {
    orderbyItem(orderbyClause.orderby, record)
    orderbyClause.orderbys map { x => orderbyItem(x, record) }
  }

  def orderbyItem(item: OrderbyItem, record: Any): Any = {
    (item.expr match {
      case Left(x)  => simpleArithExpr(x, record)
      case Right(x) => scalarExpr(x, record)
    }) match {
      case x: CharSequence  =>
      case x: Number        =>
      case x: LocalTime     =>
      case x: LocalDate     =>
      case x: LocalDateTime =>
      case x                => throw JPQLRuntimeException(x, "can not be applied order")
    }
  }

  def groupbyClause(groupbyClause: GroupbyClause, record: Any): Any = {
    scalarExpr(groupbyClause.expr, record) :: (groupbyClause.exprs map { x => scalarExpr(x, record) })
  }

  def havingClause(having: HavingClause, record: Any): Any = {
    condExpr(having.condExpr, record)
  }

}
