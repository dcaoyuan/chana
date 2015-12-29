package chana.jpql

import chana.avro.FlattenRecord
import chana.jpql.nodes._
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneId
import java.time.temporal.Temporal
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8

case class JPQLRuntimeException(value: Any, message: String)
  extends RuntimeException(
    value + " " + message + ". " + value + "'s type is: " + (value match {
      case null      => null
      case x: AnyRef => x.getClass.getName
      case _         => "primary type."
    }))

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

abstract class JPQLEvaluator {

  protected def asToEntity: Map[String, String]
  protected def asToJoin: Map[String, List[String]]
  protected def addAsToEntity(as: String, entity: String): Unit = throw new UnsupportedOperationException()
  protected def addAsToJoin(as: String, joinPath: List[String]): Unit = throw new UnsupportedOperationException()

  private var asToItem = Map[String, Any]()
  private var asToCollectionMember = Map[String, Any]()

  protected var selectObjects = List[Any]()
  protected var selectMapEntries = List[Any]()
  protected var selectNewInstances = List[Any]()

  protected var selectedItems = List[Any]()
  protected var isSelectDistinct: Boolean = _
  protected var isToGather: Boolean = _
  protected var enterJoin: Boolean = _

  /**
   * For simple test
   */
  private[jpql] def simpleEval(stmt: Statement, record: Any): List[Any] = {
    stmt match {
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

      case UpdateStatement(update, set, where)         => List() // NOT YET
      case DeleteStatement(delete, attributes, where)  => List() // NOT YET
      case InsertStatement(insert, attributes, values) => List() // NOT YET
    }
  }

  /**
   * Normalize entity attrs from alias and join
   */
  def normalizeEntityAttrs(qual: String, attrs: List[String], schema: Schema): List[String] = {
    // TODO in case of record does not contain schema, get EntityNames from DistributedSchemaBoard?
    val EntityName = schema.getName.toLowerCase

    val (qual1, attrs1) = asToJoin.get(qual) match {
      case Some(paths) => (paths.head, paths.tail ::: attrs)
      case None        => (qual, attrs)
    }

    asToEntity.get(qual1) match {
      case Some(EntityName) => attrs1
      case _                => throw JPQLRuntimeException(qual1, "is not an AS alias of entity: " + EntityName)
    }
  }

  def valueOf(qual: String, attrPaths: List[String], record: Any): Any = {
    record match {
      case rec: GenericRecord => valueOfRecord(qual, attrPaths, rec)
    }
  }

  def valueOfRecord(qual: String, attrs: List[String], record: GenericRecord): Any = {
    val attrs1 = normalizeEntityAttrs(qual, attrs, record.getSchema)
    valueOfRecord(attrs1, record, toGather = true)
  }

  def valueOfRecord(attrs: List[String], record: GenericRecord, toGather: Boolean): Any = {
    var paths = attrs
    var currValue: Any = record

    while (paths.nonEmpty) {
      val path = paths.head
      paths = paths.tail

      currValue match {
        case fieldRec: GenericRecord                  => currValue = fieldRec.get(path)
        case arr: java.util.Collection[_]             => throw JPQLRuntimeException(currValue, "is an avro array when fetch its attribute: " + path) // TODO
        case map: java.util.Map[String, _] @unchecked => throw JPQLRuntimeException(currValue, "is an avro map when fetch its attribute: " + path) // TODO
        case null                                     => throw JPQLRuntimeException(currValue, "is null when fetch its attribute: " + paths)
        case _                                        => throw JPQLRuntimeException(currValue, "is not a record when fetch its attribute: " + paths)
      }
    } // end while

    if (currValue.isInstanceOf[Utf8]) {
      currValue.toString
    } else {
      currValue
    }
  }

  def updateClause(updateClause: UpdateClause, record: Any) = {
    val entityName = updateClause.entityName.ident
    updateClause.as foreach { x => addAsToEntity(x.ident, entityName) }
    updateClause.joins foreach { x => join(x, record) }
  }

  def setClause(setClause: SetClause, record: Any) = {
    val assign = setAssignClause(setClause.assign, record)
    setClause.assigns foreach { x => setAssignClause(x, record) }
  }

  def setAssignClause(assign: SetAssignClause, record: Any) = {
    val target = setAssignTarget(assign.target, record)
    val value = newValue(assign.value, record)
  }

  def setAssignTarget(target: SetAssignTarget, record: Any) = {
    target.path match {
      case Left(x)  => pathExpr(x, record)
      case Right(x) => attribute(x, record)
    }
  }

  def newValue(expr: NewValue, record: Any) = {
    scalarExpr(expr.v, record)
  }

  def deleteClause(deleteClause: DeleteClause, record: Any) = {
    val from = deleteClause.from.ident
    deleteClause.as foreach { x => addAsToEntity(x.ident, from) }
    from
  }

  def selectClause(select: SelectClause, record: Any): Unit = {
    isToGather = true
    isSelectDistinct = select.isDistinct
    selectItem(select.item, record)
    select.items foreach { x => selectItem(x, record) }
    isToGather = false
  }

  def selectItem(item: SelectItem, record: Any): Any = {
    val item1 = selectExpr(item.expr, record)
    item.as foreach { x => asToItem += (x.ident -> item1) }
    item1
  }

  def insertClause(clause: InsertClause, record: Any) = {
    clause.entityName.ident
  }

  def attributesClause(clause: AttributesClause, record: Any) = {
    attribute(clause.attr, record) :: (clause.attrs map (x => attribute(x, record)))
  }

  def valuesClause(clause: ValuesClause, record: Any) = {
    rowValuesClause(clause.row, record) :: (clause.rows map (x => rowValuesClause(x, record)))
  }

  def rowValuesClause(clause: RowValuesClause, record: Any) = {
    newValue(clause.value, record) :: (clause.values map (x => newValue(x, record)))
  }

  def selectExpr(expr: SelectExpr, record: Any) = {
    expr match {
      case SelectExpr_AggregateExpr(expr)   => selectedItems ::= aggregateExpr(expr, record)
      case SelectExpr_ScalarExpr(expr)      => selectedItems ::= scalarExpr(expr, record)
      case SelectExpr_OBJECT(expr)          => selectObjects ::= varAccessOrTypeConstant(expr, record)
      case SelectExpr_ConstructorExpr(expr) => selectNewInstances ::= constructorExpr(expr, record)
      case SelectExpr_MapEntryExpr(expr)    => selectMapEntries ::= mapEntryExpr(expr, record)
    }
  }

  // SELECT ENTRY(e.contactInfo) from Employee e
  def mapEntryExpr(expr: MapEntryExpr, record: Any): Any = {
    varAccessOrTypeConstant(expr.entry, record)
  }

  def pathExprOrVarAccess(expr: PathExprOrVarAccess, record: Any): Any = {
    expr match {
      case PathExprOrVarAccess_QualIdentVar(qual, attrs) =>
        val qualx = qualIdentVar(qual, record)
        val paths = attrs map { x => attribute(x, record) }
        valueOf(qualx, paths, record)
      case PathExprOrVarAccess_FuncsReturingAny(expr, attrs) =>
        funcsReturningAny(expr, record) match {
          case rec: GenericRecord =>
            val paths = attrs map { x => attribute(x, record) }
            valueOfRecord(paths, rec, toGather = false) // return fieldRec's attribute, but do not gather 
          case v if (attrs.nonEmpty) => throw new JPQLRuntimeException(v, "is not a record, can not be applied attributes")
          case v                     => v
        }
    }
  }

  def qualIdentVar(qual: QualIdentVar, record: Any): String = {
    varAccessOrTypeConstant(qual.v, record)
  }

  def aggregateExpr(expr: AggregateExpr, record: Any) = {
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

  def constructorExpr(expr: ConstructorExpr, record: Any) = {
    val fullname = constructorName(expr.name, record)
    val args = constructorItem(expr.arg, record) :: (expr.args map { x => constructorItem(x, record) })
    null // NOT YET 
  }

  def constructorName(name: ConstructorName, record: Any): String = {
    val fullname = new StringBuilder(name.id.ident)
    name.ids foreach fullname.append(".").append
    fullname.toString
  }

  def constructorItem(item: ConstructorItem, record: Any) = {
    item match {
      case ConstructorItem_ScalarExpr(expr)    => scalarExpr(expr, record)
      case ConstructorItem_AggregateExpr(expr) => aggregateExpr(expr, record) // TODO aggregate here!?
    }
  }

  /**
   * Will collect asToEntity and asToJoin etc
   */
  def fromClause(from: FromClause, record: Any) = {
    identVarDecl(from.from, record)
    from.froms foreach {
      case Left(x)  => identVarDecl(x, record)
      case Right(x) => collectionMemberDecl(x, record)
    }
  }

  def identVarDecl(ident: IdentVarDecl, record: Any) = {
    rangeVarDecl(ident.range, record)
    ident.joins foreach { x => join(x, record) }
  }

  def rangeVarDecl(range: RangeVarDecl, record: Any): Unit = {
    addAsToEntity(range.as.ident.toLowerCase, range.entityName.ident.toLowerCase)
  }

  /**
   *  The JOIN clause allows any of the object's relationships to be joined into
   *  the query so they can be used in the WHERE clause. JOIN does not mean the
   *  relationships will be fetched, unless the FETCH option is included.
   */
  def join(join: Join, record: Any) = {
    enterJoin = true
    join match {
      case Join_General(spec, expr, as, cond) =>
        spec match {
          case JOIN            =>
          case LEFT_JOIN       =>
          case LEFT_OUTER_JOIN =>
          case INNER_JOIN      =>
        }
        val joinPath = joinAssocPathExpr(expr, record)
        addAsToJoin(as.ident.toLowerCase, joinPath)
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
        val joinPath = joinAssocPathExpr(expr, record)
        addAsToJoin(as.ident.toLowerCase, joinPath)
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
        val joinPath = joinAssocPathExpr(expr, record)
        alias foreach { x => addAsToJoin(x.ident.toLowerCase, joinPath) }
        cond match {
          case Some(x) => joinCond(x, record)
          case None    =>
        }
    }
    enterJoin = false
  }

  def joinCond(joinCond: JoinCond, record: Any) = {
    condExpr(joinCond.expr, record)
  }

  def collectionMemberDecl(expr: CollectionMemberDecl, record: Any): Unit = {
    val member = collectionValuedPathExpr(expr.in, record)
    asToCollectionMember += (expr.as.ident -> member)
  }

  def collectionValuedPathExpr(expr: CollectionValuedPathExpr, record: Any) = {
    pathExpr(expr.path, record)
  }

  def assocPathExpr(expr: AssocPathExpr, record: Any) = {
    pathExpr(expr.path, record)
  }

  def joinAssocPathExpr(expr: JoinAssocPathExpr, record: Any): List[String] = {
    val qual = qualIdentVar(expr.qualId, record)
    val paths = expr.attrbutes map { x => attribute(x, record) }
    qual :: paths
  }

  def singleValuedPathExpr(expr: SingleValuedPathExpr, record: Any) = {
    pathExpr(expr.path, record)
  }

  def stateFieldPathExpr(expr: StateFieldPathExpr, record: Any) = {
    pathExpr(expr.path, record)
  }

  def pathExpr(expr: PathExpr, record: Any): Any = {
    val qual = qualIdentVar(expr.qual, record)
    val paths = expr.attributes map { x => attribute(x, record) }
    valueOf(qual, paths, record)
  }

  def attribute(attr: Attribute, record: Any): String = {
    attr.name
  }

  def varAccessOrTypeConstant(expr: VarAccessOrTypeConstant, record: Any): String = {
    expr.id.ident
  }

  def whereClause(where: WhereClause, record: Any): Boolean = {
    condExpr(where.expr, record)
  }

  def condExpr(expr: CondExpr, record: Any): Boolean = {
    expr.orTerms.foldLeft(condTerm(expr.term, record)) { (acc, orTerm) =>
      acc || condTerm(orTerm, record)
    }
  }

  def condTerm(term: CondTerm, record: Any): Boolean = {
    term.andFactors.foldLeft(condFactor(term.factor, record)) { (acc, andFactor) =>
      acc && condFactor(andFactor, record)
    }
  }

  def condFactor(factor: CondFactor, record: Any): Boolean = {
    val res = factor.expr match {
      case Left(x)  => condPrimary(x, record)
      case Right(x) => existsExpr(x, record)
    }

    factor.not ^ res
  }

  def condPrimary(primary: CondPrimary, record: Any): Boolean = {
    primary match {
      case CondPrimary_CondExpr(expr)       => condExpr(expr, record)
      case CondPrimary_SimpleCondExpr(expr) => simpleCondExpr(expr, record)
    }
  }

  def simpleCondExpr(expr: SimpleCondExpr, record: Any): Boolean = {
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

  def betweenExpr(expr: BetweenExpr, record: Any) = {
    (scalarOrSubselectExpr(expr.min, record), scalarOrSubselectExpr(expr.max, record))
  }

  def inExpr(expr: InExpr, record: Any): Boolean = {
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

  def likeExpr(expr: LikeExpr, record: Any): (String, Option[String]) = {
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

  def collectionMemberExpr(expr: CollectionMemberExpr, record: Any): Boolean = {
    collectionValuedPathExpr(expr.of, record)
    true // TODO
  }

  def existsExpr(expr: ExistsExpr, record: Any): Boolean = {
    subquery(expr.subquery, record)
    true // TODO
  }

  def comparsionExprRightOperand(expr: ComparsionExprRightOperand, record: Any) = {
    expr match {
      case ComparsionExprRightOperand_ArithExpr(expr)          => arithExpr(expr, record)
      case ComparsionExprRightOperand_NonArithScalarExpr(expr) => nonArithScalarExpr(expr, record)
      case ComparsionExprRightOperand_AnyOrAllExpr(expr)       => anyOrAllExpr(expr, record)
    }
  }

  def arithExpr(expr: ArithExpr, record: Any) = {
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
      case ArithPrimary_Minus(primary) => JPQLFunctions.neg(arithPrimary(primary, record))
    }
  }

  def arithPrimary(primary: ArithPrimary, record: Any) = {
    primary match {
      case ArithPrimary_PathExprOrVarAccess(expr)   => pathExprOrVarAccess(expr, record)
      case ArithPrimary_InputParam(expr)            => inputParam(expr, record)
      case ArithPrimary_CaseExpr(expr)              => caseExpr(expr, record)
      case ArithPrimary_FuncsReturningNumeric(expr) => funcsReturningNumeric(expr, record)
      case ArithPrimary_SimpleArithExpr(expr)       => simpleArithExpr(expr, record)
      case ArithPrimary_LiteralNumeric(expr)        => expr
    }
  }

  def scalarExpr(expr: ScalarExpr, record: Any): Any = {
    expr match {
      case ScalarExpr_SimpleArithExpr(expr)    => simpleArithExpr(expr, record)
      case ScalarExpr_NonArithScalarExpr(expr) => nonArithScalarExpr(expr, record)
    }
  }

  def scalarOrSubselectExpr(expr: ScalarOrSubselectExpr, record: Any) = {
    expr match {
      case ScalarOrSubselectExpr_ArithExpr(expr)          => arithExpr(expr, record)
      case ScalarOrSubselectExpr_NonArithScalarExpr(expr) => nonArithScalarExpr(expr, record)
    }
  }

  def nonArithScalarExpr(expr: NonArithScalarExpr, record: Any): Any = {
    expr match {
      case NonArithScalarExpr_FuncsReturningDatetime(expr) => funcsReturningDatetime(expr, record)
      case NonArithScalarExpr_FuncsReturningString(expr)   => funcsReturningString(expr, record)
      case NonArithScalarExpr_LiteralString(expr)          => expr
      case NonArithScalarExpr_LiteralBoolean(expr)         => expr
      case NonArithScalarExpr_LiteralTemporal(expr)        => expr
      case NonArithScalarExpr_EntityTypeExpr(expr)         => entityTypeExpr(expr, record)
    }
  }

  def anyOrAllExpr(expr: AnyOrAllExpr, record: Any) = {
    val subq = subquery(expr.subquery, record)
    expr.anyOrAll match {
      case ALL  =>
      case ANY  =>
      case SOME =>
    }
  }

  def entityTypeExpr(expr: EntityTypeExpr, record: Any) = {
    typeDiscriminator(expr.typeDis, record)
  }

  def typeDiscriminator(expr: TypeDiscriminator, record: Any) = {
    expr.expr match {
      case Left(expr1)  => varOrSingleValuedPath(expr1, record)
      case Right(expr1) => inputParam(expr1, record)
    }
  }

  def caseExpr(expr: CaseExpr, record: Any): Any = {
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

  def simpleCaseExpr(expr: SimpleCaseExpr, record: Any) = {
    caseOperand(expr.caseOperand, record)
    simpleWhenClause(expr.when, record)
    expr.whens foreach { when =>
      simpleWhenClause(when, record)
    }
    val elseExpr = scalarExpr(expr.elseExpr, record)
  }

  def generalCaseExpr(expr: GeneralCaseExpr, record: Any) = {
    whenClause(expr.when, record)
    expr.whens foreach { when =>
      whenClause(when, record)
    }
    val elseExpr = scalarExpr(expr.elseExpr, record)
  }

  def coalesceExpr(expr: CoalesceExpr, record: Any) = {
    scalarExpr(expr.expr, record)
    expr.exprs map { x => scalarExpr(x, record) }
  }

  def nullifExpr(expr: NullifExpr, record: Any) = {
    val left = scalarExpr(expr.leftExpr, record)
    val right = scalarExpr(expr.rightExpr, record)
    left // TODO
  }

  def caseOperand(expr: CaseOperand, record: Any) = {
    expr.expr match {
      case Left(x)  => stateFieldPathExpr(x, record)
      case Right(x) => typeDiscriminator(x, record)
    }
  }

  def whenClause(whenClause: WhenClause, record: Any) = {
    val when = condExpr(whenClause.when, record)
    val thenExpr = scalarExpr(whenClause.thenExpr, record)
  }

  def simpleWhenClause(whenClause: SimpleWhenClause, record: Any) = {
    val when = scalarExpr(whenClause.when, record)
    val thenExpr = scalarExpr(whenClause.thenExpr, record)
  }

  def varOrSingleValuedPath(expr: VarOrSingleValuedPath, record: Any) = {
    expr.expr match {
      case Left(x)  => singleValuedPathExpr(x, record)
      case Right(x) => varAccessOrTypeConstant(x, record)
    }
  }

  def stringPrimary(expr: StringPrimary, record: Any): Either[String, Any => String] = {
    expr match {
      case StringPrimary_LiteralString(expr) => Left(expr)
      case StringPrimary_FuncsReturningString(expr) =>
        try {
          Left(funcsReturningString(expr, record))
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

  def inputParam(expr: InputParam, record: Any) = {
    expr match {
      case InputParam_Named(name)   => name
      case InputParam_Position(pos) => pos
    }
  }

  def funcsReturningNumeric(expr: FuncsReturningNumeric, record: Any): Any = {
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
        record match {
          case FlattenRecord(_, field, _, index) => index + 1 // jpql index start at 1 // TODO check flat field name
          case x                                 => throw JPQLRuntimeException(x, "is not a indexed list member")
        }

      case Func(name, args) =>
        // try to call function: name(as: _*) TODO
        val as = args map { x => newValue(x, record) }
        0
    }
  }

  def funcsReturningDatetime(expr: FuncsReturningDatetime, record: Any): Temporal = {
    expr match {
      case CURRENT_DATE      => JPQLFunctions.currentDate()
      case CURRENT_TIME      => JPQLFunctions.currentTime()
      case CURRENT_TIMESTAMP => JPQLFunctions.currentDateTime()
    }
  }

  def funcsReturningString(expr: FuncsReturningString, record: Any): String = {
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

      case MapKey(expr) =>
        record match {
          case r @ FlattenRecord(_, field, fieldValue: java.util.Map.Entry[CharSequence, _] @unchecked, index) =>
            val qual = varAccessOrTypeConstant(expr, record)
            valueOfRecord(qual, List(), r) // force to gather this map field. TODO
            fieldValue.getKey.toString
          case x => throw JPQLRuntimeException(x, "is not a map entry")
        }
    }
  }

  // SELECT e from Employee e join e.contactInfo c where KEY(c) = 'Email' and VALUE(c) = 'joe@gmail.com'
  def funcsReturningAny(expr: FuncsReturningAny, record: Any): Any = {
    expr match {
      case MapValue(expr) =>
        record match {
          case r @ FlattenRecord(_, field, fieldValue: java.util.Map.Entry[CharSequence, _] @unchecked, index) =>
            val qual = varAccessOrTypeConstant(expr, record)
            valueOfRecord(qual, List(), r) // force to gather this map field. TODO
            fieldValue.getValue
          case x => throw JPQLRuntimeException(x, "is not a map entry")
        }
      case JPQLJsonValue(jsonNode) => jsonNode
    }
  }

  def subquery(subquery: Subquery, record: Any) = {
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

  def simpleSelectClause(select: SimpleSelectClause, record: Any) = {
    val isDistinct = select.isDistinct
    simpleSelectExpr(select.expr, record)
  }

  def simpleSelectExpr(expr: SimpleSelectExpr, record: Any) = {
    expr match {
      case SimpleSelectExpr_SingleValuedPathExpr(expr)    => singleValuedPathExpr(expr, record)
      case SimpleSelectExpr_AggregateExpr(expr)           => aggregateExpr(expr, record)
      case SimpleSelectExpr_VarAccessOrTypeConstant(expr) => varAccessOrTypeConstant(expr, record)
    }
  }

  def subqueryFromClause(fromClause: SubqueryFromClause, record: Any) = {
    val from = subselectIdentVarDecl(fromClause.from, record)
    val froms = fromClause.froms map {
      case Left(x)  => subselectIdentVarDecl(x, record)
      case Right(x) => collectionMemberDecl(x, record)
    }
  }

  def subselectIdentVarDecl(ident: SubselectIdentVarDecl, record: Any) = {
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

  def orderbyClause(orderbyClause: OrderbyClause, record: Any): List[Any] = {
    isToGather = true
    val items = orderbyItem(orderbyClause.orderby, record) :: (orderbyClause.orderbys map { x => orderbyItem(x, record) })
    isToGather = false
    items
  }

  def orderbyItem(item: OrderbyItem, record: Any): Any = {
    val orderingItem = item.expr match {
      case Left(x)  => simpleArithExpr(x, record)
      case Right(x) => scalarExpr(x, record)
    }
    orderingItem match {
      case x: CharSequence  => (item.isAsc, x)
      case x: Number        => (if (item.isAsc) x else JPQLFunctions.neg(x))
      case x: LocalTime     => (if (item.isAsc) 1 else -1) * (x.toNanoOfDay)
      case x: LocalDate     => (if (item.isAsc) 1 else -1) * (x.getYear * 12 * 31 + x.getMonthValue * 12 + x.getDayOfMonth)
      case x: LocalDateTime => (if (item.isAsc) 1 else -1) * (x.atZone(JPQLEvaluator.timeZone).toInstant.toEpochMilli)
      case x                => throw JPQLRuntimeException(x, "can not be applied order")
    }
  }

  def groupbyClause(groupbyClause: GroupbyClause, record: Any): List[Any] = {
    isToGather = true
    val groupbys = scalarExpr(groupbyClause.expr, record) :: (groupbyClause.exprs map { x => scalarExpr(x, record) })
    isToGather = false
    groupbys
  }

  def havingClause(having: HavingClause, record: Any): Boolean = {
    isToGather = true
    val cond = condExpr(having.condExpr, record)
    isToGather = false
    cond
  }

}

