package chana.jpql

import chana.avro.FlattenRecord
import chana.jpql.nodes._
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneId
import java.time.temporal.Temporal
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
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

  val ID = "id"
  val SOME_ID = Some(ID)
}

abstract class JPQLEvaluator {

  def id: String
  protected def asToEntity: Map[String, String]
  protected def asToJoin: Map[String, List[String]]
  protected def addAsToEntity(as: String, entity: String): Unit = throw new UnsupportedOperationException()
  protected def addAsToJoin(as: String, joinPath: List[String]): Unit = throw new UnsupportedOperationException()

  protected var asToItem = Map[String, Any]()
  protected def addAsToItem(as: String, item: Any) = asToItem += (as -> item)

  protected def forceGather = false

  protected var asToCollectionMember = Map[String, Any]()

  protected var isSelectDistinct: Boolean = _

  private var _enterGather: Boolean = _
  protected def enterGather: Boolean = _enterGather
  private def enterGather_=(x: Boolean) {
    _enterGather = x
  }
  protected var enterJoin: Boolean = _

  /**
   * For simple test
   */
  private[jpql] def simpleEval(stmt: Statement, record: Any): List[Any] = {
    stmt match {
      case SelectStatement(select, from, where, groupby, having, orderby) =>
        val entity = fromClause(from, record)

        val whereCond = where.fold(true) { x => whereClause(x, record) }
        if (whereCond) {
          val selectedItems = selectClause(select, record, toGather = false)

          groupby.fold(List[Any]()) { x => groupbyClause(x, record) }

          having.fold(true) { x => havingClause(x, record) }
          orderby.fold(List[Any]()) { x => orderbyClause(x, record) }

          selectedItems
        } else {
          List()
        }

      case UpdateStatement(update, set, where)                => List() // NOT YET
      case DeleteStatement(delete, attributes, where)         => List() // NOT YET
      case InsertStatement(insert, attributes, values, where) => List() // NOT YET
    }
  }

  // ----- helper methods

  /**
   * Normalize entity attrs from alias and join
   */
  def normalizeEntityAttrPaths(_qual: String, _attrs: List[String], schema: Schema): List[String] = {
    // TODO in case of record does not contain schema, get EntityNames from DistributedSchemaBoard?

    val (qual, attrPaths) = asToJoin.get(_qual) match {
      case Some(paths) => (paths.head.toLowerCase, paths.tail ::: _attrs)
      case None        => (_qual.toLowerCase, _attrs)
    }

    val EntityName = schema.getName.toLowerCase
    asToEntity.get(qual) match {
      case Some(EntityName) => attrPaths
      case _                => List()
    }
  }

  final def valueOf(qual: String, attrPaths: List[String], record: Any): Any =
    valueOf(qual, attrPaths, record, forceGather)
  def valueOf(qual: String, attrPaths: List[String], record: Any, toGather: Boolean): Any = {
    record match {
      case rec: IndexedRecord => valueOfRecord(qual, attrPaths, rec, toGather)
    }
  }

  final def valueOfRecord(qual: String, attrs: List[String], record: IndexedRecord): Any = valueOfRecord(qual, attrs, record, forceGather)
  final def valueOfRecord(qual: String, attrs: List[String], record: IndexedRecord, toGather: Boolean): Any = {
    normalizeEntityAttrPaths(qual, attrs, record.getSchema) match {
      case Nil =>
        asToItem.get(qual) match {
          case Some(x) => x
          case None    => throw JPQLRuntimeException(qual, "is not an AS alias of entity: " + EntityName)
        }
      case attrs1 => valueOfRecord(attrs1, record, toGather)
    }
  }

  def valueOfRecord(attrs: List[String], record: IndexedRecord, toGather: Boolean): Any = {
    var paths = attrs
    var currValue: Any = record

    var isTopField = true
    while (paths.nonEmpty) {
      val path = paths.head
      paths = paths.tail

      currValue match {
        case fieldRec: IndexedRecord =>
          if (isTopField && path == JPQLEvaluator.ID) {
            currValue = id
          } else {
            val pathField = fieldRec.getSchema.getField(path)
            currValue = fieldRec.get(pathField.pos)
          }
        case arr: java.util.Collection[_]             => throw JPQLRuntimeException(currValue, "is an avro array when fetch its attribute: " + path) // TODO
        case map: java.util.Map[String, _] @unchecked => throw JPQLRuntimeException(currValue, "is an avro map when fetch its attribute: " + path) // TODO
        case null                                     => throw JPQLRuntimeException(currValue, "is null when fetch its attribute: " + paths)
        case _                                        => throw JPQLRuntimeException(currValue, "is not a record when fetch its attribute: " + paths)
      }
      isTopField = false
    } // end while

    if (currValue.isInstanceOf[Utf8]) {
      currValue.toString
    } else {
      currValue
    }
  }

  // ----- AST visiting section

  /**
   * @return main entity name
   */
  def updateClause(updateClause: UpdateClause, record: Any): String = {
    val entity = updateClause.entityName.ident.toLowerCase
    updateClause.as foreach { x => addAsToEntity(x.ident.toLowerCase, entity) }
    updateClause.joins foreach { x => join(x, record) }
    entity
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

  /**
   * @return main entity name
   */
  def deleteClause(delete: DeleteClause, record: Any): String = {
    val entity = delete.from.ident.toLowerCase
    delete.as foreach { x => addAsToEntity(x.ident.toLowerCase, entity) }
    delete.joins foreach { x => join(x, record) }
    entity
  }

  def selectClause(select: SelectClause, record: Any, toGather: Boolean): List[Any] = {
    enterGather = toGather
    isSelectDistinct = select.isDistinct
    val items = (select.item :: select.items) map { x => selectItem(x, record) }
    enterGather = false
    items
  }

  def selectItem(item: SelectItem, record: Any): Any = {
    val item0 = selectExpr(item.expr, record)
    item.as foreach { x => addAsToItem(x.ident.toLowerCase, item0) }
    item0
  }

  /**
   * @return main entity name
   */
  def insertClause(insert: InsertClause, record: Any): String = {
    val entity = insert.entityName.ident.toLowerCase
    insert.as foreach { x => addAsToEntity(x.ident.toLowerCase, entity) }
    insert.joins foreach { x => join(x, record) }
    entity
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
      case x: AggregateExpr           => aggregateExpr(x, record)
      case x: ScalarExpr              => scalarExpr(x, record) // SELECT OBJECT
      case x: VarAccessOrTypeConstant => varAccessOrTypeConstant(x, record)
      case x: ConstructorExpr         => constructorExpr(x, record)
      case x: MapEntryExpr            => mapEntryExpr(x, record)
    }
  }

  // SELECT ENTRY(e.contactInfo) from Employee e
  def mapEntryExpr(expr: MapEntryExpr, record: Any): Any = {
    varAccessOrTypeConstant(expr.entry, record)
  }

  def pathExprOrVarAccess(expr: PathExprOrVarAccess, record: Any): Any = {
    expr match {
      case PathExprOrVarAccess(Left(qual), attrs) =>
        val qual0 = qualIdentVar(qual, record)
        val paths = attrs map { x => attribute(x, record) }
        valueOf(qual0, paths, record)
      case PathExprOrVarAccess(Right(func), attrs) =>
        funcsReturningAny(func, record) match {
          case rec: IndexedRecord =>
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

  /**
   * Should be evaluated in reducing
   */
  def aggregateExpr(expr: AggregateExpr, record: Any) = {
    expr match {
      case Avg(isDistinct, expr)   => scalarExpr(expr, record)
      case Max(isDistinct, expr)   => scalarExpr(expr, record)
      case Min(isDistinct, expr)   => scalarExpr(expr, record)
      case Sum(isDistinct, expr)   => scalarExpr(expr, record)
      case Count(isDistinct, expr) => scalarExpr(expr, record)
    }
  }

  def constructorExpr(expr: ConstructorExpr, record: Any) = {
    val fullname = constructorName(expr.name, record)
    val args = (expr.arg :: expr.args) map { x => constructorItem(x, record) }
    null // NOT YET 
  }

  def constructorName(name: ConstructorName, record: Any): String = {
    val fullname = new StringBuilder(name.id.ident)
    name.ids foreach fullname.append(".").append
    fullname.toString
  }

  def constructorItem(item: ConstructorItem, record: Any) = {
    item match {
      case x: ScalarExpr    => scalarExpr(x, record)
      case x: AggregateExpr => aggregateExpr(x, record) // TODO aggregate here!?
    }
  }

  /**
   * Will collect asToEntity and asToJoin etc
   *
   * @return main entity name
   */
  def fromClause(from: FromClause, record: Any): String = {
    val entity = identVarDecl(from.from, record)
    from.froms foreach {
      case Left(x)  => identVarDecl(x, record)
      case Right(x) => collectionMemberDecl(x, record)
    }
    entity
  }

  def identVarDecl(ident: IdentVarDecl, record: Any): String = {
    val entity = rangeVarDecl(ident.range, record)
    ident.joins foreach { x => join(x, record) }
    entity
  }

  def rangeVarDecl(range: RangeVarDecl, record: Any): String = {
    val entity = range.entityName.ident.toLowerCase
    val as = range.as.ident.toLowerCase
    addAsToEntity(as, entity)
    entity
  }

  /**
   *  The JOIN clause allows any of the object's relationships to be joined into
   *  the query so they can be used in the WHERE clause. JOIN does not mean the
   *  relationships will be fetched, unless the FETCH option is included.
   */
  def join(join: Join, record: Any) = {
    enterJoin = true
    join match {
      case Join_GENERAL(spec, expr, as, cond) =>
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
      case x: CondExpr       => condExpr(x, record)
      case x: SimpleCondExpr => simpleCondExpr(x, record)
    }
  }

  def simpleCondExpr(expr: SimpleCondExpr, record: Any): Boolean = {
    val base = expr.expr match {
      case Left(x)  => arithExpr(x, record)
      case Right(x) => nonArithScalarExpr(x, record)
    }

    // simpleCondExprRem
    expr.rem match {
      case x: ComparisonExpr =>
        // comparisionExpr
        val operand = comparsionExprRightOperand(x.operand, record)
        x.op match {
          case EQ => JPQLFunctions.eq(base, operand)
          case NE => JPQLFunctions.ne(base, operand)
          case GT => JPQLFunctions.gt(base, operand)
          case GE => JPQLFunctions.ge(base, operand)
          case LT => JPQLFunctions.lt(base, operand)
          case LE => JPQLFunctions.le(base, operand)
        }

      case CondExprNotableWithNot(not, expr) =>
        // condExprNotable
        val res = expr match {
          case x: BetweenExpr =>
            val minMax = betweenExpr(x, record)
            JPQLFunctions.between(base, minMax._1, minMax._2)

          case x: LikeExpr =>
            base match {
              case c: CharSequence =>
                val like = likeExpr(x, record)
                JPQLFunctions.strLike(c.toString, like._1, like._2)
              case x => throw JPQLRuntimeException(x, "is not a string")
            }

          case x: InExpr =>
            val in = inExpr(x, record)
            true // TODO

          case x: CollectionMemberExpr =>
            collectionMemberExpr(x, record)
        }

        not ^ res

      case IsExprWithNot(not, expr) =>
        // isExpr
        val res = expr match {
          case IsNull =>
            base match {
              case x: AnyRef => x eq null
              case _         => false // TODO
            }
          case IsEmpty =>
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

  def inExpr(expr: InExpr, record: Any): Any = {
    expr match {
      case ScalarOrSubselectExprs(expr, exprs) =>
        (expr :: exprs) map { x => scalarOrSubselectExpr(x, record) }
      case x: InputParam =>
        inputParam(x, record)
      case x: Subquery =>
        subquery(x, record)
    }
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
      case x: ArithExpr          => arithExpr(x, record)
      case x: NonArithScalarExpr => nonArithScalarExpr(x, record)
      case x: AnyOrAllExpr       => anyOrAllExpr(x, record)
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
      case (acc, term @ ArithTerm(PLUS, _, _))  => JPQLFunctions.plus(acc, arithTerm(term, record))
      case (acc, term @ ArithTerm(MINUS, _, _)) => JPQLFunctions.minus(acc, arithTerm(term, record))
      case (acc, ArithTerm(op, _, _))           => throw JPQLRuntimeException(op, "should be PLUS or MINUS")
    }
  }

  def arithTerm(term: ArithTerm, record: Any): Any = {
    term.rightFactors.foldLeft(arithFactor(term.factor, record)) {
      case (acc, factor @ ArithFactor(MULTIPLY, _)) => JPQLFunctions.multiply(acc, arithFactor(factor, record))
      case (acc, factor @ ArithFactor(DIVIDE, _))   => JPQLFunctions.divide(acc, arithFactor(factor, record))
      case (acc, ArithFactor(op, _))                => throw JPQLRuntimeException(op, "should be MULTIPLY or DIVIDE")
    }
  }

  def arithFactor(factor: ArithFactor, record: Any): Any = {
    plusOrMinusPrimary(factor.primary, record)
  }

  def plusOrMinusPrimary(primary: PlusOrMinusPrimary, record: Any): Any = {
    val primary0 = arithPrimary(primary.primary, record)
    primary.prefixOp match {
      case PLUS  => primary0
      case MINUS => JPQLFunctions.neg(primary0)
      case op    => throw JPQLRuntimeException(op, "should be PLUS or MINUS")
    }
  }

  def arithPrimary(primary: ArithPrimary, record: Any) = {
    primary match {
      case LiteralInteger(v)        => v
      case LiteralLong(v)           => v
      case LiteralFloat(v)          => v
      case LiteralDouble(v)         => v
      case x: PathExprOrVarAccess   => pathExprOrVarAccess(x, record)
      case x: InputParam            => inputParam(x, record)
      case x: CaseExpr              => caseExpr(x, record)
      case x: FuncsReturningNumeric => funcsReturningNumeric(x, record)
      case x: SimpleArithExpr       => simpleArithExpr(x, record)
    }
  }

  def scalarExpr(expr: ScalarExpr, record: Any): Any = {
    expr match {
      case x: SimpleArithExpr    => simpleArithExpr(x, record)
      case x: NonArithScalarExpr => nonArithScalarExpr(x, record)
    }
  }

  def scalarOrSubselectExpr(expr: ScalarOrSubselectExpr, record: Any) = {
    expr match {
      case x: ArithExpr          => arithExpr(x, record)
      case x: NonArithScalarExpr => nonArithScalarExpr(x, record)
    }
  }

  def nonArithScalarExpr(expr: NonArithScalarExpr, record: Any): Any = {
    expr match {
      case LiteralString(v)          => v
      case LiteralBoolean(v)         => v
      case LiteralDate(v)            => v
      case LiteralTime(v)            => v
      case LiteralTimestamp(v)       => v
      case x: FuncsReturningDatetime => funcsReturningDatetime(x, record)
      case x: FuncsReturningString   => funcsReturningString(x, record)
      case x: EntityTypeExpr         => entityTypeExpr(x, record)
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
      case x: SimpleCaseExpr  => simpleCaseExpr(x, record)
      case x: GeneralCaseExpr => generalCaseExpr(x, record)
      case x: CoalesceExpr    => coalesceExpr(x, record)
      case x: NullifExpr      => nullifExpr(x, record)
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
      case LiteralString(v) => Left(v)
      case x: FuncsReturningString =>
        try {
          Left(funcsReturningString(x, record))
        } catch {
          case ex: Throwable => throw ex
        }
      case x: InputParam =>
        val param = inputParam(x, record)
        Right(param => "") // TODO
      case x: StateFieldPathExpr =>
        pathExpr(x.path, record) match {
          case c: CharSequence => Left(c.toString)
          case c               => throw JPQLRuntimeException(c, "is not a StringPrimary")
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
        val xs = args map { x => newValue(x, record) }
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
          case Some(LiteralString(x)) => x
          case Some(x: InputParam) =>
            inputParam(x, record)
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
      case x: SingleValuedPathExpr    => singleValuedPathExpr(x, record)
      case x: AggregateExpr           => aggregateExpr(x, record)
      case x: VarAccessOrTypeConstant => varAccessOrTypeConstant(x, record)
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
      case x: IdentVarDecl         => identVarDecl(x, record)
      case x: CollectionMemberDecl => collectionMemberDecl(x, record)
      case x: AssocPathExprWithAs  => assocPathExprWithAs(x.expr, x.as, record)
    }
  }

  def assocPathExprWithAs(expr: AssocPathExpr, as: Ident, record: Any) = {
    assocPathExpr(expr, record)
    as.ident
  }

  def orderbyClause(orderbyClause: OrderbyClause, record: Any): List[Any] = {
    enterGather = true
    val items = orderbyItem(orderbyClause.orderby, record) :: (orderbyClause.orderbys map { x => orderbyItem(x, record) })
    enterGather = false
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
    enterGather = true
    val groupbys = scalarExpr(groupbyClause.expr, record) :: (groupbyClause.exprs map { x => scalarExpr(x, record) })
    enterGather = false
    groupbys
  }

  /**
   * HavingClause is applied on aggregated select items, do not gather it
   */
  def havingClause(having: HavingClause, record: Any): Boolean = {
    condExpr(having.condExpr, record)
  }

}

