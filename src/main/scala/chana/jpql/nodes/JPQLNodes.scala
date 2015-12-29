package chana.jpql

/*
 * Definition of JPQL AST nodes.
 * 
 * @author Caoyuan Deng
 */
package object nodes {

  sealed trait Statement extends Serializable
  final case class SelectStatement(
    select: SelectClause,
    from: FromClause,
    where: Option[WhereClause],
    groupby: Option[GroupbyClause],
    having: Option[HavingClause],
    orderby: Option[OrderbyClause]) extends Statement

  final case class UpdateStatement(
    update: UpdateClause,
    set: SetClause,
    where: Option[WhereClause]) extends Statement

  final case class DeleteStatement(
    delete: DeleteClause,
    attributes: Option[AttributesClause],
    where: Option[WhereClause]) extends Statement

  final case class InsertStatement(
    insert: InsertClause,
    attributes: Option[AttributesClause],
    values: ValuesClause) extends Statement

  final case class UpdateClause(entityName: EntityName, as: Option[Ident], joins: List[Join])
  final case class SetClause(assign: SetAssignClause, assigns: List[SetAssignClause])
  final case class SetAssignClause(target: SetAssignTarget, value: NewValue)
  final case class SetAssignTarget(path: Either[PathExpr, Attribute])

  final case class NewValue(v: ScalarExpr) // could be null

  final case class DeleteClause(from: EntityName, as: Option[Ident])

  final case class SelectClause(isDistinct: Boolean, item: SelectItem, items: List[SelectItem])

  final case class SelectItem(expr: SelectExpr, as: Option[Ident])

  final case class InsertClause(entityName: EntityName)
  final case class AttributesClause(attr: Attribute, attrs: List[Attribute])
  final case class ValuesClause(row: RowValuesClause, rows: List[RowValuesClause])
  final case class RowValuesClause(value: NewValue, values: List[NewValue])

  sealed trait SelectExpr
  final case class SelectExpr_AggregateExpr(expr: AggregateExpr) extends SelectExpr
  final case class SelectExpr_ScalarExpr(expr: ScalarExpr) extends SelectExpr
  final case class SelectExpr_OBJECT(expr: VarAccessOrTypeConstant) extends SelectExpr
  final case class SelectExpr_ConstructorExpr(expr: ConstructorExpr) extends SelectExpr
  final case class SelectExpr_MapEntryExpr(expr: MapEntryExpr) extends SelectExpr

  final case class MapEntryExpr(entry: VarAccessOrTypeConstant)

  sealed trait PathExprOrVarAccess
  final case class PathExprOrVarAccess_QualIdentVar(qual: QualIdentVar, attributes: List[Attribute]) extends PathExprOrVarAccess
  final case class PathExprOrVarAccess_FuncsReturingAny(expr: FuncsReturningAny, attributes: List[Attribute]) extends PathExprOrVarAccess

  final case class QualIdentVar(v: VarAccessOrTypeConstant)

  sealed trait AggregateExpr
  final case class AggregateExpr_AVG(isDistinct: Boolean, expr: ScalarExpr) extends AggregateExpr
  final case class AggregateExpr_MAX(isDistinct: Boolean, expr: ScalarExpr) extends AggregateExpr
  final case class AggregateExpr_MIN(isDistinct: Boolean, expr: ScalarExpr) extends AggregateExpr
  final case class AggregateExpr_SUM(isDistinct: Boolean, expr: ScalarExpr) extends AggregateExpr
  final case class AggregateExpr_COUNT(isDistinct: Boolean, expr: ScalarExpr) extends AggregateExpr

  final case class ConstructorExpr(name: ConstructorName, arg: ConstructorItem, args: List[ConstructorItem])

  final case class ConstructorName(id: Ident, ids: List[Ident])

  sealed trait ConstructorItem
  final case class ConstructorItem_ScalarExpr(expr: ScalarExpr) extends ConstructorItem
  final case class ConstructorItem_AggregateExpr(expr: AggregateExpr) extends ConstructorItem

  final case class FromClause(from: IdentVarDecl, froms: List[Either[IdentVarDecl, CollectionMemberDecl]])

  final case class IdentVarDecl(range: RangeVarDecl, joins: List[Join])

  final case class RangeVarDecl(entityName: EntityName, as: Ident)

  final case class EntityName(ident: String)

  sealed trait Join
  final case class Join_General(spec: JoinSpec, expr: JoinAssocPathExpr, as: Ident, cond: Option[JoinCond]) extends Join
  final case class Join_TREAT(spec: JoinSpec, expr: JoinAssocPathExpr, exprAs: Ident, as: Ident, cond: Option[JoinCond]) extends Join
  final case class Join_FETCH(spec: JoinSpec, expr: JoinAssocPathExpr, alias: Option[Ident], cond: Option[JoinCond]) extends Join

  sealed trait JoinSpec
  case object JOIN extends JoinSpec
  case object LEFT_JOIN extends JoinSpec
  case object LEFT_OUTER_JOIN extends JoinSpec
  case object INNER_JOIN extends JoinSpec

  final case class JoinCond(expr: CondExpr)

  final case class CollectionMemberDecl(in: CollectionValuedPathExpr, as: Ident)

  final case class CollectionValuedPathExpr(path: PathExpr)

  final case class AssocPathExpr(path: PathExpr)

  final case class JoinAssocPathExpr(qualId: QualIdentVar, attrbutes: List[Attribute])

  final case class SingleValuedPathExpr(path: PathExpr)

  final case class StateFieldPathExpr(path: PathExpr)

  final case class PathExpr(qual: QualIdentVar, attributes: List[Attribute])

  final case class Attribute(name: String)

  final case class VarAccessOrTypeConstant(id: Ident)

  final case class WhereClause(expr: CondExpr)

  final case class CondExpr(term: CondTerm, orTerms: List[CondTerm])

  final case class CondTerm(factor: CondFactor, andFactors: List[CondFactor])

  final case class CondFactor(not: Boolean, expr: Either[CondPrimary, ExistsExpr])

  sealed trait CondPrimary
  final case class CondPrimary_CondExpr(expr: CondExpr) extends CondPrimary
  final case class CondPrimary_SimpleCondExpr(expr: SimpleCondExpr) extends CondPrimary

  final case class SimpleCondExpr(expr: Either[ArithExpr, NonArithScalarExpr], rem: SimpleCondExprRem)

  sealed trait SimpleCondExprRem
  final case class SimpleCondExprRem_ComparisonExpr(expr: ComparisonExpr) extends SimpleCondExprRem
  final case class SimpleCondExprRem_CondWithNotExpr(not: Boolean, expr: CondWithNotExpr) extends SimpleCondExprRem
  final case class SimpleCondExprRem_IsExpr(not: Boolean, expr: IsExpr) extends SimpleCondExprRem

  sealed trait CondWithNotExpr
  final case class CondWithNotExpr_BetweenExpr(expr: BetweenExpr) extends CondWithNotExpr
  final case class CondWithNotExpr_LikeExpr(expr: LikeExpr) extends CondWithNotExpr
  final case class CondWithNotExpr_InExpr(expr: InExpr) extends CondWithNotExpr
  final case class CondWithNotExpr_CollectionMemberExpr(expr: CollectionMemberExpr) extends CondWithNotExpr

  sealed trait IsExpr
  case object IsNullExpr extends IsExpr // NULL 
  case object IsEmptyExpr extends IsExpr // EMPTY

  final case class BetweenExpr(min: ScalarOrSubselectExpr, max: ScalarOrSubselectExpr)

  sealed trait InExpr
  final case class InExpr_InputParam(expr: InputParam) extends InExpr
  final case class InExpr_ScalarOrSubselectExpr(expr: ScalarOrSubselectExpr, exprs: List[ScalarOrSubselectExpr]) extends InExpr
  final case class InExpr_Subquery(expr: Subquery) extends InExpr

  final case class LikeExpr(like: ScalarOrSubselectExpr, escape: Option[Escape])

  final case class Escape(expr: ScalarExpr)

  final case class CollectionMemberExpr(of: CollectionValuedPathExpr)

  final case class ExistsExpr(subquery: Subquery)

  final case class ComparisonExpr(op: ComparisonOp, operand: ComparsionExprRightOperand)

  sealed trait ComparisonOp
  case object EQ extends ComparisonOp
  case object NE extends ComparisonOp
  case object GT extends ComparisonOp
  case object GE extends ComparisonOp
  case object LT extends ComparisonOp
  case object LE extends ComparisonOp

  sealed trait ComparsionExprRightOperand
  final case class ComparsionExprRightOperand_ArithExpr(expr: ArithExpr) extends ComparsionExprRightOperand
  final case class ComparsionExprRightOperand_NonArithScalarExpr(expr: NonArithScalarExpr) extends ComparsionExprRightOperand
  final case class ComparsionExprRightOperand_AnyOrAllExpr(expr: AnyOrAllExpr) extends ComparsionExprRightOperand

  final case class ArithExpr(expr: Either[SimpleArithExpr, Subquery])

  final case class SimpleArithExpr(term: ArithTerm, rightTerms: List[PlusOrMinusTerm])

  sealed trait PlusOrMinusTerm
  final case class ArithTerm_Plus(term: ArithTerm) extends PlusOrMinusTerm
  final case class ArithTerm_Minus(term: ArithTerm) extends PlusOrMinusTerm

  final case class ArithTerm(factor: ArithFactor, rightFactors: List[MultiplyOrDivideFactor])

  sealed trait MultiplyOrDivideFactor
  final case class ArithFactor_Multiply(factor: ArithFactor) extends MultiplyOrDivideFactor
  final case class ArithFactor_Divide(factor: ArithFactor) extends MultiplyOrDivideFactor

  final case class ArithFactor(primary: PlusOrMinusPrimary)

  sealed trait PlusOrMinusPrimary
  final case class ArithPrimary_Plus(primary: ArithPrimary) extends PlusOrMinusPrimary
  final case class ArithPrimary_Minus(primary: ArithPrimary) extends PlusOrMinusPrimary

  sealed trait ArithPrimary
  final case class ArithPrimary_PathExprOrVarAccess(expr: PathExprOrVarAccess) extends ArithPrimary
  final case class ArithPrimary_InputParam(expr: InputParam) extends ArithPrimary
  final case class ArithPrimary_CaseExpr(expr: CaseExpr) extends ArithPrimary
  final case class ArithPrimary_FuncsReturningNumeric(expr: FuncsReturningNumeric) extends ArithPrimary
  final case class ArithPrimary_SimpleArithExpr(expr: SimpleArithExpr) extends ArithPrimary
  final case class ArithPrimary_LiteralNumeric(expr: Number) extends ArithPrimary

  sealed trait ScalarExpr
  final case class ScalarExpr_SimpleArithExpr(expr: SimpleArithExpr) extends ScalarExpr
  final case class ScalarExpr_NonArithScalarExpr(expr: NonArithScalarExpr) extends ScalarExpr

  sealed trait ScalarOrSubselectExpr
  final case class ScalarOrSubselectExpr_ArithExpr(expr: ArithExpr) extends ScalarOrSubselectExpr
  final case class ScalarOrSubselectExpr_NonArithScalarExpr(expr: NonArithScalarExpr) extends ScalarOrSubselectExpr

  sealed trait NonArithScalarExpr
  final case class NonArithScalarExpr_FuncsReturningDatetime(expr: FuncsReturningDatetime) extends NonArithScalarExpr
  final case class NonArithScalarExpr_FuncsReturningString(expr: FuncsReturningString) extends NonArithScalarExpr
  final case class NonArithScalarExpr_LiteralString(expr: String) extends NonArithScalarExpr
  final case class NonArithScalarExpr_LiteralBoolean(expr: Boolean) extends NonArithScalarExpr
  final case class NonArithScalarExpr_LiteralTemporal(expr: java.time.temporal.Temporal) extends NonArithScalarExpr
  final case class NonArithScalarExpr_EntityTypeExpr(expr: EntityTypeExpr) extends NonArithScalarExpr

  final case class AnyOrAllExpr(anyOrAll: AnyOrAll, subquery: Subquery)

  sealed trait AnyOrAll
  case object ALL extends AnyOrAll
  case object ANY extends AnyOrAll
  case object SOME extends AnyOrAll

  final case class EntityTypeExpr(typeDis: TypeDiscriminator)

  final case class TypeDiscriminator(expr: Either[VarOrSingleValuedPath, InputParam])

  sealed trait CaseExpr
  final case class CaseExpr_SimpleCaseExpr(expr: SimpleCaseExpr) extends CaseExpr
  final case class CaseExpr_GeneralCaseExpr(expr: GeneralCaseExpr) extends CaseExpr
  final case class CaseExpr_CoalesceExpr(expr: CoalesceExpr) extends CaseExpr
  final case class CaseExpr_NullifExpr(expr: NullifExpr) extends CaseExpr

  final case class SimpleCaseExpr(caseOperand: CaseOperand, when: SimpleWhenClause, whens: List[SimpleWhenClause], elseExpr: ScalarExpr)

  final case class GeneralCaseExpr(when: WhenClause, whens: List[WhenClause], elseExpr: ScalarExpr)

  final case class CoalesceExpr(expr: ScalarExpr, exprs: List[ScalarExpr])

  final case class NullifExpr(leftExpr: ScalarExpr, rightExpr: ScalarExpr)

  final case class CaseOperand(expr: Either[StateFieldPathExpr, TypeDiscriminator])

  final case class WhenClause(when: CondExpr, thenExpr: ScalarExpr)

  final case class SimpleWhenClause(when: ScalarExpr, thenExpr: ScalarExpr)

  final case class VarOrSingleValuedPath(expr: Either[SingleValuedPathExpr, VarAccessOrTypeConstant])

  sealed trait StringPrimary
  final case class StringPrimary_LiteralString(expr: String) extends StringPrimary
  final case class StringPrimary_FuncsReturningString(expr: FuncsReturningString) extends StringPrimary
  final case class StringPrimary_InputParam(expr: InputParam) extends StringPrimary
  final case class StringPrimary_StateFieldPathExpr(expr: StateFieldPathExpr) extends StringPrimary

  sealed trait Literal
  final case class LiteralBoolean(v: Boolean) extends Literal
  final case class LiteralString(v: String) extends Literal

  sealed trait LiteralNumeric extends Literal
  final case class LiteralInteger(v: Int) extends LiteralNumeric
  final case class LiteralLong(v: Long) extends LiteralNumeric
  final case class LiteralFloat(v: Float) extends LiteralNumeric
  final case class LiteralDouble(v: Double) extends LiteralNumeric

  sealed trait LiteralTemporal
  final case class LiteralDate(date: java.time.LocalDate) extends LiteralTemporal
  final case class LiteralTime(time: java.time.LocalTime) extends LiteralTemporal
  final case class LiteralTimestamp(time: java.time.LocalDateTime) extends LiteralTemporal

  sealed trait InputParam
  final case class InputParam_Named(name: String) extends InputParam
  final case class InputParam_Position(pos: Int) extends InputParam

  sealed trait FuncsReturningNumeric
  final case class Abs(expr: SimpleArithExpr) extends FuncsReturningNumeric
  final case class Length(expr: ScalarExpr) extends FuncsReturningNumeric
  final case class Mod(expr: ScalarExpr, divisorExpr: ScalarExpr) extends FuncsReturningNumeric
  final case class Locate(expr: ScalarExpr, searchExpr: ScalarExpr, startExpr: Option[ScalarExpr]) extends FuncsReturningNumeric
  final case class Size(expr: CollectionValuedPathExpr) extends FuncsReturningNumeric
  final case class Sqrt(expr: ScalarExpr) extends FuncsReturningNumeric
  final case class Index(expr: VarAccessOrTypeConstant) extends FuncsReturningNumeric
  final case class Func(name: String, args: List[NewValue]) extends FuncsReturningNumeric

  sealed trait FuncsReturningDatetime
  case object CURRENT_DATE extends FuncsReturningDatetime
  case object CURRENT_TIME extends FuncsReturningDatetime
  case object CURRENT_TIMESTAMP extends FuncsReturningDatetime

  sealed trait FuncsReturningString
  final case class Concat(expr: ScalarExpr, exprs: List[ScalarExpr]) extends FuncsReturningString
  final case class Substring(expr: ScalarExpr, startExpr: ScalarExpr, lengthExpr: Option[ScalarExpr]) extends FuncsReturningString
  final case class Trim(trimSpec: Option[TrimSpec], trimChar: Option[TrimChar], from: StringPrimary) extends FuncsReturningString
  final case class Upper(expr: ScalarExpr) extends FuncsReturningString
  final case class Lower(expr: ScalarExpr) extends FuncsReturningString
  final case class MapKey(expr: VarAccessOrTypeConstant) extends FuncsReturningString

  sealed trait FuncsReturningAny
  final case class MapValue(expr: VarAccessOrTypeConstant) extends FuncsReturningAny
  final case class JPQLJsonValue(jsonNode: org.codehaus.jackson.JsonNode) extends FuncsReturningAny

  sealed trait TrimSpec
  case object LEADING extends TrimSpec
  case object TRAILING extends TrimSpec
  case object BOTH extends TrimSpec

  sealed trait TrimChar
  final case class TrimChar_String(char: String) extends TrimChar
  final case class TrimChar_InputParam(param: InputParam) extends TrimChar

  final case class Subquery(select: SimpleSelectClause, from: SubqueryFromClause, where: Option[WhereClause], groupby: Option[GroupbyClause], having: Option[HavingClause])

  final case class SimpleSelectClause(isDistinct: Boolean, expr: SimpleSelectExpr)

  sealed trait SimpleSelectExpr
  final case class SimpleSelectExpr_SingleValuedPathExpr(expr: SingleValuedPathExpr) extends SimpleSelectExpr
  final case class SimpleSelectExpr_AggregateExpr(expr: AggregateExpr) extends SimpleSelectExpr
  final case class SimpleSelectExpr_VarAccessOrTypeConstant(expr: VarAccessOrTypeConstant) extends SimpleSelectExpr

  final case class SubqueryFromClause(from: SubselectIdentVarDecl, froms: List[Either[SubselectIdentVarDecl, CollectionMemberDecl]])

  sealed trait SubselectIdentVarDecl
  final case class SubselectIdentVarDecl_IdentVarDecl(expr: IdentVarDecl) extends SubselectIdentVarDecl
  final case class SubselectIdentVarDecl_AssocPathExpr(expr: AssocPathExpr, as: Ident) extends SubselectIdentVarDecl
  final case class SubselectIdentVarDecl_CollectionMemberDecl(expr: CollectionMemberDecl) extends SubselectIdentVarDecl

  final case class OrderbyClause(orderby: OrderbyItem, orderbys: List[OrderbyItem])

  final case class OrderbyItem(expr: Either[SimpleArithExpr, ScalarExpr], isAsc: Boolean)

  final case class GroupbyClause(expr: ScalarExpr, exprs: List[ScalarExpr])

  final case class HavingClause(condExpr: CondExpr)

  final case class Ident(ident: String)

}