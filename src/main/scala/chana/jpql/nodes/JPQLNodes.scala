package chana.jpql.nodes

/*
 * Definition of JPQL AST nodes.
 * 
 * @author Caoyuan Deng
 */
sealed trait Statement
case class SelectStatement(select: SelectClause, from: FromClause, where: Option[WhereClause], groupby: Option[GroupbyClause], having: Option[HavingClause], orderby: Option[OrderbyClause]) extends Statement
case class UpdateStatement(update: UpdateClause, set: SetClause, where: Option[WhereClause]) extends Statement
case class DeleteStatement(delete: DeleteClause, where: Option[WhereClause]) extends Statement

case class UpdateClause(entityName: EntityName, as: Option[Ident])

case class SetClause(assign: SetAssignClause, assigns: List[SetAssignClause])

case class SetAssignClause(target: SetAssignTarget, value: NewValue)

case class SetAssignTarget(path: Either[PathExpr, Attribute])

case class NewValue(v: ScalarExpr) // could be null

case class DeleteClause(from: EntityName, as: Option[Ident])

case class SelectClause(isDistinct: Boolean, item: SelectItem, items: List[SelectItem])

case class SelectItem(expr: SelectExpr, as: Option[Ident])

sealed trait SelectExpr
case class SelectExpr_AggregateExpr(expr: AggregateExpr) extends SelectExpr
case class SelectExpr_ScalarExpr(expr: ScalarExpr) extends SelectExpr
case class SelectExpr_OBJECT(expr: VarAccessOrTypeConstant) extends SelectExpr
case class SelectExpr_ConstructorExpr(expr: ConstructorExpr) extends SelectExpr
case class SelectExpr_MapEntryExpr(expr: MapEntryExpr) extends SelectExpr

case class MapEntryExpr(entry: VarAccessOrTypeConstant)

case class PathExprOrVarAccess(qual: QualIdentVar, attributes: List[Attribute])

sealed trait QualIdentVar
case class QualIdentVar_VarAccessOrTypeConstant(v: VarAccessOrTypeConstant) extends QualIdentVar
case class QualIdentVar_KEY(v: VarAccessOrTypeConstant) extends QualIdentVar
case class QualIdentVar_VALUE(v: VarAccessOrTypeConstant) extends QualIdentVar

sealed trait AggregateExpr
case class AggregateExpr_AVG(isDistinct: Boolean, expr: ScalarExpr) extends AggregateExpr
case class AggregateExpr_MAX(isDistinct: Boolean, expr: ScalarExpr) extends AggregateExpr
case class AggregateExpr_MIN(isDistinct: Boolean, expr: ScalarExpr) extends AggregateExpr
case class AggregateExpr_SUM(isDistinct: Boolean, expr: ScalarExpr) extends AggregateExpr
case class AggregateExpr_COUNT(isDistinct: Boolean, expr: ScalarExpr) extends AggregateExpr

case class ConstructorExpr(name: ConstructorName, arg: ConstructorItem, args: List[ConstructorItem])

case class ConstructorName(id: Ident, ids: List[Ident])

sealed trait ConstructorItem
case class ConstructorItem_ScalarExpr(expr: ScalarExpr) extends ConstructorItem
case class ConstructorItem_AggregateExpr(expr: AggregateExpr) extends ConstructorItem

case class FromClause(from: IdentVarDecl, froms: List[Either[IdentVarDecl, CollectionMemberDecl]])

case class IdentVarDecl(range: RangeVarDecl, joins: List[Join])

case class RangeVarDecl(entityName: EntityName, as: Ident)

case class EntityName(ident: String)

sealed trait Join
case class Join_General(spec: JoinSpec, expr: JoinAssocPathExpr, as: Ident, cond: Option[JoinCond]) extends Join
case class Join_TREAT(spec: JoinSpec, expr: JoinAssocPathExpr, exprAs: Ident, as: Ident, cond: Option[JoinCond]) extends Join
case class Join_FETCH(spec: JoinSpec, expr: JoinAssocPathExpr, alias: Option[Ident], cond: Option[JoinCond]) extends Join

sealed trait JoinSpec
case object JOIN extends JoinSpec
case object LEFT_JOIN extends JoinSpec
case object LEFT_OUTER_JOIN extends JoinSpec
case object INNER_JOIN extends JoinSpec

case class JoinCond(expr: CondExpr)

case class CollectionMemberDecl(in: CollectionValuedPathExpr, as: Ident)

case class CollectionValuedPathExpr(path: PathExpr)

case class AssocPathExpr(path: PathExpr)

case class JoinAssocPathExpr(qualId: QualIdentVar, attrbutes: List[Attribute])

case class SingleValuedPathExpr(path: PathExpr)

case class StateFieldPathExpr(path: PathExpr)

case class PathExpr(qual: QualIdentVar, attributes: List[Attribute])

case class Attribute(name: String)

case class VarAccessOrTypeConstant(id: Ident)

case class WhereClause(expr: CondExpr)

case class CondExpr(term: CondTerm, orTerms: List[CondTerm])

case class CondTerm(factor: CondFactor, andFactors: List[CondFactor])

case class CondFactor(not: Boolean, expr: Either[CondPrimary, ExistsExpr])

sealed trait CondPrimary
case class CondPrimary_CondExpr(expr: CondExpr) extends CondPrimary
case class CondPrimary_SimpleCondExpr(expr: SimpleCondExpr) extends CondPrimary

case class SimpleCondExpr(expr: Either[ArithExpr, NonArithScalarExpr], rem: SimpleCondExprRem)

sealed trait SimpleCondExprRem
case class SimpleCondExprRem_ComparisonExpr(expr: ComparisonExpr) extends SimpleCondExprRem
case class SimpleCondExprRem_CondWithNotExpr(not: Boolean, expr: CondWithNotExpr) extends SimpleCondExprRem
case class SimpleCondExprRem_IsExpr(not: Boolean, expr: IsExpr) extends SimpleCondExprRem

sealed trait CondWithNotExpr
case class CondWithNotExpr_BetweenExpr(expr: BetweenExpr) extends CondWithNotExpr
case class CondWithNotExpr_LikeExpr(expr: LikeExpr) extends CondWithNotExpr
case class CondWithNotExpr_InExpr(expr: InExpr) extends CondWithNotExpr
case class CondWithNotExpr_CollectionMemberExpr(expr: CollectionMemberExpr) extends CondWithNotExpr

sealed trait IsExpr
case object IsNullExpr extends IsExpr // NULL 
case object IsEmptyExpr extends IsExpr // EMPTY

case class BetweenExpr(min: ScalarOrSubselectExpr, max: ScalarOrSubselectExpr)

sealed trait InExpr
case class InExpr_InputParam(expr: InputParam) extends InExpr
case class InExpr_ScalarOrSubselectExpr(expr: ScalarOrSubselectExpr, exprs: List[ScalarOrSubselectExpr]) extends InExpr
case class InExpr_Subquery(expr: Subquery) extends InExpr

case class LikeExpr(like: ScalarOrSubselectExpr, escape: Option[Escape])

case class Escape(expr: ScalarExpr)

case class CollectionMemberExpr(of: CollectionValuedPathExpr)

case class ExistsExpr(subquery: Subquery)

case class ComparisonExpr(op: ComparisonOp, operand: ComparsionExprRightOperand)

sealed trait ComparisonOp
case object EQ extends ComparisonOp
case object NE extends ComparisonOp
case object GT extends ComparisonOp
case object GE extends ComparisonOp
case object LT extends ComparisonOp
case object LE extends ComparisonOp

sealed trait ComparsionExprRightOperand
case class ComparsionExprRightOperand_ArithExpr(expr: ArithExpr) extends ComparsionExprRightOperand
case class ComparsionExprRightOperand_NonArithScalarExpr(expr: NonArithScalarExpr) extends ComparsionExprRightOperand
case class ComparsionExprRightOperand_AnyOrAllExpr(expr: AnyOrAllExpr) extends ComparsionExprRightOperand

case class ArithExpr(expr: Either[SimpleArithExpr, Subquery])

case class SimpleArithExpr(term: ArithTerm, rightTerms: List[PlusOrMinusTerm])

sealed trait PlusOrMinusTerm
case class ArithTerm_Plus(term: ArithTerm) extends PlusOrMinusTerm
case class ArithTerm_Minus(term: ArithTerm) extends PlusOrMinusTerm

case class ArithTerm(factor: ArithFactor, rightFactors: List[MultiplyOrDivideFactor])

sealed trait MultiplyOrDivideFactor
case class ArithFactor_Multiply(factor: ArithFactor) extends MultiplyOrDivideFactor
case class ArithFactor_Divide(factor: ArithFactor) extends MultiplyOrDivideFactor

case class ArithFactor(primary: PlusOrMinusPrimary)

sealed trait PlusOrMinusPrimary
case class ArithPrimary_Plus(primary: ArithPrimary) extends PlusOrMinusPrimary
case class ArithPrimary_Minus(primary: ArithPrimary) extends PlusOrMinusPrimary

sealed trait ArithPrimary
case class ArithPrimary_PathExprOrVarAccess(expr: PathExprOrVarAccess) extends ArithPrimary
case class ArithPrimary_InputParam(expr: InputParam) extends ArithPrimary
case class ArithPrimary_CaseExpr(expr: CaseExpr) extends ArithPrimary
case class ArithPrimary_FuncsReturningNumerics(expr: FuncsReturningNumerics) extends ArithPrimary
case class ArithPrimary_SimpleArithExpr(expr: SimpleArithExpr) extends ArithPrimary
case class ArithPrimary_LiteralNumeric(expr: Number) extends ArithPrimary

sealed trait ScalarExpr
case class ScalarExpr_SimpleArithExpr(expr: SimpleArithExpr) extends ScalarExpr
case class ScalarExpr_NonArithScalarExpr(expr: NonArithScalarExpr) extends ScalarExpr

sealed trait ScalarOrSubselectExpr
case class ScalarOrSubselectExpr_ArithExpr(expr: ArithExpr) extends ScalarOrSubselectExpr
case class ScalarOrSubselectExpr_NonArithScalarExpr(expr: NonArithScalarExpr) extends ScalarOrSubselectExpr

sealed trait NonArithScalarExpr
case class NonArithScalarExpr_FuncsReturningDatetime(expr: FuncsReturningDatetime) extends NonArithScalarExpr
case class NonArithScalarExpr_FuncsReturningStrings(expr: FuncsReturningStrings) extends NonArithScalarExpr
case class NonArithScalarExpr_LiteralString(expr: String) extends NonArithScalarExpr
case class NonArithScalarExpr_LiteralBoolean(expr: Boolean) extends NonArithScalarExpr
case class NonArithScalarExpr_LiteralTemporal(expr: java.time.temporal.Temporal) extends NonArithScalarExpr
case class NonArithScalarExpr_EntityTypeExpr(expr: EntityTypeExpr) extends NonArithScalarExpr

case class AnyOrAllExpr(anyOrAll: AnyOrAll, subquery: Subquery)

sealed trait AnyOrAll
case object ALL extends AnyOrAll
case object ANY extends AnyOrAll
case object SOME extends AnyOrAll

case class EntityTypeExpr(typeDis: TypeDiscriminator)

case class TypeDiscriminator(expr: Either[VarOrSingleValuedPath, InputParam])

sealed trait CaseExpr
case class CaseExpr_SimpleCaseExpr(expr: SimpleCaseExpr) extends CaseExpr
case class CaseExpr_GeneralCaseExpr(expr: GeneralCaseExpr) extends CaseExpr
case class CaseExpr_CoalesceExpr(expr: CoalesceExpr) extends CaseExpr
case class CaseExpr_NullifExpr(expr: NullifExpr) extends CaseExpr

case class SimpleCaseExpr(caseOperand: CaseOperand, when: SimpleWhenClause, whens: List[SimpleWhenClause], elseExpr: ScalarExpr)

case class GeneralCaseExpr(when: WhenClause, whens: List[WhenClause], elseExpr: ScalarExpr)

case class CoalesceExpr(expr: ScalarExpr, exprs: List[ScalarExpr])

case class NullifExpr(leftExpr: ScalarExpr, rightExpr: ScalarExpr)

case class CaseOperand(expr: Either[StateFieldPathExpr, TypeDiscriminator])

case class WhenClause(when: CondExpr, thenExpr: ScalarExpr)

case class SimpleWhenClause(when: ScalarExpr, thenExpr: ScalarExpr)

case class VarOrSingleValuedPath(expr: Either[SingleValuedPathExpr, VarAccessOrTypeConstant])

sealed trait StringPrimary
case class StringPrimary_LiteralString(expr: String) extends StringPrimary
case class StringPrimary_FuncsReturningStrings(expr: FuncsReturningStrings) extends StringPrimary
case class StringPrimary_InputParam(expr: InputParam) extends StringPrimary
case class StringPrimary_StateFieldPathExpr(expr: StateFieldPathExpr) extends StringPrimary

sealed trait Literal
case class LiteralBoolean(v: Boolean) extends Literal
case class LiteralString(v: String) extends Literal

sealed trait LiteralNumeric extends Literal
case class LiteralInteger(v: Int) extends LiteralNumeric
case class LiteralLong(v: Long) extends LiteralNumeric
case class LiteralFloat(v: Float) extends LiteralNumeric
case class LiteralDouble(v: Double) extends LiteralNumeric

sealed trait LiteralTemporal
case class LiteralDate(date: java.time.LocalDate) extends LiteralTemporal
case class LiteralTime(time: java.time.LocalTime) extends LiteralTemporal
case class LiteralTimestamp(time: java.time.LocalDateTime) extends LiteralTemporal

sealed trait InputParam
case class InputParam_Named(name: String) extends InputParam
case class InputParam_Position(pos: Int) extends InputParam

sealed trait FuncsReturningNumerics
case class Abs(expr: SimpleArithExpr) extends FuncsReturningNumerics
case class Length(expr: ScalarExpr) extends FuncsReturningNumerics
case class Mod(expr: ScalarExpr, divisorExpr: ScalarExpr) extends FuncsReturningNumerics
case class Locate(expr: ScalarExpr, searchExpr: ScalarExpr, startExpr: Option[ScalarExpr]) extends FuncsReturningNumerics
case class Size(expr: CollectionValuedPathExpr) extends FuncsReturningNumerics
case class Sqrt(expr: ScalarExpr) extends FuncsReturningNumerics
case class Index(expr: VarAccessOrTypeConstant) extends FuncsReturningNumerics
case class Func(name: String, args: List[NewValue]) extends FuncsReturningNumerics

sealed trait FuncsReturningDatetime
case object CURRENT_DATE extends FuncsReturningDatetime
case object CURRENT_TIME extends FuncsReturningDatetime
case object CURRENT_TIMESTAMP extends FuncsReturningDatetime

sealed trait FuncsReturningStrings
case class Concat(expr: ScalarExpr, exprs: List[ScalarExpr]) extends FuncsReturningStrings
case class Substring(expr: ScalarExpr, startExpr: ScalarExpr, lengthExpr: Option[ScalarExpr]) extends FuncsReturningStrings
case class Trim(trimSpec: Option[TrimSpec], trimChar: Option[TrimChar], from: StringPrimary) extends FuncsReturningStrings
case class Upper(expr: ScalarExpr) extends FuncsReturningStrings
case class Lower(expr: ScalarExpr) extends FuncsReturningStrings

sealed trait TrimSpec
case object LEADING extends TrimSpec
case object TRAILING extends TrimSpec
case object BOTH extends TrimSpec

sealed trait TrimChar
case class TrimChar_String(char: String) extends TrimChar
case class TrimChar_InputParam(param: InputParam) extends TrimChar

case class Subquery(select: SimpleSelectClause, from: SubqueryFromClause, where: Option[WhereClause], groupby: Option[GroupbyClause], having: Option[HavingClause])

case class SimpleSelectClause(isDistinct: Boolean, expr: SimpleSelectExpr)

sealed trait SimpleSelectExpr
case class SimpleSelectExpr_SingleValuedPathExpr(expr: SingleValuedPathExpr) extends SimpleSelectExpr
case class SimpleSelectExpr_AggregateExpr(expr: AggregateExpr) extends SimpleSelectExpr
case class SimpleSelectExpr_VarAccessOrTypeConstant(expr: VarAccessOrTypeConstant) extends SimpleSelectExpr

case class SubqueryFromClause(from: SubselectIdentVarDecl, froms: List[Either[SubselectIdentVarDecl, CollectionMemberDecl]])

sealed trait SubselectIdentVarDecl
case class SubselectIdentVarDecl_IdentVarDecl(expr: IdentVarDecl) extends SubselectIdentVarDecl
case class SubselectIdentVarDecl_AssocPathExpr(expr: AssocPathExpr, as: Ident) extends SubselectIdentVarDecl
case class SubselectIdentVarDecl_CollectionMemberDecl(expr: CollectionMemberDecl) extends SubselectIdentVarDecl

case class OrderbyClause(orderby: OrderbyItem, orderbys: List[OrderbyItem])

case class OrderbyItem(expr: Either[SimpleArithExpr, ScalarExpr], isAsc: Boolean)

case class GroupbyClause(expr: ScalarExpr, exprs: List[ScalarExpr])

case class HavingClause(condExpr: CondExpr)

case class Ident(ident: String)

