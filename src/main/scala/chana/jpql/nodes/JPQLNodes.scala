package chana.jpql.nodes

/*
 * Definition of JPQL AST nodes.
 * 
 * @author Caoyuan Deng
 */

trait Statement
case class SelectStatement(select: SelectClause, from: FromClause, where: Option[WhereClause], groupby: Option[GroupbyClause], having: Option[HavingClause], orderby: Option[OrderbyClause]) extends Statement
case class UpdateStatement(update: UpdateClause, where: Option[WhereClause]) extends Statement
case class DeleteStatement(delete: DeleteClause, where: Option[WhereClause]) extends Statement

case class FromClause(from: IdentVarDecl, froms: List[Either[IdentVarDecl, CollectionMemberDecl]])

case class IdentVarDecl(rangeVarDecl: RangeVarDecl, joins: List[Either[Join, FetchJoin]])

case class RangeVarDecl(entityName: EntityName, as: IdentVar)

case class Join(joinSpec: JoinSpec, joinAssocExpr: JoinAssocPathExpr, as: IdentVar, joinCond: Option[JoinCond])

case class FetchJoin(joinSpec: JoinSpec, fetch: JoinAssocPathExpr, as: IdentVar, joinCond: Option[JoinCond])

trait JoinSpec
case object JOIN_Spec extends JoinSpec
case object LEFT_JOIN_Spec extends JoinSpec
case object LEFT_OUTER_JOIN_Spec extends JoinSpec
case object INNER_JOIN_Spec extends JoinSpec

case class JoinCond(on: CondExpr)

trait JoinAssocPathExpr
case class JoinAssocPathExpr_JoinCollectionValuedPathExpr(path: IdentVar, paths: List[SingleValuedEmbeddableObjectField], field: CollectionValuedField, as: Option[Subtype]) extends JoinAssocPathExpr
case class JoinAssocPathExpr_JoinSingleValuedPathExpr(path: IdentVar, paths: List[SingleValuedEmbeddableObjectField], field: SingleValuedObjectField, as: Option[Subtype]) extends JoinAssocPathExpr

case class CollectionMemberDecl(in: CollectionValuedPathExpr, as: IdentVar)

trait QualIdentVar
case class QualIdentVar_ComposableQualIdentVar(variable: ComposableQualIdentVar) extends QualIdentVar
case class QualIdentVar_ENTRY(variable: IdentVar) extends QualIdentVar

trait ComposableQualIdentVar
case class ComposableQualIdentVar_KEY(name: IdentVar) extends ComposableQualIdentVar
case class ComposableQualIdentVar_VALUE(name: IdentVar) extends ComposableQualIdentVar

trait SingleValuedPathExpr
case class SingleValuedPathExpr_QualIdentVar(variable: QualIdentVar) extends SingleValuedPathExpr
case class SingleValuedPathExpr_TREAT(variable: QualIdentVar) extends SingleValuedPathExpr
case class SingleValuedPathExpr_StateFieldPathExpr(variable: StateFieldPathExpr) extends SingleValuedPathExpr
case class SingleValuedPathExpr_SingleValuedObjectPathExpr(variable: SingleValuedObjectPathExpr) extends SingleValuedPathExpr

trait GeneralIdentVar
case class GeneralIdentVar_Identvariable(variable: IdentVar) extends GeneralIdentVar
case class GeneralIdentVar_ComposableQualIdentVar(variable: ComposableQualIdentVar) extends GeneralIdentVar

trait GeneralSubpath
case class GeneralSubpath_SimpleSubpath(subpath: SimpleSubpath) extends GeneralSubpath
case class GeneralSubpath_TreatedSubpath(subpath: TreatedSubpath, fields: List[SingleValuedObjectField]) extends GeneralSubpath

case class SimpleSubpath(path: GeneralIdentVar, paths: List[SingleValuedObjectField])

case class TreatedSubpath(path: GeneralSubpath, as: Subtype)

case class StateFieldPathExpr(path: GeneralSubpath, field: StateField)

case class SingleValuedObjectPathExpr(path: GeneralSubpath, field: SingleValuedObjectField)

case class CollectionValuedPathExpr(path: GeneralSubpath, field: CollectionValuedField)

case class UpdateClause(entityName: EntityName, as: Option[IdentVar], set: List[UpdateItem])

case class UpdateItem(id: Option[IdentVar], fields: List[SingleValuedEmbeddableObjectField], field: Either[StateField, SingleValuedObjectField], newValue: NewValue)

trait NewValue // = ScalarExpr

case class DeleteClause(from: EntityName, as: Option[IdentVar])

case class SelectClause(isDistince: Boolean, selectItems: List[SelectItem])

case class SelectItem(selectExpr: SelectExpr, as: Option[ResultVar])

trait SelectExpr
case class SelectExpr_AggregateExpr(expr: AggregateExpr) extends SelectExpr
case class SelectExpr_OBJECT(id: IdentVar) extends SelectExpr
case class SelectExpr_ConstructorExpr(expr: ConstructorExpr) extends SelectExpr
case class SelectExpr_SingleValuedPathExpr(expr: SingleValuedPathExpr) extends SelectExpr
case class SelectExpr_ScalarExpr(expr: ScalarExpr) extends SelectExpr
//;

case class ConstructorExpr(constructorName: ConstructorName, constructorItems: List[ConstructorItem])

trait ConstructorItem
case class AggregateExpr_ConstructorItem(expr: AggregateExpr) extends ConstructorItem
case class SingleValuedPathExpr_ConstructorItem(expr: SingleValuedPathExpr) extends ConstructorItem
case class ScalarExpr_ConstructorItem(expr: ScalarExpr) extends ConstructorItem

trait AggregateExpr
case class AggregateExpr_AVG(isDistinct: Boolean, field: StateFieldPathExpr) extends AggregateExpr
case class AggregateExpr_MAX_(isDistinct: Boolean, field: StateFieldPathExpr) extends AggregateExpr
case class AggregateExpr_MIN(isDistinct: Boolean, field: StateFieldPathExpr) extends AggregateExpr
case class AggregateExpr_SUM(isDistinct: Boolean, field: StateFieldPathExpr) extends AggregateExpr
case class AggregateExpr_COUNT1(isDistinct: Boolean, field: IdentVar) extends AggregateExpr
case class AggregateExpr_COUNT2(isDistinct: Boolean, field: StateFieldPathExpr) extends AggregateExpr
case class AggregateExpr_COUNT3(isDistinct: Boolean, field: SingleValuedObjectPathExpr) extends AggregateExpr
case class AggregateExpr_FuncInvocation(function: FuncInvocation) extends AggregateExpr

case class WhereClause(where: CondExpr)

case class GroupbyClause(groupbyItems: List[GroupbyItem])

trait GroupbyItem
case class GroupbyItem_SingleValuedPathExpr(x: SingleValuedPathExpr) extends GroupbyItem
case class GroupbyItem_IdentVar(x: QualIdentVar) extends GroupbyItem

case class HavingClause(having: CondExpr)

case class OrderbyClause(orderbyItems: List[OrderbyItem])

case class OrderbyItem(item: ScalarExpr, isAsc: Boolean)

case class Subquery(simpleSelect: SimpleSelectClause, subqueryFrom: SubqueryFromClause, where: Option[WhereClause], groupby: Option[GroupbyClause], having: Option[HavingClause])

case class SubqueryFromClause(from: SubselectIdentVarDecl, names: List[Either[SubselectIdentVarDecl, CollectionMemberDecl]])

trait SubselectIdentVarDecl
case class SubselectIdentVarDecl_IdentVarDecl(variable: IdentVarDecl) extends SubselectIdentVarDecl
case class SubselectIdentVarDecl_DerivedPathExpr(variable: DerivedPathExpr, as: Option[IdentVar], joins: List[Join]) extends SubselectIdentVarDecl
case class SubselectIdentVarDecl_DerivedCollectionMemberDecl(variable: DerivedCollectionMemberDecl) extends SubselectIdentVarDecl

case class DerivedPathExpr(path: GeneralDerivedPath, field: Either[SingleValuedObjectField, CollectionValuedField])

trait GeneralDerivedPath
case class GeneralDerivedPath_SimpleDerivedPath(path: SimpleDerivedPath) extends GeneralDerivedPath
case class GeneralDerivedPath_TreatedDerivedPath(path: TreatedDerivedPath, fields: List[SingleValuedObjectField]) extends GeneralDerivedPath

case class SimpleDerivedPath(variable: SuperqueryIdentVar, fields: List[SingleValuedObjectField])

case class TreatedDerivedPath(path: GeneralDerivedPath, as: Subtype)

case class DerivedCollectionMemberDecl(in: SuperqueryIdentVar, fields: List[SingleValuedObjectField], field: CollectionValuedField)

case class SimpleSelectClause(isDistince: Boolean, expression: SimpleSelectExpr)

trait SimpleSelectExpr
case class SimpleSelectExpr_AggregateExpr(expr: AggregateExpr) extends SimpleSelectExpr
case class SimpleSelectExpr_SingleValuedPathExpr(expr: SingleValuedPathExpr) extends SimpleSelectExpr
case class SimpleSelectExpr_ScalarExpr(expr: ScalarExpr) extends SimpleSelectExpr

trait ScalarExpr
case class ScalarExpr_StringExpr(expr: StringExpr) extends ScalarExpr
case class ScalarExpr_EnumExpr(expr: EnumExpr) extends ScalarExpr
case class ScalarExpr_DatetimeExpr(expr: DatetimeExpr) extends ScalarExpr
case class ScalarExpr_BooleanExpr(expr: BooleanExpr) extends ScalarExpr
case class ScalarExpr_CaseExpr(expr: CaseExpr) extends ScalarExpr
case class ScalarExpr_EntityTypeExpr(expr: EntityTypeExpr) extends ScalarExpr
case class ScalarExpr_ArithExpr(expr: ArithExpr) extends ScalarExpr

case class CondExpr(term: CondTerm, orConds: List[CondExpr])

case class CondTerm(factor: CondFactor, andConds: List[CondFactor])

case class CondFactor(isNot: Boolean, cond: CondPrimary)

trait CondPrimary
case class CondPrimary_SimpleCondExpr(expr: SimpleCondExpr) extends CondPrimary
case class CondPrimary_CondExpr(expr: CondExpr) extends CondPrimary

trait SimpleCondExpr
case class SimpleCondExpr_ComparisonExpr(expr: ComparisonExpr) extends SimpleCondExpr
case class SimpleCondExpr_BetweenExpr(expr: BetweenExpr) extends SimpleCondExpr
case class SimpleCondExpr_LikeExpr(expr: LikeExpr) extends SimpleCondExpr
case class SimpleCondExpr_InExpr(expr: InExpr) extends SimpleCondExpr
case class SimpleCondExpr_NullComparisonExpr(expr: NullComparisonExpr) extends SimpleCondExpr
case class SimpleCondExpr_EmptyCollectionComparisonExpr(expr: EmptyCollectionComparisonExpr) extends SimpleCondExpr
case class SimpleCondExpr_CollectionMemberExpr(expr: CollectionMemberExpr) extends SimpleCondExpr
case class SimpleCondExpr_ExistsExpr(expr: ExistsExpr) extends SimpleCondExpr

trait BetweenExpr
case class BetweenExpr_ArithExpr(expr: ArithExpr, isNot: Boolean, left: ArithExpr, right: ArithExpr) extends BetweenExpr
case class BetweenExpr_StringExpr(expr: StringExpr, isNot: Boolean, left: StringExpr, right: StringExpr) extends BetweenExpr
case class BetweenExpr_DatetimeExpr(expr: DatetimeExpr, isNot: Boolean, left: DatetimeExpr, right: DatetimeExpr) extends BetweenExpr

case class InExpr(expr: Either[StateFieldPathExpr, TypeDiscriminator], isNot: Boolean, inItems: List[InItem], inSubqury: Subquery, inCollection: CollectionValuedInputParam)

trait InItem
case class InItem_Literal(item: Literal) extends InItem
case class InItem_SingleValuedInputParam(item: SingleValuedInputParam) extends InItem

case class LikeExpr(expr: StringExpr, isNot: Boolean, like: PatternValue, escape: Option[EscapeChar])

case class NullComparisonExpr(expr: Either[SingleValuedPathExpr, InputParam], isNull: Boolean)

case class EmptyCollectionComparisonExpr(expr: CollectionValuedPathExpr, isEmpty: Boolean)

case class CollectionMemberExpr(expr: EntityOrValueExpr, isNot: Boolean, memberOf: CollectionValuedPathExpr)

trait EntityOrValueExpr
case class EntityOrValueExpr_SingleValuedObjectPathExpr(expr: SingleValuedObjectPathExpr) extends EntityOrValueExpr
case class EntityOrValueExpr_StateFieldPathExpr(expr: StateFieldPathExpr) extends EntityOrValueExpr
case class EntityOrValueExprP_SimpleEntityOrValueExpr(expr: SimpleEntityOrValueExpr) extends EntityOrValueExpr

trait SimpleEntityOrValueExpr
case class SimpleEntityOrValueExpr_IdentVar(expr: IdentVar) extends SimpleEntityOrValueExpr
case class SimpleEntityOrValueExpr_InputParam(expr: InputParam) extends SimpleEntityOrValueExpr
case class SimpleEntityOrValueExpr_Literal(expr: Literal) extends SimpleEntityOrValueExpr

case class ExistsExpr(isNot: Boolean, exists: Subquery)

trait AllOrAnyExpr
case class AllOrAnyExpr_All(subquery: Subquery) extends AllOrAnyExpr
case class AllOrAnyExpr_Any(subquery: Subquery) extends AllOrAnyExpr
case class AllOrAnyExpr_Some(subquery: Subquery) extends AllOrAnyExpr

trait ComparisonExpr
case class ComparisonExpr_StringExpr(op: ComparisonOp, rightExpr: Either[StringExpr, AllOrAnyExpr])
case class ComparisonExpr_BooleanExpr(isEq: Boolean, rightExpr: Either[BooleanExpr, AllOrAnyExpr])
case class ComparisonExpr_EnumExpr(isEq: Boolean, rightExpr: Either[EnumExpr, AllOrAnyExpr])
case class ComparisonExpr_DatetimeExpr(op: ComparisonOp, rightExpr: Either[DatetimeExpr, AllOrAnyExpr])
case class ComparisonExpr_EntityExpr(isEq: Boolean, rightExpr: Either[EntityExpr, AllOrAnyExpr])
case class ComparisonExpr_EntityTypeExpr(isEq: Boolean, rightExpr: EntityTypeExpr)
case class ComparisonExpr_ArithExpr(op: ComparisonOp, rightExpr: Either[AllOrAnyExpr, ArithExpr])

trait ComparisonOp
case object EQ extends ComparisonOp
case object GE extends ComparisonOp
case object GT extends ComparisonOp
case object NE extends ComparisonOp
case object LT extends ComparisonOp
case object LE extends ComparisonOp

/**
 * rightTerms: Left - Plus, Right - Minus
 */
case class ArithExpr(term: ArithTerm, rightTerms: List[Either[ArithExpr, ArithExpr]])

/**
 * rightTerms: Left - Times, Right - Div
 */
case class ArithTerm(factor: ArithFactor, rightTerms: List[Either[ArithTerm, ArithTerm]])

trait ArithOp
case object Plus extends ArithOp
case object Minus extends ArithOp
case object Times extends ArithOp
case object Div extends ArithOp

/**
 * prefix: Left - Plus, Right - Minus
 */
case class ArithFactor(prefix: Either[ArithOp, ArithOp], primary: ArithPrimary)

trait ArithPrimary
case class ArithPrimary_StateFieldPathExpr(expr: StateFieldPathExpr) extends ArithPrimary
case class ArithPrimary_NumericLiteral(expr: NumericLiteral) extends ArithPrimary
case class ArithPrimary_ArithExpr(expr: ArithExpr) extends ArithPrimary
case class ArithPrimary_InputParam(expr: InputParam) extends ArithPrimary
case class ArithPrimary_FuncsReturningNumerics(expr: FuncsReturningNumerics) extends ArithPrimary
case class ArithPrimary_AggregateExpr(expr: AggregateExpr) extends ArithPrimary
case class ArithPrimary_CaseExpr(expr: CaseExpr) extends ArithPrimary
case class ArithPrimary_FuncInvocation(expr: FuncInvocation) extends ArithPrimary
case class ArithPrimary_Subquery(expr: Subquery) extends ArithPrimary

trait StringExpr
case class StringExpr_StateFieldPathExpr(expr: StateFieldPathExpr) extends StringExpr
case class StringExpr_StringLiteral(expr: StringLiteral) extends StringExpr
case class StringExpr_InputParam(expr: InputParam) extends StringExpr
case class StringExpr_FuncsReturningStrings(expr: FuncsReturningStrings) extends StringExpr
case class StringExpr_AggregateExpr(expr: AggregateExpr) extends StringExpr
case class StringExpr_CaseExpr(expr: CaseExpr) extends StringExpr
case class StringExpr_FuncInvocation(expr: FuncInvocation) extends StringExpr
case class StringExpr_Subquery(expr: Subquery) extends StringExpr

trait DatetimeExpr
case class DatetimeExpr_StateFieldPathExpr(expr: StateFieldPathExpr) extends DatetimeExpr
case class DatetimeExpr_InputParam(expr: InputParam) extends DatetimeExpr
case class DatetimeExpr_FuncsReturningDatetime(expr: FuncsReturningDatetime) extends DatetimeExpr
case class DatetimeExpr_AggregateExpr(expr: AggregateExpr) extends DatetimeExpr
case class DatetimeExpr_CaseExpr(expr: CaseExpr) extends DatetimeExpr
case class DatetimeExpr_FuncInvocation(expr: FuncInvocation) extends DatetimeExpr
case class DatetimeExpr_DatetimeTimestampLiteral(expr: DatetimeTimestampLiteral) extends DatetimeExpr
case class DatetimeExpr_Subquery(expr: Subquery) extends DatetimeExpr

trait BooleanExpr
case class BooleanExpr_StateFieldPathExpr(expr: StateFieldPathExpr) extends BooleanExpr
case class BooleanExpr_BooleanLiteral(expr: BooleanLiteral) extends BooleanExpr
case class BooleanExpr_InputParam(expr: InputParam) extends BooleanExpr
case class BooleanExpr_CaseExpr(expr: CaseExpr) extends BooleanExpr
case class BooleanExpr_FuncInvocation(expr: FuncInvocation) extends BooleanExpr
case class BooleanExpr_Subquery(expr: Subquery) extends BooleanExpr

trait EnumExpr
case class EnumExpr_StateFieldPathExpr(expr: StateFieldPathExpr) extends EnumExpr
case class EnumExpr_EnumLiteral(expr: EnumLiteral) extends EnumExpr
case class EnumExpr_InputParam(expr: InputParam) extends EnumExpr
case class EnumExpr_CaseExpr(expr: CaseExpr) extends EnumExpr
case class EnumExpr_Subquery(expr: Subquery) extends EnumExpr

trait EntityExpr
case class EntityExpr_SingleValuedObjectPathExpr(expr: SingleValuedObjectPathExpr) extends EntityExpr
case class EntityExpr_SimpleEntityExpr(expr: SimpleEntityExpr) extends EntityExpr

trait SimpleEntityExpr
case class SimpleEntityExpr_IdentVar(expr: IdentVar) extends SimpleEntityExpr
case class SimpleEntityExpr_InputParam(expr: InputParam) extends SimpleEntityExpr

trait EntityTypeExpr
case class EntityTypeExpr_TypeDiscriminator(expr: TypeDiscriminator) extends EntityTypeExpr
case class EntityTypeExpr_InputParam(expr: InputParam) extends EntityTypeExpr
case class EntityTypeExpr_EntityTypeLiteral(expr: EntityTypeLiteral) extends EntityTypeExpr

trait TypeDiscriminator
case class TypeDiscriminator_IdentVar(expr: IdentVar) extends TypeDiscriminator
case class TypeDiscriminator_SingleValuedObjectPathExpr(expr: SingleValuedObjectPathExpr) extends TypeDiscriminator
case class TypeDiscriminator_InputParam(expr: InputParam) extends TypeDiscriminator

trait FuncsReturningNumerics
case class FuncsReturningNumerics_LENGTH(expr: StringExpr) extends FuncsReturningNumerics
case class FuncsReturningNumerics_LOCATE(expr: StringExpr, expr2: StringExpr, exprs: List[ArithExpr]) extends FuncsReturningNumerics
case class FuncsReturningNumerics_ABS(expr: ArithExpr) extends FuncsReturningNumerics
case class FuncsReturningNumerics_SQRT(expr: ArithExpr) extends FuncsReturningNumerics
case class FuncsReturningNumerics_MOD(expr: ArithExpr, expr2: ArithExpr) extends FuncsReturningNumerics
case class FuncsReturningNumerics_SIZE(expr: CollectionValuedPathExpr) extends FuncsReturningNumerics
case class FuncsReturningNumerics_INDEX(expr: IdentVar) extends FuncsReturningNumerics

trait FuncsReturningDatetime
case object CURRENT_DATE extends FuncsReturningDatetime
case object CURRENT_TIME extends FuncsReturningDatetime
case object CURRENT_TIMESTAMP extends FuncsReturningDatetime

trait FuncsReturningStrings
case class FuncReturningStrings_CONCAT(expr1: StringExpr, expr2: StringExpr, exprs: List[StringExpr]) extends FuncsReturningStrings
case class FuncReturningStrings_SUBSTRING(expr1: StringExpr, expr2: ArithExpr, exprs: List[ArithExpr]) extends FuncsReturningStrings
case class FuncReturningStrings_TRIM(trimSepc: Option[TrimSpec], trimChar: Option[TrimChar], from: StringExpr) extends FuncsReturningStrings
case class FuncReturningStrings_LOWER(str: StringExpr) extends FuncsReturningStrings
case class FuncReturningStrings_UPPER(str: StringExpr) extends FuncsReturningStrings;

trait TrimSpec
case object LEADING extends TrimSpec
case object TRAILING extends TrimSpec
case object BOTH extends TrimSpec

case class FuncInvocation(functionName: StringLiteral, args: List[FuncArg])

trait FuncArg
case class FuncArg_Literal(arg: Literal) extends FuncArg
case class FuncArg_StateFieldPathExpr(arg: StateFieldPathExpr) extends FuncArg
case class FuncArg_InputParam(arg: InputParam) extends FuncArg
case class FuncArg_ScalarExpr(arg: ScalarExpr) extends FuncArg

trait CaseExpr
case class CaseExpr_GeneralCaseExpr(expr: GeneralCaseExpr) extends CaseExpr
case class CaseExpr_SimpleCaseExpr(expr: SimpleCaseExpr) extends CaseExpr
case class CaseExpr_CoalesceExpr(expr: CoalesceExpr) extends CaseExpr
case class CaseExpr_NullifExpr(expr: NullifExpr) extends CaseExpr

case class GeneralCaseExpr(whens: List[WhenClause], elseExpr: ScalarExpr)

case class WhenClause(condExpr: CondExpr, thenExpr: ScalarExpr)

case class SimpleCaseExpr(caseOperand: CaseOperand, whens: List[SimpleWhenClause], elseExpr: ScalarExpr)

trait CaseOperand
case class CaseOperand_StateFieldPathExpr(operand: StateFieldPathExpr) extends CaseOperand
case class CaseOperand_TypeDiscriminato(operand: TypeDiscriminator) extends CaseOperand

case class SimpleWhenClause(whenExpr: ScalarExpr, thenExpr: ScalarExpr)

case class CoalesceExpr(coalesceExpr: ScalarExpr, exprs: List[ScalarExpr])

case class NullifExpr(leftExpr: ScalarExpr, rightExpr: ScalarExpr)

trait Literal
case class BooleanLiteral(v: Boolean) extends Literal
case class EnumLiteral(v: List[String]) extends Literal
case class NumericLiteral(v: Double) extends Literal // TODO
case class StringLiteral(v: String) extends Literal

trait DatetimeTimestampLiteral extends Literal
case class DateLiteral(date: java.util.Date) extends DatetimeTimestampLiteral
case class TimeLiteral(time: java.util.Date) extends DatetimeTimestampLiteral
case class TimestampLiteral(timeInMills: Long) extends DatetimeTimestampLiteral

case class EntityTypeLiteral(name: Identifier) // TODO

trait InputParam
case class InputParam_Named(name: String) extends InputParam
case class InputParam_Position(i: Int) extends InputParam

case class CollectionValuedInputParam(param: InputParam)

trait PatternValue
case class PatternValue_InputParam(pattern: InputParam) extends PatternValue
case class PatternValue_StringLiteral(pattern: String) extends PatternValue

case class EscapeChar(chars: String)
case class TrimChar(chars: String)

case class IdentVar(name: Identifier)

case class ResultVar(name: Identifier)

case class EntityName(name: Identifier)

case class SingleValuedObjectField(name: PathComponent)
case class CollectionValuedField(name: PathComponent)

case class StateField(paths: List[PathComponent])

case class SingleValuedEmbeddableObjectField(name: PathComponent)
case class ConstructorName(path: Identifier, paths: List[PathComponent])
case class Subtype(name: Identifier)
case class SuperqueryIdentVar(name: Identifier)
case class SingleValuedInputParam(name: Identifier)

case class Identifier(name: String)
case class PathComponent(name: String)
