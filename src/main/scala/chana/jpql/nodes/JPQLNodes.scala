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

case class FromClause(from: IdentificationVariableDeclaration, froms: List[Either[IdentificationVariableDeclaration, CollectionMemberDeclaration]])

case class IdentificationVariableDeclaration(rangeVariableDeclaration: RangeVariableDeclaration, joins: List[Either[Join, FetchJoin]])

case class RangeVariableDeclaration(entityName: EntityName, as: IdentificationVariable)

case class Join(joinSpec: JoinSpec, joinAssociationExpression: JoinAssociationPathExpression, as: IdentificationVariable, joinCondition: Option[JoinCondition])

case class FetchJoin(joinSpec: JoinSpec, fetch: JoinAssociationPathExpression, joinCondition: Option[JoinCondition])

trait JoinSpec
case object JOIN extends JoinSpec
case object LEFT_JOIN extends JoinSpec
case object LEFT_OUTER_JOIN extends JoinSpec
case object INNER_JOIN extends JoinSpec

case class JoinCondition(on: ConditionalExpression)

trait JoinAssociationPathExpression
case class JoinAssociationPathExpression_JoinCollectionValuedPathExpression(path: IdentificationVariable, paths: List[SingleValuedEmbeddableObjectField], field: CollectionValuedField, as: Option[Subtype]) extends JoinAssociationPathExpression
case class JoinAssociationPathExpression_JoinSingleValuedPathExpression(path: IdentificationVariable, paths: List[SingleValuedEmbeddableObjectField], field: SingleValuedObjectField, as: Option[Subtype]) extends JoinAssociationPathExpression

case class CollectionMemberDeclaration(in: CollectionValuedPathExpression, as: IdentificationVariable)

trait QualifiedIdentificationVariable
case class QualifiedIdentificationVariable_ComposableQualifiedIdentificationVariable(variable: ComposableQualifiedIdentificationVariable) extends QualifiedIdentificationVariable
case class QualifiedIdentificationVariable_ENTRY(variable: IdentificationVariable) extends QualifiedIdentificationVariable

trait ComposableQualifiedIdentificationVariable
case class ComposableQualifiedIdentificationVariable_KEY(name: IdentificationVariable) extends ComposableQualifiedIdentificationVariable
case class ComposableQualifiedIdentificationVariable_VALUE(name: IdentificationVariable) extends ComposableQualifiedIdentificationVariable

trait SingleValuedPathExpression
case class SingleValuedPathExpression_QualifiedIdentificationVariable(variable: QualifiedIdentificationVariable) extends SingleValuedPathExpression
case class SingleValuedPathExpression_TREAT(variable: QualifiedIdentificationVariable) extends SingleValuedPathExpression
case class SingleValuedPathExpression_StateFieldPathExpression(variable: StateFieldPathExpression) extends SingleValuedPathExpression
case class SingleValuedPathExpression_SingleValuedObjectPathExpression(variable: SingleValuedObjectPathExpression) extends SingleValuedPathExpression

trait GeneralIdentificationVariable
case class GeneralIdentificationVariable_Identificationvariable(variable: IdentificationVariable) extends GeneralIdentificationVariable
case class GeneralIdentificationVariable_ComposableQualifiedIdentificationVariable(variable: ComposableQualifiedIdentificationVariable) extends GeneralIdentificationVariable

trait GeneralSubpath
case class GeneralSubpath_SimpleSubpath(subpath: SimpleSubpath) extends GeneralSubpath
case class GeneralSubpath_TreatedSubpath(subpath: TreatedSubpath, fields: List[SingleValuedObjectField]) extends GeneralSubpath

case class SimpleSubpath(path: GeneralIdentificationVariable, paths: List[SingleValuedObjectField])

case class TreatedSubpath(path: GeneralSubpath, as: Subtype)

case class StateFieldPathExpression(path: GeneralSubpath, field: StateField)

case class SingleValuedObjectPathExpression(path: GeneralSubpath, field: SingleValuedObjectField)

case class CollectionValuedPathExpression(path: GeneralSubpath, field: CollectionValuedField)

case class UpdateClause(entityName: EntityName, as: Option[IdentificationVariable], set: List[UpdateItem])

case class UpdateItem(id: Option[IdentificationVariable], fields: List[SingleValuedEmbeddableObjectField], field: Either[StateField, SingleValuedObjectField], newValue: NewValue)

trait NewValue // = ScalarExpression

case class DeleteClause(from: EntityName, as: Option[IdentificationVariable])

case class SelectClause(isDistince: Boolean, selectItems: List[SelectItem])

case class SelectItem(selectExpr: SelectExpression, as: Option[ResultVariable])

trait SelectExpression
case class SelectExpression_AggregateExpression(expr: AggregateExpression) extends SelectExpression
case class SelectExpression_OBJECT(id: IdentificationVariable) extends SelectExpression
case class SelectExpression_ConstructorExpression(expr: ConstructorExpression) extends SelectExpression
case class SelectExpression_SingleValuedPathExpression(expr: SingleValuedPathExpression) extends SelectExpression
case class SelectExpression_ScalarExpression(expr: ScalarExpression) extends SelectExpression
//;

case class ConstructorExpression(constructorName: ConstructorName, constructorItems: List[ConstructorItem])

trait ConstructorItem
case class AggregateExpression_ConstructorItem(expr: AggregateExpression) extends ConstructorItem
case class SingleValuedPathExpression_ConstructorItem(expr: SingleValuedPathExpression) extends ConstructorItem
case class ScalarExpression_ConstructorItem(expr: ScalarExpression) extends ConstructorItem

trait AggregateExpression
case class AggregateExpression_AVG(isDistinct: Boolean, field: StateFieldPathExpression) extends AggregateExpression
case class AggregateExpression_MAX_(isDistinct: Boolean, field: StateFieldPathExpression) extends AggregateExpression
case class AggregateExpression_MIN(isDistinct: Boolean, field: StateFieldPathExpression) extends AggregateExpression
case class AggregateExpression_SUM(isDistinct: Boolean, field: StateFieldPathExpression) extends AggregateExpression
case class AggregateExpression_COUNT1(isDistinct: Boolean, field: IdentificationVariable) extends AggregateExpression
case class AggregateExpression_COUNT2(isDistinct: Boolean, field: StateFieldPathExpression) extends AggregateExpression
case class AggregateExpression_COUNT3(isDistinct: Boolean, field: SingleValuedObjectPathExpression) extends AggregateExpression
case class AggregateExpression_FunctionInvocation(function: FunctionInvocation) extends AggregateExpression

case class WhereClause(where: ConditionalExpression)

case class GroupbyClause(groupbyItems: List[GroupbyItem])

trait GroupbyItem
case class GroupbyItem_SingleValuedPathExpression(x: SingleValuedPathExpression) extends GroupbyItem
case class GroupbyItem_IdentificationVariable(x: QualifiedIdentificationVariable) extends GroupbyItem

case class HavingClause(having: ConditionalExpression)

case class OrderbyClause(orderbyItems: List[OrderbyItem])

case class OrderbyItem(item: Either[StateFieldPathExpression, ResultVariable], isAsc: Boolean)

case class Subquery(simpleSelect: SimpleSelectClause, subqueryFrom: SubqueryFromClause, where: Option[WhereClause], groupby: Option[GroupbyClause], having: Option[HavingClause])

case class SubqueryFromClause(from: SubselectIdentificationVariableDeclaration, names: List[Either[SubselectIdentificationVariableDeclaration, CollectionMemberDeclaration]])

trait SubselectIdentificationVariableDeclaration
case class SubselectIdentificationVariableDeclaration_IdentificationVariableDeclaration(variable: IdentificationVariableDeclaration) extends SubselectIdentificationVariableDeclaration
case class SubselectIdentificationVariableDeclaration_DerivedPathExpression(variable: DerivedPathExpression, as: Option[IdentificationVariable], joins: List[Join]) extends SubselectIdentificationVariableDeclaration
case class SubselectIdentificationVariableDeclaration_DerivedCollectionMemberDeclaration(variable: DerivedCollectionMemberDeclaration) extends SubselectIdentificationVariableDeclaration

case class DerivedPathExpression(path: GeneralDerivedPath, field: Either[SingleValuedObjectField, CollectionValuedField])

trait GeneralDerivedPath
case class GeneralDerivedPath_SimpleDerivedPath(path: SimpleDerivedPath) extends GeneralDerivedPath
case class GeneralDerivedPath_TreatedDerivedPath(path: TreatedDerivedPath, fields: List[SingleValuedObjectField]) extends GeneralDerivedPath

case class SimpleDerivedPath(variable: SuperqueryIdentificationVariable, fields: List[SingleValuedObjectField])

case class TreatedDerivedPath(path: GeneralDerivedPath, as: Subtype)

case class DerivedCollectionMemberDeclaration(in: SuperqueryIdentificationVariable, fields: List[SingleValuedObjectField], field: CollectionValuedField)

case class SimpleSelectClause(isDistince: Boolean, expression: SimpleSelectExpression)

trait SimpleSelectExpression
case class SimpleSelectExpression_AggregateExpression(expr: AggregateExpression) extends SimpleSelectExpression
case class SimpleSelectExpression_SingleValuedPathExpression(expr: SingleValuedPathExpression) extends SimpleSelectExpression
case class SimpleSelectExpression_ScalarExpression(expr: ScalarExpression) extends SimpleSelectExpression

trait ScalarExpression
case class ScalarExpression_StringExpression(expr: StringExpression) extends ScalarExpression
case class ScalarExpression_EnumExpression(expr: EnumExpression) extends ScalarExpression
case class ScalarExpression_DatetimeExpression(expr: DatetimeExpression) extends ScalarExpression
case class ScalarExpression_BooleanExpression(expr: BooleanExpression) extends ScalarExpression
case class ScalarExpression_CaseExpression(expr: CaseExpression) extends ScalarExpression
case class ScalarExpression_EntityTypeExpression(expr: EntityTypeExpression) extends ScalarExpression
case class ScalarExpression_ArithmeticExpression(expr: ArithmeticExpression) extends ScalarExpression

case class ConditionalExpression(term: ConditionalTerm, orConds: List[ConditionalExpression])

case class ConditionalTerm(factor: ConditionalFactor, andConds: List[ConditionalFactor])

case class ConditionalFactor(isNot: Boolean, cond: ConditionalPrimary)

trait ConditionalPrimary
case class ConditionalPrimary_SimpleCondExpression(expr: SimpleCondExpression) extends ConditionalPrimary
case class ConditionalPrimary_ConditionalExpression(expr: ConditionalExpression) extends ConditionalPrimary

trait SimpleCondExpression
case class SimpleCondExpression_ComparisonExpression(expr: ComparisonExpression) extends SimpleCondExpression
case class SimpleCondExpression_BetweenExpression(expr: BetweenExpression) extends SimpleCondExpression
case class SimpleCondExpression_LikeExpression(expr: LikeExpression) extends SimpleCondExpression
case class SimpleCondExpression_InExpression(expr: InExpression) extends SimpleCondExpression
case class SimpleCondExpression_NullComparisonExpression(expr: NullComparisonExpression) extends SimpleCondExpression
case class SimpleCondExpression_EmptyCollectionComparisonExpression(expr: EmptyCollectionComparisonExpression) extends SimpleCondExpression
case class SimpleCondExpression_CollectionMemberExpression(expr: CollectionMemberExpression) extends SimpleCondExpression
case class SimpleCondExpression_ExistsExpression(expr: ExistsExpression) extends SimpleCondExpression

trait BetweenExpression
case class BetweenExpression_ArithmeticExpression(expr: ArithmeticExpression, isNot: Boolean, left: ArithmeticExpression, right: ArithmeticExpression) extends BetweenExpression
case class BetweenExpression_StringExpression(expr: StringExpression, isNot: Boolean, left: StringExpression, right: StringExpression) extends BetweenExpression
case class BetweenExpression_DatetimeExpression(expr: DatetimeExpression, isNot: Boolean, left: DatetimeExpression, right: DatetimeExpression) extends BetweenExpression

case class InExpression(expr: Either[StateFieldPathExpression, TypeDiscriminator], isNot: Boolean, inItems: List[InItem], inSubqury: Subquery, inCollection: CollectionValuedInputParameter)

trait InItem
case class InItem_Literal(item: Literal) extends InItem
case class InItem_SingleValuedInputParameter(item: SingleValuedInputParameter) extends InItem

case class LikeExpression(expr: StringExpression, isNot: Boolean, like: PatternValue, escape: Option[EscapeCharacter])

case class NullComparisonExpression(expr: Either[SingleValuedPathExpression, InputParameter], isNull: Boolean)

case class EmptyCollectionComparisonExpression(expr: CollectionValuedPathExpression, isEmpty: Boolean)

case class CollectionMemberExpression(expr: EntityOrValueExpression, isNot: Boolean, memberOf: CollectionValuedPathExpression)

trait EntityOrValueExpression
case class EntityOrValueExpression_SingleValuedObjectPathExpression(expr: SingleValuedObjectPathExpression) extends EntityOrValueExpression
case class EntityOrValueExpression_StateFieldPathExpression(expr: StateFieldPathExpression) extends EntityOrValueExpression
case class EntityOrValueExpressionP_SimpleEntityOrValueExpression(expr: SimpleEntityOrValueExpression) extends EntityOrValueExpression

trait SimpleEntityOrValueExpression
case class SimpleEntityOrValueExpression_IdentificationVariable(expr: IdentificationVariable) extends SimpleEntityOrValueExpression
case class SimpleEntityOrValueExpression_InputParameter(expr: InputParameter) extends SimpleEntityOrValueExpression
case class SimpleEntityOrValueExpression_Literal(expr: Literal) extends SimpleEntityOrValueExpression

case class ExistsExpression(isNot: Boolean, exists: Subquery)

trait AllOrAnyExpression
case class AllOrAnyExpression_All(subquery: Subquery) extends AllOrAnyExpression
case class AllOrAnyExpression_Any(subquery: Subquery) extends AllOrAnyExpression
case class AllOrAnyExpression_Some(subquery: Subquery) extends AllOrAnyExpression

trait ComparisonExpression
case class ComparisonExpression_StringExpression(op: ComparisonOperator, rightExpr: Either[StringExpression, AllOrAnyExpression])
case class ComparisonExpression_BooleanExpression(isEq: Boolean, rightExpr: Either[BooleanExpression, AllOrAnyExpression])
case class ComparisonExpression_EnumExpression(isEq: Boolean, rightExpr: Either[EnumExpression, AllOrAnyExpression])
case class ComparisonExpression_DatetimeExpression(op: ComparisonOperator, rightExpr: Either[DatetimeExpression, AllOrAnyExpression])
case class ComparisonExpression_EntityExpression(isEq: Boolean, rightExpr: Either[EntityExpression, AllOrAnyExpression])
case class ComparisonExpression_EntityTypeExpression(isEq: Boolean, rightExpr: EntityTypeExpression)
case class ComparisonExpression_ArithmeticExpression(op: ComparisonOperator, rightExpr: Either[AllOrAnyExpression, ArithmeticExpression])

trait ComparisonOperator
case object EQ extends ComparisonOperator
case object GE extends ComparisonOperator
case object GT extends ComparisonOperator
case object NE extends ComparisonOperator
case object LT extends ComparisonOperator
case object LE extends ComparisonOperator

/**
 * rightTerms: Left: Plus, Right: Minus
 */
case class ArithmeticExpression(term: ArithmeticTerm, rightTerms: List[Either[ArithmeticExpression, ArithmeticExpression]])

/**
 * rightTerms: Left: Times, Right: Div
 */
case class ArithmeticTerm(factor: ArithmeticFactor, rightTerms: List[Either[ArithmeticTerm, ArithmeticTerm]])

trait MathOperator
case object Plus extends MathOperator
case object Minus extends MathOperator
case object Times extends MathOperator
case object Div extends MathOperator

/**
 * Left: Plus, Right: Minus
 */
case class ArithmeticFactor(primary: ArithmeticPrimary, pre: Either[MathOperator, MathOperator])

trait ArithmeticPrimary
case class ArithmeticPrimary_StateFieldPathExpression(expr: StateFieldPathExpression) extends ArithmeticPrimary
case class ArithmeticPrimary_NumericLiteral(expr: NumericLiteral) extends ArithmeticPrimary
case class ArithmeticPrimary_ArithmeticExpression(expr: ArithmeticExpression) extends ArithmeticPrimary
case class ArithmeticPrimary_InputParameter(expr: InputParameter) extends ArithmeticPrimary
case class ArithmeticPrimary_FunctionsReturningNumerics(expr: FunctionsReturningNumerics) extends ArithmeticPrimary
case class ArithmeticPrimary_AggregateExpression(expr: AggregateExpression) extends ArithmeticPrimary
case class ArithmeticPrimary_CaseExpression(expr: CaseExpression) extends ArithmeticPrimary
case class ArithmeticPrimary_FunctionInvocation(expr: FunctionInvocation) extends ArithmeticPrimary
case class ArithmeticPrimary_Subquery(expr: Subquery) extends ArithmeticPrimary

trait StringExpression
case class StringExpression_StateFieldPathExpression(expr: StateFieldPathExpression) extends StringExpression
case class StringExpression_StringLiteral(expr: StringLiteral) extends StringExpression
case class StringExpression_InputParameter(expr: InputParameter) extends StringExpression
case class StringExpression_FunctionsReturningStrings(expr: FunctionsReturningStrings) extends StringExpression
case class StringExpression_AggregateExpression(expr: AggregateExpression) extends StringExpression
case class StringExpression_CaseExpression(expr: CaseExpression) extends StringExpression
case class StringExpression_FunctionInvocation(expr: FunctionInvocation) extends StringExpression
case class StringExpression_Subquery(expr: Subquery) extends StringExpression

trait DatetimeExpression
case class DatetimeExpression_StateFieldPathExpression(expr: StateFieldPathExpression) extends DatetimeExpression
case class DatetimeExpression_InputParameter(expr: InputParameter) extends DatetimeExpression
case class DatetimeExpression_FunctionsReturningDatetime(expr: FunctionsReturningDatetime) extends DatetimeExpression
case class DatetimeExpression_AggregateExpression(expr: AggregateExpression) extends DatetimeExpression
case class DatetimeExpression_CaseExpression(expr: CaseExpression) extends DatetimeExpression
case class DatetimeExpression_FunctionInvocation(expr: FunctionInvocation) extends DatetimeExpression
case class DatetimeExpression_DatetimeTimestampLiteral(expr: DatetimeTimestampLiteral) extends DatetimeExpression
case class DatetimeExpression_Subquery(expr: Subquery) extends DatetimeExpression

trait BooleanExpression
case class BooleanExpression_StateFieldPathExpression(expr: StateFieldPathExpression) extends BooleanExpression
case class BooleanExpression_BooleanLiteral(expr: BooleanLiteral) extends BooleanExpression
case class BooleanExpression_InputParameter(expr: InputParameter) extends BooleanExpression
case class BooleanExpression_CaseExpression(expr: CaseExpression) extends BooleanExpression
case class BooleanExpression_FunctionInvocation(expr: FunctionInvocation) extends BooleanExpression
case class BooleanExpression_Subquery(expr: Subquery) extends BooleanExpression

trait EnumExpression
case class EnumExpression_StateFieldPathExpression(expr: StateFieldPathExpression) extends EnumExpression
case class EnumExpression_EnumLiteral(expr: EnumLiteral) extends EnumExpression
case class EnumExpression_InputParameter(expr: InputParameter) extends EnumExpression
case class EnumExpression_CaseExpression(expr: CaseExpression) extends EnumExpression
case class EnumExpression_Subquery(expr: Subquery) extends EnumExpression

trait EntityExpression
case class EntityExpression_SingleValuedObjectPathExpression(expr: SingleValuedObjectPathExpression) extends EntityExpression
case class EntityExpression_SimpleEntityExpression(expr: SimpleEntityExpression) extends EntityExpression

trait SimpleEntityExpression
case class SimpleEntityExpression_IdentificationVariable(expr: IdentificationVariable) extends SimpleEntityExpression
case class SimpleEntityExpression_InputParameter(expr: InputParameter) extends SimpleEntityExpression

trait EntityTypeExpression
case class EntityTypeExpression_TypeDiscriminator(expr: TypeDiscriminator) extends EntityTypeExpression
case class EntityTypeExpression_InputParameter(expr: InputParameter) extends EntityTypeExpression
case class EntityTypeExpression_EntityTypeLiteral(expr: EntityTypeLiteral) extends EntityTypeExpression

trait TypeDiscriminator
case class TypeDiscriminator_IdentificationVariable(expr: IdentificationVariable) extends TypeDiscriminator
case class TypeDiscriminator_SingleValuedObjectPathExpression(expr: SingleValuedObjectPathExpression) extends TypeDiscriminator
case class TypeDiscriminator_InputParameter(expr: InputParameter) extends TypeDiscriminator

trait FunctionsReturningNumerics
case class FunctionsReturningNumerics_LENGTH(expr: StringExpression) extends FunctionsReturningNumerics
case class FunctionsReturningNumerics_LOCATE(expr: StringExpression, expr2: StringExpression, exprs: List[ArithmeticExpression]) extends FunctionsReturningNumerics
case class FunctionsReturningNumerics_ABS(expr: ArithmeticExpression) extends FunctionsReturningNumerics
case class FunctionsReturningNumerics_SQRT(expr: ArithmeticExpression) extends FunctionsReturningNumerics
case class FunctionsReturningNumerics_MOD(expr: ArithmeticExpression, expr2: ArithmeticExpression) extends FunctionsReturningNumerics
case class FunctionsReturningNumerics_SIZE(expr: CollectionValuedPathExpression) extends FunctionsReturningNumerics
case class FunctionsReturningNumerics_INDEX(expr: IdentificationVariable) extends FunctionsReturningNumerics

trait FunctionsReturningDatetime
case object CURRENT_DATE extends FunctionsReturningDatetime
case object CURRENT_TIME extends FunctionsReturningDatetime
case object CURRENT_TIMESTAMP extends FunctionsReturningDatetime

trait FunctionsReturningStrings
case class FunctionReturningStrings_CONCAT(expr1: StringExpression, expr2: StringExpression, exprs: List[StringExpression]) extends FunctionsReturningStrings
case class FunctionReturningStrings_SUBSTRING(expr1: StringExpression, expr2: ArithmeticExpression, exprs: List[ArithmeticExpression]) extends FunctionsReturningStrings
case class FunctionReturningStrings_TRIM(trimSepc: Option[TrimSpecification], trimChar: Option[TrimCharacter], from: StringExpression) extends FunctionsReturningStrings
case class FunctionReturningStrings_LOWER(str: StringExpression) extends FunctionsReturningStrings
case class FunctionReturningStrings_UPPER(str: StringExpression) extends FunctionsReturningStrings;

trait TrimSpecification
case object LEADING extends TrimSpecification
case object TRAILING extends TrimSpecification
case object BOTH extends TrimSpecification

case class FunctionInvocation(functionName: StringLiteral, args: List[FunctionArg])

trait FunctionArg
case class FunctionArg_Literal(arg: Literal) extends FunctionArg
case class FunctionArg_StateFieldPathExpression(arg: StateFieldPathExpression) extends FunctionArg
case class FunctionArg_InputParameter(arg: InputParameter) extends FunctionArg
case class FunctionArg_ScalarExpression(arg: ScalarExpression) extends FunctionArg

trait CaseExpression
case class CaseExpression_GeneralCaseExpression(expr: GeneralCaseExpression) extends CaseExpression
case class CaseExpression_SimpleCaseExpression(expr: SimpleCaseExpression) extends CaseExpression
case class CaseExpression_CoalesceExpression(expr: CoalesceExpression) extends CaseExpression
case class CaseExpression_NullifExpression(expr: NullifExpression) extends CaseExpression

case class GeneralCaseExpression(whens: List[WhenClause], elseExpr: ScalarExpression)

case class WhenClause(condExpr: ConditionalExpression, thenExpr: ScalarExpression)

case class SimpleCaseExpression(caseOperand: CaseOperand, whens: List[SimpleWhenClause], elseExpr: ScalarExpression)

trait CaseOperand
case class CaseOperand_StateFieldPathExpression(operand: StateFieldPathExpression) extends CaseOperand
case class CaseOperand_TypeDiscriminato(operand: TypeDiscriminator) extends CaseOperand

case class SimpleWhenClause(whenExpr: ScalarExpression, thenExpr: ScalarExpression)

case class CoalesceExpression(coalesceExpr: ScalarExpression, exprs: List[ScalarExpression])

case class NullifExpression(leftExpr: ScalarExpression, rightExpr: ScalarExpression)

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

trait InputParameter
case class InputParameter_Named(name: String) extends InputParameter
case class InputParameter_Position(i: Int) extends InputParameter

case class CollectionValuedInputParameter(param: InputParameter)

trait PatternValue
case class PatternValue_InputParameter(pattern: InputParameter) extends PatternValue
case class PatternValue_StringLiteral(pattern: String) extends PatternValue

case class EscapeCharacter(chars: String)
case class TrimCharacter(chars: String)

case class IdentificationVariable(name: Identifier)

case class ResultVariable(name: Identifier)

case class EntityName(name: Identifier)

case class SingleValuedObjectField(name: PathComponent)
case class CollectionValuedField(name: PathComponent)

case class StateField(paths: List[PathComponent])

case class SingleValuedEmbeddableObjectField(name: PathComponent)
case class ConstructorName(path: Identifier, paths: List[PathComponent])
case class Subtype(name: Identifier)
case class SuperqueryIdentificationVariable(name: Identifier)
case class SingleValuedInputParameter(name: Identifier)

case class Identifier(name: String)
case class PathComponent(name: String)
