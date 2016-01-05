package chana.jpql

/*
 * Definition of JPQL AST nodes.
 * 
 * @author Caoyuan Deng
 */
package object nodes {

  sealed trait Statement extends Serializable {
    def where: Option[WhereClause]

    def collectSpecifiedIds(): List[(String, String)] = {
      where match {
        case Some(WhereClause(CondExpr(term, orTerms))) =>
          (term :: orTerms) map collectSpecifiedIds flatten
        case None => List()
      }
    }

    private def collectSpecifiedIds(term: CondTerm): List[(String, String)] = {
      (term.factor :: term.andFactors) map collectSpecifiedIds flatten
    }

    private def collectSpecifiedIds(factor: CondFactor): Option[(String, String)] = {
      val not = factor.not
      factor.expr match {
        case Left(SimpleCondExpr(Left(ArithExpr(Left(
          SimpleArithExpr(ArithTerm(ArithFactor(ArithPrimary_Plus(
            QualIdentVarWithAttrs(
              QualIdentVar(VarAccessOrTypeConstant(Ident(alias))), List(Attribute(attr))))), /*rightFactors*/ List()), /*rightTerms*/ List())))),
          ComparisonExpr(EQ, LiteralString(id)))) if attr.toLowerCase == JPQLEvaluator.ID =>
          if (not) None else Some((alias, id))
        case _ => None
      }
    }
  }

  final case class SelectStatement(
      select: SelectClause,
      from: FromClause,
      where: Option[WhereClause],
      groupby: Option[GroupbyClause],
      having: Option[HavingClause],
      orderby: Option[OrderbyClause]) extends Statement {

    def isSelectItemsAllAggregate() = {
      (select.item :: select.items) forall {
        case SelectItem(_: AggregateExpr, _) => true
        case _                               => false
      }
    }
  }

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
    values: ValuesClause,
    where: Option[WhereClause]) extends Statement

  final case class UpdateClause(entityName: EntityName, as: Option[Ident], joins: List[Join])
  final case class SetClause(assign: SetAssignClause, assigns: List[SetAssignClause])
  final case class SetAssignClause(target: SetAssignTarget, value: NewValue)
  final case class SetAssignTarget(path: Either[PathExpr, Attribute])

  final case class NewValue(v: ScalarExpr) // could be null

  final case class DeleteClause(from: EntityName, as: Option[Ident], joins: List[Join])

  final case class SelectClause(isDistinct: Boolean, item: SelectItem, items: List[SelectItem])

  final case class SelectItem(expr: SelectExpr, as: Option[Ident])

  final case class InsertClause(entityName: EntityName, as: Option[Ident], joins: List[Join])
  final case class AttributesClause(attr: Attribute, attrs: List[Attribute])
  final case class ValuesClause(row: RowValuesClause, rows: List[RowValuesClause])
  final case class RowValuesClause(value: NewValue, values: List[NewValue])

  sealed trait SelectExpr

  final case class MapEntryExpr(entry: VarAccessOrTypeConstant) extends SelectExpr

  sealed trait PathExprOrVarAccess extends ArithPrimary
  final case class QualIdentVarWithAttrs(qual: QualIdentVar, attributes: List[Attribute]) extends PathExprOrVarAccess
  final case class FuncsReturingAnyWithAttrs(expr: FuncsReturningAny, attributes: List[Attribute]) extends PathExprOrVarAccess

  final case class QualIdentVar(v: VarAccessOrTypeConstant)

  sealed trait AggregateExpr extends SelectExpr with ConstructorItem with SimpleSelectExpr
  final case class Avg(isDistinct: Boolean, expr: ScalarExpr) extends AggregateExpr
  final case class Max(isDistinct: Boolean, expr: ScalarExpr) extends AggregateExpr
  final case class Min(isDistinct: Boolean, expr: ScalarExpr) extends AggregateExpr
  final case class Sum(isDistinct: Boolean, expr: ScalarExpr) extends AggregateExpr
  final case class Count(isDistinct: Boolean, expr: ScalarExpr) extends AggregateExpr

  final case class ConstructorExpr(name: ConstructorName, arg: ConstructorItem, args: List[ConstructorItem]) extends SelectExpr

  final case class ConstructorName(id: Ident, ids: List[Ident])

  sealed trait ConstructorItem

  final case class FromClause(from: IdentVarDecl, froms: List[Either[IdentVarDecl, CollectionMemberDecl]])

  final case class IdentVarDecl(range: RangeVarDecl, joins: List[Join]) extends SubselectIdentVarDecl

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

  final case class CollectionMemberDecl(in: CollectionValuedPathExpr, as: Ident) extends SubselectIdentVarDecl

  final case class CollectionValuedPathExpr(path: PathExpr)

  final case class AssocPathExpr(path: PathExpr)

  final case class JoinAssocPathExpr(qualId: QualIdentVar, attrbutes: List[Attribute])

  final case class SingleValuedPathExpr(path: PathExpr) extends SimpleSelectExpr

  final case class StateFieldPathExpr(path: PathExpr) extends StringPrimary

  final case class PathExpr(qual: QualIdentVar, attributes: List[Attribute])

  final case class Attribute(name: String)

  // could be SelectExpr_OBJECT when treated as SelectExpr
  final case class VarAccessOrTypeConstant(id: Ident) extends SelectExpr with SimpleSelectExpr

  final case class WhereClause(expr: CondExpr)

  final case class CondExpr(term: CondTerm, orTerms: List[CondTerm]) extends CondPrimary

  final case class CondTerm(factor: CondFactor, andFactors: List[CondFactor])

  final case class CondFactor(not: Boolean, expr: Either[CondPrimary, ExistsExpr])

  sealed trait CondPrimary

  final case class SimpleCondExpr(expr: Either[ArithExpr, NonArithScalarExpr], rem: SimpleCondExprRem) extends CondPrimary

  sealed trait SimpleCondExprRem
  final case class CondWithNotExprWithNot(not: Boolean, expr: CondWithNotExpr) extends SimpleCondExprRem
  final case class IsExprWithNot(not: Boolean, expr: IsExpr) extends SimpleCondExprRem

  sealed trait CondWithNotExpr

  sealed trait IsExpr
  case object IsNull extends IsExpr // NULL 
  case object IsEmpty extends IsExpr // EMPTY

  final case class BetweenExpr(min: ScalarOrSubselectExpr, max: ScalarOrSubselectExpr) extends CondWithNotExpr

  sealed trait InExpr extends CondWithNotExpr
  final case class InExpr_InputParam(expr: InputParam) extends InExpr
  final case class InExpr_ScalarOrSubselectExpr(expr: ScalarOrSubselectExpr, exprs: List[ScalarOrSubselectExpr]) extends InExpr
  final case class InExpr_Subquery(expr: Subquery) extends InExpr

  final case class LikeExpr(like: ScalarOrSubselectExpr, escape: Option[Escape]) extends CondWithNotExpr

  final case class Escape(expr: ScalarExpr)

  final case class CollectionMemberExpr(of: CollectionValuedPathExpr) extends CondWithNotExpr

  final case class ExistsExpr(subquery: Subquery)

  final case class ComparisonExpr(op: ComparisonOp, operand: ComparsionExprRightOperand) extends SimpleCondExprRem

  sealed trait ComparisonOp
  case object EQ extends ComparisonOp
  case object NE extends ComparisonOp
  case object GT extends ComparisonOp
  case object GE extends ComparisonOp
  case object LT extends ComparisonOp
  case object LE extends ComparisonOp

  sealed trait ComparsionExprRightOperand

  final case class ArithExpr(expr: Either[SimpleArithExpr, Subquery]) extends ComparsionExprRightOperand with ScalarOrSubselectExpr

  final case class SimpleArithExpr(term: ArithTerm, rightTerms: List[PlusOrMinusTerm]) extends ArithPrimary with ScalarExpr

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

  sealed trait ScalarExpr extends SelectExpr with ConstructorItem

  sealed trait ScalarOrSubselectExpr

  sealed trait NonArithScalarExpr extends ComparsionExprRightOperand with ScalarOrSubselectExpr with ScalarExpr

  final case class AnyOrAllExpr(anyOrAll: AnyOrAll, subquery: Subquery) extends ComparsionExprRightOperand

  sealed trait AnyOrAll
  case object ALL extends AnyOrAll
  case object ANY extends AnyOrAll
  case object SOME extends AnyOrAll

  final case class EntityTypeExpr(typeDis: TypeDiscriminator) extends NonArithScalarExpr

  final case class TypeDiscriminator(expr: Either[VarOrSingleValuedPath, InputParam])

  sealed trait CaseExpr extends ArithPrimary

  final case class SimpleCaseExpr(caseOperand: CaseOperand, when: SimpleWhenClause, whens: List[SimpleWhenClause], elseExpr: ScalarExpr) extends CaseExpr

  final case class GeneralCaseExpr(when: WhenClause, whens: List[WhenClause], elseExpr: ScalarExpr) extends CaseExpr

  final case class CoalesceExpr(expr: ScalarExpr, exprs: List[ScalarExpr]) extends CaseExpr

  final case class NullifExpr(leftExpr: ScalarExpr, rightExpr: ScalarExpr) extends CaseExpr

  final case class CaseOperand(expr: Either[StateFieldPathExpr, TypeDiscriminator])

  final case class WhenClause(when: CondExpr, thenExpr: ScalarExpr)

  final case class SimpleWhenClause(when: ScalarExpr, thenExpr: ScalarExpr)

  final case class VarOrSingleValuedPath(expr: Either[SingleValuedPathExpr, VarAccessOrTypeConstant])

  sealed trait StringPrimary

  sealed trait Literal
  final case class LiteralBoolean(v: Boolean) extends Literal with NonArithScalarExpr
  final case class LiteralString(v: String) extends Literal with NonArithScalarExpr with StringPrimary

  sealed trait LiteralNumeric extends Literal with ArithPrimary
  final case class LiteralInteger(v: Int) extends LiteralNumeric
  final case class LiteralLong(v: Long) extends LiteralNumeric
  final case class LiteralFloat(v: Float) extends LiteralNumeric
  final case class LiteralDouble(v: Double) extends LiteralNumeric

  sealed trait LiteralTemporal extends NonArithScalarExpr
  final case class LiteralDate(date: java.time.LocalDate) extends LiteralTemporal
  final case class LiteralTime(time: java.time.LocalTime) extends LiteralTemporal
  final case class LiteralTimestamp(time: java.time.LocalDateTime) extends LiteralTemporal

  sealed trait InputParam extends ArithPrimary with StringPrimary
  final case class InputParam_Named(name: String) extends InputParam
  final case class InputParam_Position(pos: Int) extends InputParam

  sealed trait FuncsReturningNumeric extends ArithPrimary
  final case class Abs(expr: SimpleArithExpr) extends FuncsReturningNumeric
  final case class Length(expr: ScalarExpr) extends FuncsReturningNumeric
  final case class Mod(expr: ScalarExpr, divisorExpr: ScalarExpr) extends FuncsReturningNumeric
  final case class Locate(expr: ScalarExpr, searchExpr: ScalarExpr, startExpr: Option[ScalarExpr]) extends FuncsReturningNumeric
  final case class Size(expr: CollectionValuedPathExpr) extends FuncsReturningNumeric
  final case class Sqrt(expr: ScalarExpr) extends FuncsReturningNumeric
  final case class Index(expr: VarAccessOrTypeConstant) extends FuncsReturningNumeric
  final case class Func(name: String, args: List[NewValue]) extends FuncsReturningNumeric

  sealed trait FuncsReturningDatetime extends NonArithScalarExpr
  case object CURRENT_DATE extends FuncsReturningDatetime
  case object CURRENT_TIME extends FuncsReturningDatetime
  case object CURRENT_TIMESTAMP extends FuncsReturningDatetime

  sealed trait FuncsReturningString extends NonArithScalarExpr with StringPrimary
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

  final case class SubqueryFromClause(from: SubselectIdentVarDecl, froms: List[Either[SubselectIdentVarDecl, CollectionMemberDecl]])

  sealed trait SubselectIdentVarDecl
  final case class AssocPathExprWithAs(expr: AssocPathExpr, as: Ident) extends SubselectIdentVarDecl

  final case class OrderbyClause(orderby: OrderbyItem, orderbys: List[OrderbyItem])

  final case class OrderbyItem(expr: Either[SimpleArithExpr, ScalarExpr], isAsc: Boolean)

  final case class GroupbyClause(expr: ScalarExpr, exprs: List[ScalarExpr])

  final case class HavingClause(condExpr: CondExpr)

  final case class Ident(ident: String)

}