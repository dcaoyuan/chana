package chana.xpath

package object nodes {

  abstract class Prefix(val text: String)
  case object Nop extends Prefix("")

  sealed trait CompOperator

  sealed trait XPath extends Serializable

  final case class ParamList(param: Param, params: List[Param])
  final case class Param(name: EQName, typeDecl: Option[TypeDeclaration])

  final case class FunctionBody(expr: EnclosedExpr)
  final case class EnclosedExpr(expr: Expr)

  final case class Expr(expr: ExprSingle, exprs: List[ExprSingle])

  sealed trait ExprSingle
  final case class ForExpr(forClause: SimpleForClause, returnExpr: ExprSingle) extends ExprSingle
  final case class SimpleForClause(binding: SimpleForBinding, bindings: List[SimpleForBinding])
  final case class SimpleForBinding(varName: VarName, inExpr: ExprSingle)
  final case class LetExpr(letClause: SimpleLetClause, returnExpr: ExprSingle) extends ExprSingle
  final case class SimpleLetClause(binding: SimpleLetBinding, bindings: List[SimpleLetBinding])
  final case class SimpleLetBinding(varName: VarName, boundTo: ExprSingle)
  final case class QuantifiedExpr(isEvery: Boolean, varExpr: VarInExprSingle, varExprs: List[VarInExprSingle], statisExpr: ExprSingle) extends ExprSingle
  final case class VarInExprSingle(varName: VarName, inExpr: ExprSingle)
  final case class IfExpr(ifExpr: Expr, thenExpr: ExprSingle, elseExpr: ExprSingle) extends ExprSingle
  final case class OrExpr(andExpr: AndExpr, andExprs: List[AndExpr]) extends ExprSingle

  final case class AndExpr(compExpr: ComparisonExpr, compExprs: List[ComparisonExpr])
  final case class ComparisonExpr(concExpr: StringConcatExpr, compExprPostfix: Option[ComparisonExprPostfix])
  final case class ComparisonExprPostfix(compOp: CompOperator, concExpr: StringConcatExpr)
  final case class StringConcatExpr(rangeExpr: RangeExpr, rangeExprs: List[RangeExpr])
  final case class RangeExpr(addExpr: AdditiveExpr, toExpr: Option[AdditiveExpr])
  final case class AdditiveExpr(multiExpr: MultiplicativeExpr, prefixedMultiExprs: List[MultiplicativeExpr])

  /**
   * prefix is "", or "+", "-"
   */
  case object Plus extends Prefix("+")
  case object Minus extends Prefix("-")
  final case class MultiplicativeExpr(prefix: Prefix, unionExpr: UnionExpr, prefixedUnionExprs: List[UnionExpr]) //  UnionExpr ( UnionExprMultiply / UnionExprDiv / UnionExprIdiv / UnionExprMod )*

  /**
   * prefix is "", or "*", "div", "idiv", "mod"
   */
  case object Aster extends Prefix("*") with Wildcard
  case object Div extends Prefix("div")
  case object IDiv extends Prefix("idiv")
  case object Mod extends Prefix("mod")
  final case class UnionExpr(prefix: Prefix, intersectExceptExpr: IntersectExceptExpr, prefixedIntersectExceptExprs: List[IntersectExceptExpr]) // ( IntersectExceptExprUnion / IntersectExceptExprList )*

  /**
   * prefix is "", or "union", "|"
   */
  case object Union extends Prefix("union")
  case object Pipe extends Prefix("|")
  final case class IntersectExceptExpr(prefix: Prefix, instanceOfExpr: InstanceofExpr, prefixedInstanceOfExprs: List[InstanceofExpr]) // ( InstanceofExprIntersect / InstanceofExprExcept )*

  /**
   * prefix is "", or "intersect", "except"
   */
  case object Intersect extends Prefix("intersect")
  case object Except extends Prefix("except")
  final case class InstanceofExpr(prefix: Prefix, treatExpr: TreatExpr, ofType: Option[SequenceType])

  final case class TreatExpr(castableExpr: CastableExpr, asType: Option[SequenceType])
  final case class CastableExpr(castExpr: CastExpr, asType: Option[SingleType])
  final case class CastExpr(unaryExpr: UnaryExpr, asType: Option[SingleType])

  /**
   * prefix is "", or "-", "+"
   */
  final case class UnaryExpr(prefix: Prefix, valueExpr: ValueExpr)
  final case class ValueExpr(simpleMapExpr: SimpleMapExpr)

  /**
   * "=", "!=", "<=", "<", ">=", ">"
   */
  final case class GeneralComp(op: String) extends CompOperator

  /**
   * "eq", "ne", "lt", "le", "gt", "ge"
   */
  final case class ValueComp(op: String) extends CompOperator

  /**
   * "is", "<<", ">>"
   */
  final case class NodeComp(op: String) extends CompOperator

  final case class SimpleMapExpr(pathExpr: PathExpr, exclamExprs: List[PathExpr]) //  ( void:"!" PathExpr )*

  /**
   * prefix is "" or "//", "/"
   *
   *   "//" RelativePathExpr
   * / "/"  RelativePathExpr?
   * / RelativePathExpr
   */
  case object Relative extends Prefix("//")
  case object Absolute extends Prefix("/")
  final case class PathExpr(prefix: Prefix, relativeExpr: Option[RelativePathExpr])
  final case class RelativePathExpr(stepExpr: StepExpr, prefixedStepExprs: List[StepExpr]) // ( StepExprAbsolute / StepExprRelative )*

  /**
   * prefix is "" or "/" or "//"
   */
  final case class StepExpr(prefix: Prefix, expr: Either[PostfixExpr, AxisStep])

  sealed trait AxisStep
  final case class ReverseAxisStep(step: ReverseStep, predicates: PredicateList) extends AxisStep
  final case class ForwardAxisStep(step: ForwardStep, predicates: PredicateList) extends AxisStep

  sealed trait ForwardStep
  final case class ForwardStep_Axis(axis: ForwardAxis, nodeTest: NodeTest) extends ForwardStep
  final case class AbbrevForwardStep(nodeTest: NodeTest, withAtMark: Boolean) extends ForwardStep

  sealed abstract class ForwardAxis(val text: String) { val textWithMark = text + "::" }
  case object Child extends ForwardAxis("child")
  case object Descendant extends ForwardAxis("decendant")
  case object Attribute extends ForwardAxis("attribute")
  case object Self extends ForwardAxis("self")
  case object DescendantOrSelf extends ForwardAxis("decendant-or-self")
  case object FollowingSibling extends ForwardAxis("following-sibling")
  case object Following extends ForwardAxis("following")
  case object Namespace extends ForwardAxis("namesapce")

  sealed trait ReverseStep
  final case class ReverseStep_Axis(axis: ReverseAxis, nodeTest: NodeTest) extends ReverseStep
  case object AbbrevReverseStep extends ReverseStep

  sealed abstract class ReverseAxis(val text: String) { val textWithMark = text + "::" }
  case object Parent extends ReverseAxis("parent")
  case object Ancestor extends ReverseAxis("ancestor")
  case object PrecedingSibling extends ReverseAxis("preceding-sibling")
  case object Preceding extends ReverseAxis("preceding")
  case object AncestorOrSelf extends ReverseAxis("ancestor-or-self")

  /**
   * generic NodeTest =
   *   NameTest
   * / KindTest
   * ;
   */
  sealed trait NodeTest

  sealed trait NameTest extends NodeTest
  final case class NameTest_Name(name: EQName) extends NameTest
  final case class NameTest_Wildcard(wildcard: Wildcard) extends NameTest

  /**
   * generic Wildcard =
   *   ASTER
   * / NCName COLON ASTER
   * / ASTER COLON NCName
   * / BracedURILiteral ASTER
   * ;
   */
  sealed trait Wildcard
  final case class NameAster(name: String) extends Wildcard
  final case class AsterName(name: String) extends Wildcard
  final case class UriAster(uri: String) extends Wildcard

  final case class PostfixExpr(expr: PrimaryExpr, postfixes: List[PostFix])
  sealed trait PostFix
  final case class Postfix_Predicate(predicate: Predicate) extends PostFix
  final case class Postfix_Arguments(args: ArgumentList) extends PostFix
  final case class Postfix_Lookup(lookup: Lookup) extends PostFix
  final case class Postfix_ArrowPostfix(arrowPostfix: ArrowPostfix) extends PostFix

  final case class ArgumentList(args: List[Argument])
  final case class PredicateList(predicates: List[Predicate])
  final case class Predicate(expr: Expr)
  final case class Lookup(keySpecifier: KeySpecifier)

  sealed trait KeySpecifier
  final case class KeySpecifier_NCName(ncName: String) extends KeySpecifier
  final case class KeySpecifier_IntegerLiteral(int: Int) extends KeySpecifier
  final case class KeySpecifier_ParenthesizedExpr(expr: ParenthesizedExpr) extends KeySpecifier
  case object KeySpecifier_ASTER extends KeySpecifier

  final case class ArrowPostfix(arrowFunction: ArrowFunctionSpecifier, args: ArgumentList)
  sealed trait ArrowFunctionSpecifier
  final case class ArrowFunctionSpecifier_EQName(eqName: EQName) extends ArrowFunctionSpecifier
  final case class ArrowFunctionSpecifier_VarRef(varRef: VarRef) extends ArrowFunctionSpecifier
  final case class ArrowFunctionSpecifier_ParenthesizedExpr(expr: ParenthesizedExpr) extends ArrowFunctionSpecifier

  sealed trait PrimaryExpr
  final case class PrimaryExpr_Literal(literal: Literal) extends PrimaryExpr
  final case class PrimaryExpr_VarRef(varRef: VarRef) extends PrimaryExpr
  final case class PrimaryExpr_ParenthesizedExpr(expr: ParenthesizedExpr) extends PrimaryExpr
  case object PrimaryExpr_ContextItemExpr extends PrimaryExpr { val text = "." }
  final case class PrimaryExpr_FunctionCall(functionCall: FunctionCall) extends PrimaryExpr
  final case class PrimaryExpr_FunctionItemExpr(functionItemExpr: FunctionItemExpr) extends PrimaryExpr
  final case class PrimaryExpr_MapConstructor(mapConstructor: MapConstructor) extends PrimaryExpr
  final case class PrimaryExpr_ArrayConstructor(arrayConstructor: ArrayConstructor) extends PrimaryExpr
  final case class PrimaryExpr_UnaryLookup(unaryLoolup: UnaryLookup) extends PrimaryExpr

  sealed trait Literal
  final case class NumericLiteral(x: Number) extends Literal
  final case class StringLiteral(x: String) extends Literal

  final case class VarRef(varName: VarName)
  final case class VarName(eqName: EQName)

  final case class ParenthesizedExpr(expr: Option[Expr])

  final case class FunctionCall(name: EQName, args: ArgumentList)

  sealed trait Argument
  final case class Argument_ExprSingle(expr: ExprSingle) extends Argument
  case object ArgumentPlaceholder extends Argument { val text = "?" }

  sealed trait FunctionItemExpr
  final case class NamedFunctionRef(name: EQName, index: Int) extends FunctionItemExpr
  final case class InlineFunctionExpr(params: Option[ParamList], asType: Option[SequenceType], functionBody: FunctionBody) extends FunctionItemExpr

  final case class MapConstructor(entrys: List[MapConstructorEntry])
  final case class MapConstructorEntry(key: MapKeyExpr, value: MapValueExpr)
  final case class MapKeyExpr(expr: ExprSingle)
  final case class MapValueExpr(expr: ExprSingle)

  sealed trait ArrayConstructor
  final case class SquareArrayConstructor(exprs: List[ExprSingle]) extends ArrayConstructor
  final case class BraceArrayConstructor(expr: Option[Expr]) extends ArrayConstructor

  final case class UnaryLookup(key: KeySpecifier)

  final case class SingleType(name: SimpleTypeName, withQuestionMark: Boolean)

  final case class TypeDeclaration(asType: SequenceType)

  sealed trait SequenceType
  case object SequenceType_Empty extends SequenceType
  final case class SequenceType_ItemType(itemType: ItemType, occurrence: Option[OccurrenceIndicator]) extends SequenceType

  sealed abstract class OccurrenceIndicator(val text: String)
  case object OccurrenceIndicator_QUEST extends OccurrenceIndicator("?")
  case object OccurrenceIndicator_ASTER extends OccurrenceIndicator("*")
  case object OccurrenceIndicator_PLUS extends OccurrenceIndicator("+")

  /**
   * generic ItemType =
   *   KindTest
   * / ITEM LParen RParen
   * / FunctionTest
   * / MapTest
   * / ArrayTest
   * / AtomicOrUnionType
   * / ParenthesizedItemType
   * ;
   */
  sealed trait ItemType
  case object ItemType_ITEM extends ItemType // item()
  final case class AtomicOrUnionType(eqName: EQName) extends ItemType

  /**
   * generic KindTest =
   *   DocumentTest
   * / ElementTest
   * / AttributeTest
   * / SchemaElementTest
   * / SchemaAttributeTest
   * / PITest
   * / CommentTest
   * / TextTest
   * / NamespaceNodeTest
   * / AnyKindTest
   * ;
   */
  sealed trait KindTest extends NodeTest with ItemType

  case object AnyKindTest extends KindTest
  /**
   * elemTest should be either ElementTest or SchemaElementTest
   */
  final case class DocumentTest(elemTest: Option[KindTest]) extends KindTest // document-node(...)
  case object TextTest extends KindTest // text()
  case object CommentTest extends KindTest // comment()
  case object NamespaceNodeTest extends KindTest // namespace-node()
  final case class PITest(name: Option[String]) extends KindTest // processing-instruction(...)

  sealed trait AttributeTest extends KindTest
  case object AttributeTest_Empty extends AttributeTest
  final case class AttributeTest_Name(name: AttribNameOrWildcard, typeName: Option[TypeName]) extends AttributeTest

  type AttribNameOrWildcard = Either[AttributeName, Aster.type]

  final case class SchemaAttributeTest(attrDecl: AttributeDeclaration) extends KindTest

  final case class AttributeDeclaration(name: AttributeName)

  sealed trait ElementTest extends KindTest
  case object ElementTest_Empty extends ElementTest
  final case class ElementTest_Name(name: ElementNameOrWildcard, typeName: Option[TypeName], withQuestionMark: Boolean) extends ElementTest

  type ElementNameOrWildcard = Either[ElementName, Aster.type]
  final case class SchemaElementTest(elemDecl: ElementDeclaration) extends KindTest
  final case class ElementDeclaration(name: ElementName)

  final case class AttributeName(name: EQName)
  final case class ElementName(name: EQName)
  final case class SimpleTypeName(name: TypeName)
  final case class TypeName(name: EQName)

  sealed trait FunctionTest extends ItemType
  case object AnyFunctionTest extends FunctionTest // function(*)
  final case class TypedFunctionTest(types: List[SequenceType], asType: SequenceType) extends FunctionTest

  sealed trait MapTest extends ItemType
  case object AnyMapTest extends MapTest
  final case class TypedMapTest(tpe: AtomicOrUnionType, asType: SequenceType) extends MapTest

  sealed trait ArrayTest extends ItemType
  case object AnyArrayTest extends ArrayTest
  final case class TypedArrayTest(tpe: SequenceType) extends ArrayTest

  final case class ParenthesizedItemType(itemType: ItemType) extends ItemType

  sealed trait EQName
  final case class URIQualifiedName(uri: String, name: String) extends EQName
  sealed trait QName extends EQName
  final case class PrefixedName(prefix: String, local: String) extends QName
  final case class UnprefixedName(local: String) extends QName

}

