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

  sealed trait ExprSingle extends Argument
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
   * prefix is "", or "union", "|". The union and | operators are equivalent.
   */
  case object Union extends Prefix("union")
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

  final case class ForwardStep(axis: ForwardAxis, nodeTest: NodeTest)

  trait Axis
  sealed abstract class ForwardAxis(val text: String) extends Axis { val textWithMark = text + "::" }
  case object Child extends ForwardAxis("child")
  case object Descendant extends ForwardAxis("decendant")
  case object Attribute extends ForwardAxis("attribute")
  case object Self extends ForwardAxis("self")
  case object DescendantOrSelf extends ForwardAxis("decendant-or-self")
  case object FollowingSibling extends ForwardAxis("following-sibling")
  case object Following extends ForwardAxis("following")
  case object Namespace extends ForwardAxis("namesapce")

  final case class ReverseStep(axis: ReverseAxis, nodeTest: NodeTest)

  sealed abstract class ReverseAxis(val text: String) extends Axis { val textWithMark = text + "::" }
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

  /**
   * generic Wildcard =
   *   ASTER
   * / NCName COLON ASTER
   * / ASTER COLON NCName
   * / BracedURILiteral ASTER
   * ;
   */
  sealed trait Wildcard extends NameTest
  final case class NameAster(name: String) extends Wildcard
  final case class AsterName(name: String) extends Wildcard
  final case class UriAster(uri: String) extends Wildcard

  final case class PostfixExpr(expr: PrimaryExpr, postfixes: List[PostFix])
  sealed trait PostFix

  final case class ArgumentList(args: List[Argument]) extends PostFix
  final case class PredicateList(predicates: List[Predicate])
  final case class Predicate(expr: Expr) extends PostFix
  final case class Lookup(keySpecifier: KeySpecifier) extends PostFix

  /**
   * @param key, String NCName, or int, or ParenthesizedExpr or Aster
   */
  final case class KeySpecifier(key: AnyRef)

  final case class ArrowPostfix(arrowFunction: ArrowFunctionSpecifier, args: ArgumentList) extends PostFix
  sealed trait ArrowFunctionSpecifier

  sealed trait PrimaryExpr
  case object ContextItemExpr extends PrimaryExpr { val text = "." }

  final case class Literal(x: Any) extends PrimaryExpr

  final case class VarRef(varName: VarName) extends ArrowFunctionSpecifier with PrimaryExpr
  final case class VarName(eqName: EQName)

  final case class ParenthesizedExpr(expr: Option[Expr]) extends ArrowFunctionSpecifier with PrimaryExpr

  final case class FunctionCall(name: EQName, args: ArgumentList) extends PrimaryExpr

  sealed trait Argument
  case object ArgumentPlaceholder extends Argument { val text = "?" }

  sealed trait FunctionItemExpr extends PrimaryExpr
  final case class NamedFunctionRef(name: EQName, index: Int) extends FunctionItemExpr
  final case class InlineFunctionExpr(params: Option[ParamList], asType: Option[SequenceType], functionBody: FunctionBody) extends FunctionItemExpr

  final case class MapConstructor(entrys: List[MapConstructorEntry]) extends PrimaryExpr
  final case class MapConstructorEntry(key: MapKeyExpr, value: MapValueExpr)
  final case class MapKeyExpr(expr: ExprSingle)
  final case class MapValueExpr(expr: ExprSingle)

  sealed trait ArrayConstructor extends PrimaryExpr
  final case class SquareArrayConstructor(exprs: List[ExprSingle]) extends ArrayConstructor
  final case class BraceArrayConstructor(expr: Option[Expr]) extends ArrayConstructor

  final case class UnaryLookup(key: KeySpecifier) extends PrimaryExpr

  final case class SingleType(name: SimpleTypeName, withQuestionMark: Boolean)

  final case class TypeDeclaration(asType: SequenceType)

  sealed trait SequenceType
  case object SequenceType_Empty extends SequenceType
  final case class SequenceType_ItemType(itemType: ItemType, occurrence: Option[OccurrenceIndicator]) extends SequenceType

  sealed abstract class OccurrenceIndicator(val text: String)
  case object OccurrenceQuestion extends OccurrenceIndicator("?")
  case object OccurrenceAster extends OccurrenceIndicator("*")
  case object OccurrencePlus extends OccurrenceIndicator("+")

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

  /*
   * node()
   */
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

  sealed trait EQName extends NameTest with ArrowFunctionSpecifier
  final case class URIQualifiedName(uri: String, name: String) extends EQName
  sealed trait QName extends EQName
  final case class PrefixedName(prefix: String, local: String) extends QName
  final case class UnprefixedName(local: String) extends QName

}

