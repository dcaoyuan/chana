package chana.xpath

import chana.xpath.nodes._

case class XPathRuntimeException(value: Any, message: String)
  extends RuntimeException(
    value + " " + message + ". " + value + "'s type is: " + (value match {
      case null      => null
      case x: AnyRef => x.getClass.getName
      case _         => "primary type."
    }))

class XPathEvaluator {

  def simpleEval(xpath: Expr, ctx: Any) = {
    expr(xpath.expr, xpath.exprs, ctx)
  }

  // -------------------------------------------------------------------------

  def paramList(param0: Param, params: List[Param], ctx: Any): List[Any] = {
    param0 :: params map { x => param(x.name, x.typeDecl, ctx) }
  }

  def param(name: EQName, typeDecl: Option[TypeDeclaration], ctx: Any) = {
    // name
    typeDecl map { x => typeDeclaration(x.asType, ctx) }
  }

  def functionBody(expr: EnclosedExpr, ctx: Any) = {
    enclosedExpr(expr.expr, ctx)
  }

  def enclosedExpr(_expr: Expr, ctx: Any): Any = {
    expr(_expr.expr, _expr.exprs, ctx)
  }

  def expr(expr: ExprSingle, exprs: List[ExprSingle], ctx: Any) = {
    expr :: exprs map { exprSingle(_, ctx) }
  }

  def exprSingle(expr: ExprSingle, ctx: Any) = {
    expr match {
      case ForExpr(forClause, returnExpr)                         => forExpr(forClause, returnExpr, ctx)
      case LetExpr(letClause, returnExpr)                         => letExpr(letClause, returnExpr, ctx)
      case QuantifiedExpr(isEvery, varExpr, varExprs, statisExpr) => quantifiedExpr(isEvery, varExpr, varExprs, statisExpr, ctx)
      case IfExpr(_ifExpr, thenExpr, elseExpr)                    => ifExpr(_ifExpr, thenExpr, elseExpr, ctx)
      case OrExpr(andExpr, andExprs)                              => orExpr(andExpr, andExprs, ctx)
    }
  }

  def forExpr(forClause: SimpleForClause, returnExpr: ExprSingle, ctx: Any): Any = {
    simpleForClause(forClause.binding, forClause.bindings, ctx)
    exprSingle(returnExpr, ctx)
  }

  def simpleForClause(binding: SimpleForBinding, bindings: List[SimpleForBinding], ctx: Any) = {
    binding :: bindings map { x => simpleForBinding(x.varName, x.inExpr, ctx) }
  }

  def simpleForBinding(_varName: VarName, inExpr: ExprSingle, ctx: Any) = {
    varName(_varName.eqName, ctx)
    exprSingle(inExpr, ctx)
  }

  def letExpr(letClause: SimpleLetClause, returnExpr: ExprSingle, ctx: Any): Any = {
    simpleLetClause(letClause.binding, letClause.bindings, ctx)
    exprSingle(returnExpr, ctx)
  }

  def simpleLetClause(binding: SimpleLetBinding, bindings: List[SimpleLetBinding], ctx: Any) = {
    binding :: bindings map { x => simpleLetBinding(x.varName, x.boundTo, ctx) }
  }

  def simpleLetBinding(_varName: VarName, boundTo: ExprSingle, ctx: Any) = {
    varName(_varName.eqName, ctx)
    exprSingle(boundTo, ctx)
  }

  def quantifiedExpr(isEvery: Boolean, varExpr: VarInExprSingle, varExprs: List[VarInExprSingle], statisExpr: ExprSingle, ctx: Any): Any = {
    varExpr :: varExprs map { x => varInExprSingle(x.varName, x.inExpr, ctx) }
    exprSingle(statisExpr, ctx)
  }

  def varInExprSingle(_varName: VarName, inExpr: ExprSingle, ctx: Any) = {
    varName(_varName.eqName, ctx)
    exprSingle(inExpr, ctx)
  }

  def ifExpr(ifExpr: Expr, thenExpr: ExprSingle, elseExpr: ExprSingle, ctx: Any): Any = {
    expr(ifExpr.expr, ifExpr.exprs, ctx)
    exprSingle(thenExpr, ctx)
    exprSingle(elseExpr, ctx)
  }

  def orExpr(_andExpr: AndExpr, andExprs: List[AndExpr], ctx: Any) = {
    val andExpr0 = andExpr(_andExpr.compExpr, _andExpr.compExprs, ctx)
    andExprs map { x => andExpr(x.compExpr, x.compExprs, ctx) }
  }

  def andExpr(compExpr: ComparisonExpr, compExprs: List[ComparisonExpr], ctx: Any) = {
    comparisonExpr(compExpr.concExpr, compExpr.compExprPostfix, ctx)
    compExprs map { x => comparisonExpr(x.concExpr, x.compExprPostfix, ctx) }
  }

  def comparisonExpr(concExpr: StringConcatExpr, compExprPostfix: Option[ComparisonExprPostfix], ctx: Any) = {
    stringConcatExpr(concExpr.rangeExpr, concExpr.rangeExprs, ctx)
    compExprPostfix map { x => comparisonExprPostfix(x.compOp, x.concExpr, ctx) }
  }

  /**
   * generalcomp: "=", "!=", "<=", "<", ">=", ">"
   * valuecomp: "eq", "ne", "lt", "le", "gt", "ge"
   * nodecomp: "is", "<<", ">>"
   */
  def comparisonExprPostfix(compOp: CompOperator, concExpr: StringConcatExpr, ctx: Any) = {
    compOp match {
      case GeneralComp(op) =>
      case ValueComp(op)   =>
      case NodeComp(op)    =>
    }
    stringConcatExpr(concExpr.rangeExpr, concExpr.rangeExprs, ctx)
  }

  def stringConcatExpr(_rangeExpr: RangeExpr, rangeExprs: List[RangeExpr], ctx: Any) = {
    _rangeExpr :: rangeExprs map { x => rangeExpr(x.addExpr, x.toExpr, ctx) }
  }

  def rangeExpr(addExpr: AdditiveExpr, toExpr: Option[AdditiveExpr], ctx: Any) = {
    additiveExpr(addExpr.multiExpr, addExpr.prefixedMultiExprs, ctx)
    toExpr map { x => additiveExpr(x.multiExpr, x.prefixedMultiExprs, ctx) }
  }

  def additiveExpr(multiExpr: MultiplicativeExpr, prefixedMultiExprs: List[MultiplicativeExpr], ctx: Any) = {
    multiplicativeExpr(multiExpr.prefix, multiExpr.unionExpr, multiExpr.prefixedUnionExprs, ctx)
    prefixedMultiExprs map { x => multiplicativeExpr(x.prefix, x.unionExpr, x.prefixedUnionExprs, ctx) }
  }

  /**
   * prefix is "", or "+", "-"
   */
  def multiplicativeExpr(prefix: Prefix, _unionExpr: UnionExpr, prefixedUnionExprs: List[UnionExpr], ctx: Any) = {
    prefix match {
      case Nop   =>
      case Plus  =>
      case Minus =>
    }
    unionExpr(_unionExpr.prefix, _unionExpr.intersectExceptExpr, _unionExpr.prefixedIntersectExceptExprs, ctx)
    prefixedUnionExprs map { x => unionExpr(x.prefix, x.intersectExceptExpr, x.prefixedIntersectExceptExprs, ctx) }
  }

  /**
   * prefix is "", or "*", "div", "idiv", "mod"
   */
  def unionExpr(prefix: Prefix, _intersectExceptExpr: IntersectExceptExpr, prefixedIntersectExceptExprs: List[IntersectExceptExpr], ctx: Any) = {
    prefix match {
      case Nop   =>
      case Aster =>
      case Div   =>
      case IDiv  =>
      case Mod   =>
    }
    intersectExceptExpr(_intersectExceptExpr.prefix, _intersectExceptExpr.instanceOfExpr, _intersectExceptExpr.prefixedInstanceOfExprs, ctx)
    prefixedIntersectExceptExprs map { x => intersectExceptExpr(x.prefix, x.instanceOfExpr, x.prefixedInstanceOfExprs, ctx) }
  }

  /**
   * prefix is "", or "union", "|"
   */
  def intersectExceptExpr(prefix: Prefix, _instanceOfExpr: InstanceofExpr, prefixedInstanceOfExprs: List[InstanceofExpr], ctx: Any) = {
    prefix match {
      case Nop   =>
      case Union =>
      case Pipe  =>
    }
    instanceofExpr(_instanceOfExpr.prefix, _instanceOfExpr.treatExpr, _instanceOfExpr.ofType, ctx)
    prefixedInstanceOfExprs map { x => instanceofExpr(x.prefix, x.treatExpr, x.ofType, ctx) }
  }

  /**
   * prefix is "", or "intersect", "except"
   */
  def instanceofExpr(prefix: Prefix, _treatExpr: TreatExpr, ofType: Option[SequenceType], ctx: Any) = {
    prefix match {
      case Nop       =>
      case Intersect =>
      case Except    =>
    }
    treatExpr(_treatExpr.castableExpr, _treatExpr.asType, ctx)
    ofType map { x => sequenceType(x, ctx) }
  }

  def treatExpr(_castableExpr: CastableExpr, asType: Option[SequenceType], ctx: Any) = {
    castableExpr(_castableExpr.castExpr, _castableExpr.asType, ctx)
    asType map { x => sequenceType(x, ctx) }
  }

  def castableExpr(_castExpr: CastExpr, asType: Option[SingleType], ctx: Any) = {
    castExpr(_castExpr.unaryExpr, _castExpr.asType, ctx)
    asType map { x => singleType(x.name, x.withQuestionMark, ctx) }
  }

  def castExpr(_unaryExpr: UnaryExpr, asType: Option[SingleType], ctx: Any) = {
    unaryExpr(_unaryExpr.prefix, _unaryExpr.valueExpr, ctx)
    asType map { x => singleType(x.name, x.withQuestionMark, ctx) }
  }

  /**
   * prefix is "", or "-", "+"
   */
  def unaryExpr(prefix: Prefix, _valueExpr: ValueExpr, ctx: Any) = {
    prefix match {
      case Nop   =>
      case Plus  =>
      case Minus =>
    }
    valueExpr(_valueExpr.simpleMapExpr, ctx)
  }

  def valueExpr(_simpleMapExpr: SimpleMapExpr, ctx: Any) = {
    simpleMapExpr(_simpleMapExpr.pathExpr, _simpleMapExpr.exclamExprs, ctx)
  }

  def simpleMapExpr(_pathExpr: PathExpr, exclamExprs: List[PathExpr], ctx: Any) = {
    pathExpr(_pathExpr.prefix, _pathExpr.relativeExpr, ctx)
    exclamExprs map { x => pathExpr(x.prefix, x.relativeExpr, ctx) }
  }

  /**
   * prefix is "" or "//", "/"
   *
   *   "//" RelativePathExpr
   * / "/"  RelativePathExpr?
   * / RelativePathExpr
   */
  def pathExpr(prefix: Prefix, relativeExpr: Option[RelativePathExpr], ctx: Any) = {
    prefix match {
      case Nop      =>
      case Absolute =>
      case Relative =>
    }
    relativeExpr map { x => relativePathExpr(x.stepExpr, x.prefixedStepExprs, ctx) }
  }

  def relativePathExpr(_stepExpr: StepExpr, prefixedStepExprs: List[StepExpr], ctx: Any) = {
    stepExpr(_stepExpr.prefix, _stepExpr.expr, ctx)
    prefixedStepExprs map { x => stepExpr(x.prefix, x.expr, ctx) }
  }

  /**
   * prefix is "" or "/" or "//"
   */
  def stepExpr(prefix: Prefix, expr: Either[PostfixExpr, AxisStep], ctx: Any) = {
    prefix match {
      case Nop      =>
      case Absolute =>
      case Relative =>
    }
    expr match {
      case Left(x)                   => postfixExpr(x.expr, x.postfixes, ctx)
      case Right(x: ReverseAxisStep) => reverseAxisStep(x.step, x.predicates, ctx)
      case Right(x: ForwardAxisStep) => forwardAxisStep(x.step, x.predicates, ctx)
    }
  }

  def reverseAxisStep(step: ReverseStep, predicates: PredicateList, ctx: Any) = {
    step match {
      case ReverseStep_Axis(axis, _nodeTest) =>
        axis match {
          case Parent           =>
          case Ancestor         =>
          case PrecedingSibling =>
          case Preceding        =>
          case AncestorOrSelf   =>
        }
        nodeTest(_nodeTest, ctx)

      case AbbrevReverseStep =>
    }
    predicateList(predicates.predicates, ctx)
  }

  def forwardAxisStep(step: ForwardStep, predicates: PredicateList, ctx: Any) = {
    step match {
      case ForwardStep_Axis(axis, _nodeTest) =>
        axis match {
          case Child            =>
          case Descendant       =>
          case Attribute        =>
          case Self             =>
          case DescendantOrSelf =>
          case FollowingSibling =>
          case Following        =>
          case Namespace        =>
        }
        nodeTest(_nodeTest, ctx)
      case AbbrevForwardStep(_nodeTest, withAtMark) =>
        nodeTest(_nodeTest, ctx)
    }
  }

  def nodeTest(test: NodeTest, ctx: Any) = {
    test match {
      case x: NameTest => nameTest(x, ctx)
      case x: KindTest => kindTest(x, ctx)
    }
  }

  def nameTest(test: NameTest, ctx: Any) = {
    test match {
      case NameTest_Name(name) =>
      case NameTest_Wildcard(wildcard) =>
        wildcard match {
          case NameAster(name) =>
          case AsterName(name) =>
          case UriAster(uri)   =>
          case Aster           =>
        }
    }
  }

  def kindTest(test: KindTest, ctx: Any) = {
    test match {
      case AnyKindTest =>
      case DocumentTest(elemTest) => documentTest(elemTest, ctx)
      case TextTest =>
      case CommentTest =>
      case NamespaceNodeTest =>
      case PITest(name) => piTest(name, ctx)
      case AttributeTest_Empty =>
      case AttributeTest_Name(name, typeName) => attributeTest_Name(name, typeName, ctx)
      case SchemaAttributeTest(attrDecl) => schemaAttributeTest(attrDecl, ctx)
      case ElementTest_Empty =>
      case ElementTest_Name(name, typeName, withQuestionMark) => elementTest_Name(name, typeName, withQuestionMark, ctx)
      case SchemaElementTest(elemDecl) => schemaElementTest(elemDecl, ctx)
    }
  }

  def postfixExpr(expr: PrimaryExpr, postfixes: List[PostFix], ctx: Any) = {
    primaryExpr(expr, ctx)
    postfixes map { x => postfix(x, ctx) }
  }

  def postfix(post: PostFix, ctx: Any) = {
    post match {
      case Postfix_Predicate(predicate)       => postfix_Predicate(predicate, ctx)
      case Postfix_Arguments(args)            => postfix_Arguments(args, ctx)
      case Postfix_Lookup(lookup)             => postfix_Lookup(lookup, ctx)
      case Postfix_ArrowPostfix(arrowPostfix) => postfix_ArrowPostfix(arrowPostfix, ctx)
    }
  }

  def postfix_Predicate(_predicate: Predicate, ctx: Any) = {
    predicate(_predicate.expr, ctx)
  }

  def postfix_Arguments(args: ArgumentList, ctx: Any) = {
    argumentList(args.args, ctx)
  }

  def postfix_Lookup(_lookup: Lookup, ctx: Any) = {
    lookup(_lookup.keySpecifier, ctx)
  }

  def postfix_ArrowPostfix(_arrowPostfix: ArrowPostfix, ctx: Any) = {
    arrowPostfix(_arrowPostfix.arrowFunction, _arrowPostfix.args, ctx)
  }

  def argumentList(args: List[Argument], ctx: Any): Any = {
    args map {
      case Argument_ExprSingle(expr) => exprSingle(expr, ctx)
      case ArgumentPlaceholder       =>
    }
  }

  def predicateList(predicates: List[Predicate], ctx: Any) = {
    predicates map { x => predicate(x.expr, ctx) }
  }

  def predicate(_expr: Expr, ctx: Any): Any = {
    expr(_expr.expr, _expr.exprs, ctx)
  }

  def lookup(_keySpecifier: KeySpecifier, ctx: Any) = {
    keySpecifier(_keySpecifier, ctx)
  }

  def keySpecifier(specifier: KeySpecifier, ctx: Any) = {
    specifier match {
      case KeySpecifier_NCName(ncName)          =>
      case KeySpecifier_IntegerLiteral(x)       => x
      case KeySpecifier_ParenthesizedExpr(expr) =>
      case KeySpecifier_ASTER                   =>
    }
  }

  def arrowPostfix(arrowFunction: ArrowFunctionSpecifier, args: ArgumentList, ctx: Any) = {
    arrowFunction match {
      case ArrowFunctionSpecifier_EQName(name)            =>
      case ArrowFunctionSpecifier_VarRef(varRef)          =>
      case ArrowFunctionSpecifier_ParenthesizedExpr(expr) => parenthesizedExpr(expr.expr, ctx)
    }
  }

  def primaryExpr(expr: PrimaryExpr, ctx: Any) = {
    expr match {
      case PrimaryExpr_Literal(_literal) =>
        literal(_literal, ctx)

      case PrimaryExpr_VarRef(ref) =>
        varRef(ref.varName, ctx)

      case PrimaryExpr_ParenthesizedExpr(expr) =>
        parenthesizedExpr(expr.expr, ctx)

      case PrimaryExpr_FunctionCall(call) =>
        functionCall(call.name, call.args, ctx)

      case PrimaryExpr_FunctionItemExpr(item) =>
        item match {
          case NamedFunctionRef(name, index)                    => namedFunctionRef(name, index, ctx)
          case InlineFunctionExpr(params, asType, functionBody) => inlineFunctionExpr(params, asType, functionBody, ctx)
        }

      case PrimaryExpr_MapConstructor(constructor) =>
        mapConstructor(constructor.entrys, ctx)

      case PrimaryExpr_ArrayConstructor(constructor) =>
        arrayConstructor(constructor, ctx)

      case PrimaryExpr_UnaryLookup(lookup) =>
        unaryLookup(lookup.key, ctx)
    }
  }

  def literal(v: Literal, ctx: Any): Any = {
    v match {
      case NumericLiteral(x) => x
      case StringLiteral(x)  => x
    }
  }

  def varRef(_varName: VarName, ctx: Any) = {
    varName(_varName.eqName, ctx)
  }

  def varName(eqName: EQName, ctx: Any): EQName = {
    eqName
  }

  def parenthesizedExpr(_expr: Option[Expr], ctx: Any): Any = {
    _expr map { x => expr(x.expr, x.exprs, ctx) }
  }

  def functionCall(name: EQName, args: ArgumentList, ctx: Any) = {
    //name
    argumentList(args.args, ctx)
  }

  def namedFunctionRef(name: EQName, index: Int, ctx: Any) = {
    // TODO
  }

  def inlineFunctionExpr(params: Option[ParamList], asType: Option[SequenceType], _functionBody: FunctionBody, ctx: Any) = {
    params match {
      case None    => Nil
      case Some(x) => paramList(x.param, x.params, ctx)
    }
    asType map { sequenceType(_, ctx) }
    functionBody(_functionBody.expr, ctx)
  }

  def mapConstructor(entrys: List[MapConstructorEntry], ctx: Any) = {
    entrys map { x => mapConstructorEntry(x.key, x.value, ctx) }
  }

  def mapConstructorEntry(key: MapKeyExpr, value: MapValueExpr, ctx: Any): Any = {
    exprSingle(key.expr, ctx)
    exprSingle(value.expr, ctx)
  }

  def arrayConstructor(constructor: ArrayConstructor, ctx: Any): Any = {
    constructor match {
      case SquareArrayConstructor(exprs) =>
        exprs map { x => exprSingle(x, ctx) }
      case BraceArrayConstructor(_expr) =>
        _expr match {
          case Some(x) => expr(x.expr, x.exprs, ctx)
          case None    =>
        }
    }
  }

  def unaryLookup(key: KeySpecifier, ctx: Any) = {
    keySpecifier(key, ctx)
  }

  def singleType(name: SimpleTypeName, withQuestionMark: Boolean, ctx: Any) = {
    simpleTypeName(name.name, ctx)
  }

  def typeDeclaration(asType: SequenceType, ctx: Any) = {
    sequenceType(asType, ctx)
  }

  def sequenceType(tpe: SequenceType, ctx: Any) = {
    tpe match {
      case SequenceType_Empty =>
      case SequenceType_ItemType(_itemType, occurrence) =>
        itemType(_itemType, ctx)
        occurrence match {
          case None                            =>
          case Some(OccurrenceIndicator_QUEST) =>
          case Some(OccurrenceIndicator_ASTER) =>
          case Some(OccurrenceIndicator_PLUS)  =>
        }
    }
  }

  def itemType(tpe: ItemType, ctx: Any): Any = {
    tpe match {
      case ItemType_ITEM                    =>
      case AtomicOrUnionType(eqName)        => eqName
      case x: KindTest                      => kindTest(x, ctx)
      case x: FunctionTest                  => functionTest(x, ctx)
      case x: MapTest                       => mapTest(x, ctx)
      case x: ArrayTest                     => arrayTest(x, ctx)
      case ParenthesizedItemType(_itemType) => itemType(_itemType, ctx)
    }
  }

  /**
   * elemTest should be either ElementTest or SchemaElementTest
   */
  def documentTest(elemTest: Option[KindTest], ctx: Any): Any = {
    elemTest map { x => kindTest(x, ctx) }
  }

  def piTest(name: Option[String], ctx: Any) = {
    name match {
      case Some(x) =>
      case None    =>
    }
  }

  def attributeTest_Name(name: AttribNameOrWildcard, typeName: Option[TypeName], ctx: Any) = {
    name match {
      case Left(x)  => attributeName(x.name, ctx)
      case Right(x) => // '*'
    }
  }

  def schemaAttributeTest(attrDecl: AttributeDeclaration, ctx: Any) = {
    attributeDeclaration(attrDecl.name, ctx)
  }

  def attributeDeclaration(name: AttributeName, ctx: Any) = {
    attributeName(name.name, ctx)
  }

  def elementTest_Name(name: ElementNameOrWildcard, typeName: Option[TypeName], withQuestionMark: Boolean, ctx: Any) = {
    name match {
      case Left(x)  => elementName(x.name, ctx)
      case Right(x) => // '*'
    }
  }

  def schemaElementTest(elemDecl: ElementDeclaration, ctx: Any) = {
    elementDeclaration(elemDecl.name, ctx)
  }

  def elementDeclaration(name: ElementName, ctx: Any) = {
    elementName(name.name, ctx)
  }

  def attributeName(name: EQName, ctx: Any): EQName = {
    name
  }

  def elementName(name: EQName, ctx: Any): EQName = {
    name
  }

  def simpleTypeName(name: TypeName, ctx: Any): EQName = {
    typeName(name.name, ctx)
  }

  def typeName(name: EQName, ctx: Any): EQName = {
    name
  }

  def functionTest(test: FunctionTest, ctx: Any) = {
    test match {
      case AnyFunctionTest                  =>
      case TypedFunctionTest(types, asType) => typedFunctionTest(types, asType, ctx)
    }
  }

  def typedFunctionTest(types: List[SequenceType], asType: SequenceType, ctx: Any): Any = {
    types map { sequenceType(_, ctx) }
    sequenceType(asType, ctx)
  }

  def mapTest(test: MapTest, ctx: Any) = {
    test match {
      case AnyMapTest                =>
      case TypedMapTest(tpe, asType) => typedMapTest(tpe, asType, ctx)
    }
  }

  def typedMapTest(tpe: AtomicOrUnionType, asType: SequenceType, ctx: Any) = {
    //tpe.eqName
    sequenceType(asType, ctx)
  }

  def arrayTest(test: ArrayTest, ctx: Any) = {
    test match {
      case AnyArrayTest        =>
      case TypedArrayTest(tpe) => typedArrayTest(tpe, ctx)
    }
  }

  def typedArrayTest(tpe: SequenceType, ctx: Any) = {
    sequenceType(tpe, ctx)
  }

}
