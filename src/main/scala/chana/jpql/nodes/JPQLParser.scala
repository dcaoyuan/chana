package chana.jpql.nodes

import xtc.tree.Node

class JPQLParser(rootNode: Node) {

  private var indentLevel = 0
  protected var nodePath = List[Node]()
  protected var _errors = List[Node]()

  // root
  def visitNode(node: Node) {
    node.getName match {
      case "SelectStatement" => visit(node)(selectStatement)
      case "UpdateStatement" => visit(node)(updateStatement)
      case "DeleteStatement" => visit(node)(deleteStatement)
    }
  }

  protected def enter(node: Node): Unit = {
    indentLevel += 1
    nodePath ::= node // push
  }

  protected def exit(node: Node) {
    indentLevel -= 1
    nodePath = nodePath.tail // pop
  }

  // -------- general node visit method
  private def visit[T](node: Node)(body: Node => T): T = {
    enter(node)
    val res = body(node)
    exit(node)
    res
  }

  // =========================================================================

  /*-
  SelectClause FromClause WhereClause? GroupbyClause? HavingClause? OrderbyClause?
  */
  def selectStatement(node: Node) = {

  }

  /*-
  UpdateClause SetClause WhereClause?
  */
  def updateStatement(node: Node) = {

  }

  /*-
  UPDATE EntityName ( AS? Ident )? 
  */
  def updateClause(node: Node) = {

  }

  /*-
  SET SetAssignClause ( COMMA SetAssignClause )*
  */
  def setClause(node: Node) = {

  }

  /*-
  SetAssignTarget EQ NewValue
  */
  def setAssignClause(node: Node) = {

  }

  /*-
  PathExpr
/ Attribute
  */
  def setAssignTarget(node: Node) = {

  }

  /*-
  ScalarExpr
/ NULL
  */
  def newValue(node: Node) = {

  }

  /*-
  DeleteClause WhereClause?
  */
  def deleteStatement(node: Node) = {

  }

  /*-
  DELETE FROM EntityName ( AS? Ident )?
  */
  def deleteClause(node: Node) = {

  }

  /*-
  SELECT DISTINCT? SelectItem (COMMA SelectItem )* 
  */
  def selectClause(node: Node) = {

  }

  /*-
  SelectExpr ( AS? Ident )?
  */
  def selectItem(node: Node) = {

  }

  /*-
  AggregateExpr
/ ScalarExpr
/ OBJECT LParen VarAccessOrTypeConstant RParen
/ ConstructorExpr
/ MapEntryExpr
  */
  def selectExpr(node: Node) = {

  }

  /*-
  ENTRY LParen VarAccessOrTypeConstant RParen
  */
  def mapEntryExpr(node: Node) = {

  }

  /*-
  QualIdentVar ( DOT Attribute )*
  */
  def pathExprOrVarAccess(node: Node) = {

  }

  /*-
  VarAccessOrTypeConstant
/ KEY   LParen VarAccessOrTypeConstant RParen
/ VALUE LParen VarAccessOrTypeConstant RParen 
  */
  def qualIdentVar(node: Node) = {

  }

  /*-
  AVG   LParen DISTINCT? ScalarExpr RParen
/ MAX   LParen DISTINCT? ScalarExpr RParen
/ MIN   LParen DISTINCT? ScalarExpr RParen
/ SUM   LParen DISTINCT? ScalarExpr RParen
/ COUNT LParen DISTINCT? ScalarExpr RParen
  */
  def aggregateExpr(node: Node) = {

  }

  /*-
  NEW ConstructorName LParen ConstructorItem ( COMMA ConstructorItem )* RParen
  */
  def constructorExpr(node: Node) = {

  }

  /*-
  Ident ( DOT Ident )*
  */
  def constructorName(node: Node) = {

  }

  /*-
  ScalarExpr
/ AggregateExpr
  */
  def constructorItem(node: Node) = {

  }

  /*-
  FROM IdentVarDecl ( COMMA ( IdentVarDecl / CollectionMemberDecl) )*
  */
  def fromClause(node: Node) = {

  }

  /*-
  RangeVarDecl Join*
  */
  def identVarDecl(node: Node) = {

  }

  /*-
  EntityName AS? Ident
  */
  def rangeVarDecl(node: Node) = {

  }

  /*-
  Identifier
  */
  def entityName(node: Node) = {

  }

  /*-
  JoinSpec              JoinAssocPathExpr                   AS? Ident  JoinCond?
/ JoinSpec TREAT LParen JoinAssocPathExpr AS  Ident  RParen AS? Ident  JoinCond?
/ JoinSpec FETCH        JoinAssocPathExpr                       Ident? JoinCond?
  */
  def join(node: Node) = {

  }

  /*-
  JOIN
/ LEFT JOIN
/ LEFT OUTER JOIN
/ INNER JOIN
  */
  def joinSpec(node: Node) = {

  }

  /*-
  ON CondExpr
  */
  def joinCond(node: Node) = {

  }

  /*-
  IN LParen CollectionValuedPathExpr RParen AS? Ident
  */
  def collectionMemberDecl(node: Node) = {

  }

  /*-
  PathExpr
  */
  def collectionValuedPathExpr(node: Node) = {

  }

  /*-
  PathExpr
  */
  def assocPathExpr(node: Node) = {

  }

  /*-
  QualIdentVar ( DOT Attribute )*
  */
  def joinAssocPathExpr(node: Node) = {

  }

  /*-
  PathExpr
  */
  def singleValuedPathExpr(node: Node) = {

  }

  /*-
  PathExpr 
  */
  def stateFieldPathExpr(node: Node) = {

  }

  /*-
  QualIdentVar ( DOT Attribute )+
  */
  def pathExpr(node: Node) = {

  }

  /*-
  AttributeName
  */
  def attribute(node: Node) = {

  }

  /*-
  Ident
  */
  def varAccessOrTypeConstant(node: Node) = {

  }

  /*-
  WHERE CondExpr 
  */
  def whereClause(node: Node) = {

  }

  /*-
  CondTerm ( OR CondTerm )*
  */
  def condExpr(node: Node) = {

  }

  /*-
  CondFactor ( AND CondFactor )*
  */
  def condTerm(node: Node) = {

  }

  /*-
  NOT? ( CondPrimary / ExistsExpr )
  */
  def condFactor(node: Node) = {

  }

  /*-
  LParen CondExpr RParen
/ SimpleCondExpr
  */
  def condPrimary(node: Node) = {

  }

  /*-
  ArithExpr          SimpleCondExprRem
/ NonArithScalarExpr SimpleCondExprRem
  */
  def simpleCondExpr(node: Node) = {

  }

  /*-
  ComparisonExpr
/ NOT? CondWithNotExpr 
/ IS NOT? IsExpr
  */
  def simpleCondExprRem(node: Node) = {

  }

  /*-
  BetweenExpr
/ LikeExpr
/ InExpr
/ CollectionMemberExpr
  */
  def condWithNotExpr(node: Node) = {

  }

  /*-
  NullComparisonExpr
/ EmptyCollectionComparisonExpr
  */
  def isExpr(node: Node) = {

  }

  /*-
  BETWEEN ScalarOrSubSelectExpr AND ScalarOrSubSelectExpr
  */
  def betweenExpr(node: Node) = {

  }

  /*-
  IN InputParam
/ IN LParen ScalarOrSubSelectExpr ( COMMA ScalarOrSubSelectExpr )* RParen
/ IN LParen Subquery                                               RParen
  */
  def inExpr(node: Node) = {

  }

  /*-
  LIKE ScalarOrSubSelectExpr Escape?
  */
  def likeExpr(node: Node) = {

  }

  /*-
  ESCAPE ScalarExpr
  */
  def escape(node: Node) = {

  }

  /*-
  NULL 
  */
  def nullComparisonExpr(node: Node) = {

  }

  /*-
  EMPTY 
  */
  def emptyCollectionComparisonExpr(node: Node) = {

  }

  /*-
  MEMBER OF? CollectionValuedPathExpr
  */
  def collectionMemberExpr(node: Node) = {

  }

  /*-
  EXISTS LParen Subquery RParen 
  */
  def existsExpr(node: Node) = {

  }

  /*-
  EQ ComparisonExprRightOperand
/ NE ComparisonExprRightOperand
/ GT ComparisonExprRightOperand
/ GE ComparisonExprRightOperand
/ LT ComparisonExprRightOperand
/ LE ComparisonExprRightOperand
  */
  def comparisonExpr(node: Node) = {

  }

  /*-
  ArithExpr
/ NonArithScalarExpr
/ AnyOrAllExpr
  */
  def comparisonExprRightOperand(node: Node) = {

  }

  /*-
  SimpleArithExpr
/ LParen Subquery RParen
  */
  def arithExpr(node: Node) = {

  }

  /*-
  ArithTerm ( PLUS  ArithTerm @PlusArithTerm 
            / MINUS ArithTerm @MinusArithTerm 
            )* 
  */
  def simpleArithExpr(node: Node) = {

  }

  /*-
  ArithFactor ( MULTIPLY ArithFactor @MultiplyArithFactor
              / DIVIDE   ArithFactor @DivideArithFactor
              )* 
  */
  def arithTerm(node: Node) = {

  }

  /*-
  PLUS  ArithPrimary @PlusArithPrimary
/ MINUS ArithPrimary @MinusArithPrimay
/ ArithPrimary
  */
  def arithFactor(node: Node) = {

  }

  /*-
  PathExprOrVarAccess
/ InputParam
/ CaseExpr
/ FuncsReturningNumerics
/ LParen SimpleArithExpr RParen
/ LiteralNumeric 
  */
  def arithPrimary(node: Node) = {

  }

  /*-
  SimpleArithExpr
/ NonArithScalarExpr
  */
  def scalarExpr(node: Node) = {

  }

  /*-
  ArithExpr
/ NonArithScalarExpr
  */
  def scalarOrSubSelectExpr(node: Node) = {

  }

  /*-
  FuncsReturningDatetime
/ FuncsReturningStrings
/ LiteralString
/ LiteralBoolean
/ LiteralTemporal
/ EntityTypeExpr
  */
  def nonArithScalarExpr(node: Node) = {

  }

  /*-
  ALL  LParen Subquery RParen
/ ANY  LParen Subquery RParen
/ SOME LParen Subquery RParen
  */
  def anyOrAllExpr(node: Node) = {

  }

  /*-
  TypeDiscriminator
  */
  def entityTypeExpr(node: Node) = {

  }

  /*-
  TYPE LParen VarOrSingleValuedPath RParen
/ TYPE LParen InputParam            RParen
  */
  def typeDiscriminator(node: Node) = {

  }

  /*-
  SimpleCaseExpr
/ GeneralCaseExpr
/ CoalesceExpr
/ NullifExpr
  */
  def caseExpr(node: Node) = {

  }

  /*-
  CASE CaseOperand SimpleWhenClause SimpleWhenClause* ELSE ScalarExpr END
  */
  def simpleCaseExpr(node: Node) = {

  }

  /*-
  CASE WhenClause WhenClause* ELSE ScalarExpr END
  */
  def generalCaseExpr(node: Node) = {

  }

  /*-
  COALESCE LParen ScalarExpr ( COMMA ScalarExpr )+ RParen
  */
  def coalesceExpr(node: Node) = {

  }

  /*-
  NULLIF LParen ScalarExpr COMMA ScalarExpr RParen
  */
  def nullifExpr(node: Node) = {

  }

  /*-
  StateFieldPathExpr
/ TypeDiscriminator
  */
  def caseOperand(node: Node) = {

  }

  /*-
  WHEN CondExpr THEN ScalarExpr
  */
  def whenClause(node: Node) = {

  }

  /*-
  WHEN ScalarExpr THEN ScalarExpr
  */
  def simpleWhenClause(node: Node) = {

  }

  /*-
  SingleValuedPathExpr
/ VarAccessOrTypeConstant
  */
  def varOrSingleValuedPath(node: Node) = {

  }

  /*-
  LiteralString 
/ FuncsReturningStrings
/ InputParam
/ StateFieldPathExpr 
  */
  def stringPrimary(node: Node) = {

  }

  /*-
  LiteralNumeric
/ LiteralBoolean
/ LiteralString
  */
  def literal(node: Node) = {

  }

  /*-
  LiteralDate
/ LiteralTime
/ LiteralTimestamp
  */
  def literalTemporal(node: Node) = {

  }

  /*-
  ':' identifier    @NamedInputParam
/ '?' [1-9] [0-9]*  @PositionInputParam
  */
  def inputParam(node: Node) = {

  }

  /*-
  Abs
/ Length 
/ Mod
/ Locate
/ Size
/ Sqrt
/ Index
/ Func
  */
  def funcsReturningNumerics(node: Node) = {

  }

  /*-
  CURRENT_DATE
/ CURRENT_TIME
/ CURRENT_TIMESTAMP
  */
  def funcsReturningDatetime(node: Node) = {

  }

  /*-
  Concat
/ Substring
/ Trim
/ Upper
/ Lower
  */
  def funcsReturningStrings(node: Node) = {

  }

  /*-
  CONCAT LParen ScalarExpr (COMMA ScalarExpr)+ RParen
  */
  def concat(node: Node) = {

  }

  /*-
  SUBSTRING LParen ScalarExpr COMMA ScalarExpr ( COMMA ScalarExpr )? RParen
  */
  def substring(node: Node) = {

  }

  /*-
  TRIM LParen TrimSpec? TrimChar? FROM? StringPrimary RParen
  */
  def trim(node: Node) = {

  }

  /*-
  LEADING 
/ TRAILING
/ BOTH
  */
  def trimSpec(node: Node) = {

  }

  /*-
  LiteralSingleQuotedString
/ InputParam
  */
  def trimChar(node: Node) = {

  }

  /*-
  UPPER LParen ScalarExpr RParen
  */
  def upper(node: Node) = {

  }

  /*-
  LOWER LParen ScalarExpr RParen
  */
  def lower(node: Node) = {

  }

  /*-
  ABS LParen SimpleArithExpr RParen
  */
  def abs(node: Node) = {

  }

  /*-
  LENGTH LParen ScalarExpr RParen
  */
  def length(node: Node) = {

  }

  /*-
  LOCATE LParen ScalarExpr COMMA ScalarExpr ( COMMA ScalarExpr )? RParen
  */
  def locate(node: Node) = {

  }

  /*-
  SIZE LParen CollectionValuedPathExpr RParen
  */
  def size(node: Node) = {

  }

  /*-
  MOD LParen ScalarExpr COMMA ScalarExpr RParen
  */
  def mod(node: Node) = {

  }

  /*-
  SQRT LParen ScalarExpr RParen
  */
  def sqrt(node: Node) = {

  }

  /*-
  INDEX LParen VarAccessOrTypeConstant RParen
  */
  def index(node: Node) = {

  }

  /*-
  FUNCTION LParen LiteralSingleQuotedString ( COMMA NewValue )* RParen
  */
  def func(node: Node) = {

  }

  /*-
  SimpleSelectClause SubqueryFromClause WhereClause? GroupbyClause? HavingClause?
  */
  def subquery(node: Node) = {

  }

  /*-
  SELECT DISTINCT? SimpleSelectExpr
  */
  def simpleSelectClause(node: Node) = {

  }

  /*-
  SingleValuedPathExpr 
/ AggregateExpr
/ VarAccessOrTypeConstant
  */
  def simpleSelectExpr(node: Node) = {

  }

  /*-
  FROM SubselectIdentVarDecl ( COMMA ( SubselectIdentVarDecl 
                                     / CollectionMemberDecl
                                     ) 
                             )*
  */
  def subqueryFromClause(node: Node) = {

  }

  /*-
  IdentVarDecl
/ AssocPathExpr AS? Ident
/ CollectionMemberDecl
  */
  def subselectIdentVarDecl(node: Node) = {

  }

  /*-
  ORDER BY OrderbyItem ( COMMA OrderbyItem )*
  */
  def orderbyClause(node: Node) = {

  }

  /*-
  ( SimpleArithExpr / ScalarExpr ) ( ASC / DESC )?
  */
  def orderbyItem(node: Node) = {

  }

  /*-
  GROUP BY ScalarExpr ( COMMA ScalarExpr )* 
  */
  def groupbyClause(node: Node) = {

  }

  /*-
  HAVING CondExpr 
  */
  def havingClause(node: Node) = {

  }

  /*-
  Identifier
  */
  def ident(node: Node) = {

  }

}