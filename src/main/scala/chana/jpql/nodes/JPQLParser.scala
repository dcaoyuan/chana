package chana.jpql.nodes

import xtc.tree.Node

class JPQLParser(rootNode: Node) {

  private var indentLevel = 0
  protected var nodePath = List[Node]()
  protected var _errors = List[Node]()

  // root
  def visitRoot() = {
    JPQL(rootNode)
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

  private def visitOpt[T](node: Node)(body: Node => T): Option[T] = {
    if (node eq null) None
    else Some(visit(node)(body))
  }

  private def visitList[T](nodes: xtc.util.Pair[Node])(body: Node => T): List[T] = {
    if (nodes eq null) Nil
    else {
      var rs = List[T]()
      val xs = nodes.iterator
      while (xs.hasNext) {
        rs ::= visit(xs.next)(body)
      }
      rs.reverse
    }
  }

  // =========================================================================

  def JPQL(node: Node) = {
    val n = node.getNode(0)
    n.getName match {
      case "SelectStatement" => visit(n)(selectStatement)
      case "UpdateStatement" => visit(n)(updateStatement)
      case "DeleteStatement" => visit(n)(deleteStatement)
    }
  }

  /*-
     SelectClause FromClause WhereClause? GroupbyClause? HavingClause? OrderbyClause?
   */
  def selectStatement(node: Node) = {
    val select = visit(node.getNode(0))(selectClause)
    val from = visit(node.getNode(1))(fromClause)
    val where = visitOpt(node.getNode(2))(whereClause)
    val groupby = visitOpt(node.getNode(3))(groupbyClause)
    val having = visitOpt(node.getNode(4))(havingClause)
    val orderby = visitOpt(node.getNode(5))(orderbyClause)
    SelectStatement(select, from, where, groupby, having, orderby)
  }

  /*-
     UpdateClause SetClause WhereClause?
   */
  def updateStatement(node: Node) = {
    val update = visit(node.getNode(0))(updateClause)
    val set = visit(node.getNode(1))(setClause)
    val where = visitOpt(node.getNode(2))(whereClause)
    UpdateStatement(update, set, where)
  }

  /*-
     UPDATE EntityName ( AS? Ident )? 
   */
  def updateClause(node: Node) = {
    val entity = visit(node.getNode(0))(entityName)
    val as = visitOpt(node.getNode(1))(ident)
    UpdateClause(entity, as)
  }

  /*-
     SET SetAssignClause ( COMMA SetAssignClause )*
   */
  def setClause(node: Node) = {
    val assign = visit(node.getNode(0))(setAssignClause)
    val assigns = visitList(node.getList(1))(setAssignClause)
    SetClause(assign, assigns)
  }

  /*-
     SetAssignTarget EQ NewValue
   */
  def setAssignClause(node: Node) = {
    val target = visit(node.getNode(0))(setAssignTarget)
    val newVal = visit(node.getNode(1))(newValue)
    SetAssignClause(target, newVal)
  }

  /*-
     PathExpr
   / Attribute
   */
  def setAssignTarget(node: Node) = {
    val n = node.getNode(0)
    val target = n.getName match {
      case "PathExpr"  => Left(visit(n)(pathExpr))
      case "Attribute" => Right(visit(n)(attribute))
    }
    SetAssignTarget(target)
  }

  /*-
     ScalarExpr
   / NULL
   */
  def newValue(node: Node) = {
    val v = if (node.isEmpty) null
    else {
      visit(node.getNode(0))(scalarExpr)
    }
    NewValue(v)
  }

  /*-
     DeleteClause WhereClause?
   */
  def deleteStatement(node: Node) = {
    val delete = visit(node.getNode(0))(deleteClause)
    val where = visitOpt(node.getNode(1))(whereClause)
    DeleteStatement(delete, where)
  }

  /*-
     DELETE FROM EntityName ( AS? Ident )?
   */
  def deleteClause(node: Node) = {
    val name = visit(node.getNode(0))(entityName)
    val as = visitOpt(node.getNode(1))(ident)
    DeleteClause(name, as)
  }

  /*-
     SELECT DISTINCT? SelectItem (COMMA SelectItem )* 
   */
  def selectClause(node: Node) = {
    val isDistinct = node.get(0) ne null
    val item = visit(node.getNode(1))(selectItem)
    val items = visitList(node.getList(2))(selectItem)
    SelectClause(isDistinct, item, items)
  }

  /*-
     SelectExpr ( AS? Ident )?
   */
  def selectItem(node: Node) = {
    val item = visit(node.getNode(0))(selectExpr)
    val as = visitOpt(node.getNode(1))(ident)
    SelectItem(item, as)
  }

  /*-
     AggregateExpr
   / ScalarExpr
   / OBJECT LParen VarAccessOrTypeConstant RParen
   / ConstructorExpr
   / MapEntryExpr
   */
  def selectExpr(node: Node): SelectExpr = {
    val n = node.getNode(0)
    n.getName match {
      case "AggregateExpr"           => SelectExpr_AggregateExpr(visit(n)(aggregateExpr))
      case "ScalarExpr"              => SelectExpr_ScalarExpr(visit(n)(scalarExpr))
      case "VarAccessOrTypeConstant" => SelectExpr_OBJECT(visit(n)(varAccessOrTypeConstant))
      case "ConstructorExpr"         => SelectExpr_ConstructorExpr(visit(n)(constructorExpr))
      case "MapEntryExpr"            => SelectExpr_MapEntryExpr(visit(n)(mapEntryExpr))
    }
  }

  /*-
     ENTRY LParen VarAccessOrTypeConstant RParen
   */
  def mapEntryExpr(node: Node) = {
    val entry = visit(node.getNode(0))(varAccessOrTypeConstant)
    MapEntryExpr(entry)
  }

  /*-
     QualIdentVar ( DOT Attribute )*
   */
  def pathExprOrVarAccess(node: Node) = {
    val qual = visit(node.getNode(0))(qualIdentVar)
    val attributes = visitList(node.getList(1))(attribute)
    PathExprOrVarAccess(qual, attributes)
  }

  /*-
     VarAccessOrTypeConstant
   / KEY   LParen VarAccessOrTypeConstant RParen
   / VALUE LParen VarAccessOrTypeConstant RParen 
   */
  def qualIdentVar(node: Node) = {
    node.size match {
      case 1 =>
        QualIdentVar_VarAccessOrTypeConstant(visit(node.getNode(0))(varAccessOrTypeConstant))
      case 2 =>
        node.getString(0) match {
          case "key"   => QualIdentVar_KEY(visit(node.getNode(1))(varAccessOrTypeConstant))
          case "value" => QualIdentVar_VALUE(visit(node.getNode(1))(varAccessOrTypeConstant))
        }
    }
  }

  /*-
     AVG   LParen DISTINCT? ScalarExpr RParen
   / MAX   LParen DISTINCT? ScalarExpr RParen
   / MIN   LParen DISTINCT? ScalarExpr RParen
   / SUM   LParen DISTINCT? ScalarExpr RParen
   / COUNT LParen DISTINCT? ScalarExpr RParen
   */
  def aggregateExpr(node: Node) = {
    val isDistinct = node.get(1) eq null
    val expr = visit(node.getNode(2))(scalarExpr)
    node.getString(0) match {
      case "avg"   => AggregateExpr_AVG(isDistinct, expr)
      case "max"   => AggregateExpr_MAX(isDistinct, expr)
      case "min"   => AggregateExpr_MIN(isDistinct, expr)
      case "sum"   => AggregateExpr_SUM(isDistinct, expr)
      case "count" => AggregateExpr_COUNT(isDistinct, expr)
    }
  }

  /*-
     NEW ConstructorName LParen ConstructorItem ( COMMA ConstructorItem )* RParen
   */
  def constructorExpr(node: Node) = {
    val name = visit(node.getNode(0))(constructorName)
    val arg = visit(node.getNode(1))(constructorItem)
    val args = visitList(node.getList(2))(constructorItem)
    ConstructorExpr(name, arg, args)
  }

  /*-
     Ident ( DOT Ident )*
   */
  def constructorName(node: Node) = {
    val field = visit(node.getNode(0))(ident)
    val paths = visitList(node.getList(1))(ident)
    ConstructorName(field, paths)
  }

  /*-
     ScalarExpr
   / AggregateExpr
   */
  def constructorItem(node: Node) = {
    val n = node.getNode(0)
    n.getName match {
      case "ScalarExpr"    => ConstructorItem_ScalarExpr(visit(n)(scalarExpr))
      case "AggregateExpr" => ConstructorItem_AggregateExpr(visit(n)(aggregateExpr))
    }
  }

  /*-
     FROM IdentVarDecl ( COMMA ( IdentVarDecl / CollectionMemberDecl) )*
   */
  def fromClause(node: Node) = {
    val from = visit(node.getNode(0))(identVarDecl)
    val froms = visitList(node.getList(1)) { n =>
      n.getName match {
        case "IdentVarDecl"         => Left(visit(n)(identVarDecl))
        case "CollectionMemberDecl" => Right(visit(n)(collectionMemberDecl))
      }
    }
    FromClause(from, froms)
  }

  /*-
     RangeVarDecl Join*
   */
  def identVarDecl(node: Node) = {
    val varDecl = visit(node.getNode(0))(rangeVarDecl)
    val joins = visitList(node.getList(1))(join)
    IdentVarDecl(varDecl, joins)
  }

  /*-
     EntityName AS? Ident
   */
  def rangeVarDecl(node: Node) = {
    val entity = visit(node.getNode(0))(entityName)
    val as = visit(node.getNode(1))(ident)
    RangeVarDecl(entity, as)
  }

  /*-
     Identifier
   */
  def entityName(node: Node) = {
    val name = node.getString(0)
    EntityName(name)
  }

  /*-
     JoinSpec              JoinAssocPathExpr                  AS? Ident  JoinCond?
   / JoinSpec TREAT LParen JoinAssocPathExpr AS Ident  RParen AS? Ident  JoinCond?
   / JoinSpec FETCH        JoinAssocPathExpr                      Ident? JoinCond?
   */
  def join(node: Node) = {
    val spec = visit(node.getNode(0))(joinSpec)
    node.size match {
      case 4 =>
        val expr = visit(node.getNode(1))(joinAssocPathExpr)
        val as = visit(node.getNode(2))(ident)
        val cond = visitOpt(node.getNode(3))(joinCond)
        Join_General(spec, expr, as, cond)

      case 6 =>
        val expr = visit(node.getNode(2))(joinAssocPathExpr)
        val exprAs = visit(node.getNode(3))(ident)
        val as = visit(node.getNode(4))(ident)
        val cond = visitOpt(node.getNode(5))(joinCond)
        Join_TREAT(spec, expr, exprAs, as, cond)

      case 5 =>
        val expr = visit(node.getNode(2))(joinAssocPathExpr)
        val as = visitOpt(node.getNode(3))(ident)
        val cond = visitOpt(node.getNode(4))(joinCond)
        Join_FETCH(spec, expr, as, cond)
    }
  }

  /*-
     JOIN
   / LEFT JOIN
   / LEFT OUTER JOIN
   / INNER JOIN
   */
  def joinSpec(node: Node) = {
    node.size match {
      case 1 => JOIN
      case 2 =>
        node.getString(0) match {
          case "left"  => LEFT_JOIN
          case "inner" => INNER_JOIN
        }
      case 3 => LEFT_OUTER_JOIN
    }
  }

  /*-
     ON CondExpr
   */
  def joinCond(node: Node) = {
    val expr = visit(node.getNode(0))(condExpr)
    JoinCond(expr)
  }

  /*-
     IN LParen CollectionValuedPathExpr RParen AS? Ident
   */
  def collectionMemberDecl(node: Node) = {
    val expr = visit(node.getNode(0))(collectionValuedPathExpr)
    val as = visit(node.getNode(1))(ident)
    CollectionMemberDecl(expr, as)
  }

  /*-
     PathExpr
   */
  def collectionValuedPathExpr(node: Node) = {
    val expr = visit(node.getNode(0))(pathExpr)
    CollectionValuedPathExpr(expr)
  }

  /*-
     PathExpr
   */
  def assocPathExpr(node: Node) = {
    val expr = visit(node.getNode(0))(pathExpr)
    AssocPathExpr(expr)
  }

  /*-
     QualIdentVar ( DOT Attribute )*
   */
  def joinAssocPathExpr(node: Node) = {
    val qual = visit(node.getNode(0))(qualIdentVar)
    val attributes = visitList(node.getList(1))(attribute)
    JoinAssocPathExpr(qual, attributes)
  }

  /*-
     PathExpr
   */
  def singleValuedPathExpr(node: Node) = {
    val expr = visit(node.getNode(0))(pathExpr)
    SingleValuedPathExpr(expr)
  }

  /*-
     PathExpr 
   */
  def stateFieldPathExpr(node: Node) = {
    val expr = visit(node.getNode(0))(pathExpr)
    StateFieldPathExpr(expr)
  }

  /*-
     QualIdentVar ( DOT Attribute )+
   */
  def pathExpr(node: Node) = {
    val qual = visit(node.getNode(0))(qualIdentVar)
    val attributes = visitList(node.getList(1))(attribute)
    PathExpr(qual, attributes)
  }

  /*-
     AttributeName
   */
  def attribute(node: Node) = {
    val name = node.getString(0)
    Attribute(name)
  }

  /*-
     Ident
   */
  def varAccessOrTypeConstant(node: Node) = {
    val id = visit(node.getNode(0))(ident)
    VarAccessOrTypeConstant(id)
  }

  /*-
     WHERE CondExpr 
   */
  def whereClause(node: Node) = {
    val expr = visit(node.getNode(0))(condExpr)
    WhereClause(expr)
  }

  /*-
     CondTerm ( OR CondTerm )*
   */
  def condExpr(node: Node): CondExpr = {
    val term = visit(node.getNode(0))(condTerm)
    val andTerms = visitList(node.getList(1))(condTerm)
    CondExpr(term, andTerms)
  }

  /*-
     CondFactor ( AND CondFactor )*
   */
  def condTerm(node: Node) = {
    val factor = visit(node.getNode(0))(condFactor)
    val andFactors = visitList(node.getList(1))(condFactor)
    CondTerm(factor, andFactors)
  }

  /*-
     NOT? ( CondPrimary / ExistsExpr )
   */
  def condFactor(node: Node) = {
    val not = node.get(0) ne null
    val n = node.getNode(1)
    val expr = n.getName match {
      case "CondPrimary" => Left(visit(n)(condPrimary))
      case "ExistsExpr"  => Right(visit(n)(existsExpr))
    }
    CondFactor(not, expr)
  }

  /*-
     LParen CondExpr RParen
   / SimpleCondExpr
   */
  def condPrimary(node: Node) = {
    val n = node.getNode(0)
    n.getName match {
      case "CondExpr"       => CondPrimary_CondExpr(visit(n)(condExpr))
      case "SimpleCondExpr" => CondPrimary_SimpleCondExpr(visit(n)(simpleCondExpr))
    }
  }

  /*-
     ArithExpr          SimpleCondExprRem
   / NonArithScalarExpr SimpleCondExprRem
   */
  def simpleCondExpr(node: Node) = {
    val n = node.getNode(0)
    val expr = n.getName match {
      case "ArithExpr"          => Left(visit(n)(arithExpr))
      case "NonArithScalarExpr" => Right(visit(n)(nonArithScalarExpr))
    }
    val rem = visit(node.getNode(1))(simpleCondExprRem)
    SimpleCondExpr(expr, rem)
  }

  /*-
     ComparisonExpr
   / NOT? CondWithNotExpr 
   / IS NOT? IsExpr
   */
  def simpleCondExprRem(node: Node) = {
    node.get(0) match {
      case n: Node => SimpleCondExprRem_ComparisonExpr(visit(n)(comparisonExpr))
      case null =>
        val n2 = node.getNode(1)
        n2.getName match {
          case "CondWithNotExpr" => SimpleCondExprRem_CondWithNotExpr(false, visit(n2)(condWithNotExpr))
          case "IsExpr"          => SimpleCondExprRem_IsExpr(false, visit(n2)(isExpr))
        }
      case "not" =>
        val n2 = node.getNode(1)
        n2.getName match {
          case "CondWithNotExpr" => SimpleCondExprRem_CondWithNotExpr(true, visit(n2)(condWithNotExpr))
          case "IsExpr"          => SimpleCondExprRem_IsExpr(true, visit(n2)(isExpr))
        }
    }
  }

  /*-
     BetweenExpr
   / LikeExpr
   / InExpr
   / CollectionMemberExpr
   */
  def condWithNotExpr(node: Node) = {
    val n = node.getNode(0)
    n.getName match {
      case "BetweenExpr"          => CondWithNotExpr_BetweenExpr(visit(n)(betweenExpr))
      case "LikeExpr"             => CondWithNotExpr_LikeExpr(visit(n)(likeExpr))
      case "InExpr"               => CondWithNotExpr_InExpr(visit(n)(inExpr))
      case "CollectionMemberExpr" => CondWithNotExpr_CollectionMemberExpr(visit(n)(collectionMemberExpr))
    }
  }

  /*-
     NullComparisonExpr
   / EmptyCollectionComparisonExpr
   */
  def isExpr(node: Node) = {
    node.getNode(0).getName match {
      case "NullComparisonExpr"            => IsNullExpr
      case "EmptyCollectionComparisonExpr" => IsEmptyExpr
    }
  }

  /*-
     BETWEEN ScalarOrSubselectExpr AND ScalarOrSubselectExpr
   */
  def betweenExpr(node: Node) = {
    val minExpr = visit(node.getNode(0))(scalarOrSubselectExpr)
    val maxExpr = visit(node.getNode(1))(scalarOrSubselectExpr)
    BetweenExpr(minExpr, maxExpr)
  }

  /*-
     IN InputParam
   / IN LParen ScalarOrSubselectExpr ( COMMA ScalarOrSubselectExpr )* RParen
   / IN LParen Subquery                                               RParen
   */
  def inExpr(node: Node) = {
    val n = node.getNode(0)
    n.getName match {
      case "InputParam"            => InExpr_InputParam(visit(n)(inputParam))
      case "ScalarOrSubselectExpr" => InExpr_ScalarOrSubselectExpr(visit(n)(scalarOrSubselectExpr), visitList(node.getList(1))(scalarOrSubselectExpr))
      case "Subquery"              => InExpr_Subquery(visit(n)(subquery))
    }
  }

  /*-
     LIKE ScalarOrSubselectExpr Escape?
   */
  def likeExpr(node: Node) = {
    val like = visit(node.getNode(0))(scalarOrSubselectExpr)
    val esc = visitOpt(node.getNode(1))(escape)
    LikeExpr(like, esc)
  }

  /*-
     ESCAPE ScalarExpr
   */
  def escape(node: Node) = {
    val expr = visit(node.getNode(0))(scalarExpr)
    Escape(expr)
  }

  /*-
     MEMBER OF? CollectionValuedPathExpr
   */
  def collectionMemberExpr(node: Node) = {
    val expr = visit(node.getNode(0))(collectionValuedPathExpr)
    CollectionMemberExpr(expr)
  }

  /*-
     EXISTS LParen Subquery RParen 
   */
  def existsExpr(node: Node) = {
    val subq = visit(node.getNode(0))(subquery)
    ExistsExpr(subq)
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
    val op = node.getString(0) match {
      case "="  => EQ
      case "<>" => NE
      case ">"  => GT
      case ">=" => GE
      case "<"  => LT
      case "<=" => LE
    }
    val right = visit(node.getNode(1))(comparisonExprRightOperand)
    ComparisonExpr(op, right)
  }

  /*-
     ArithExpr
   / NonArithScalarExpr
   / AnyOrAllExpr
   */
  def comparisonExprRightOperand(node: Node) = {
    val n = node.getNode(0)
    n.getName match {
      case "ArithExpr"          => ComparsionExprRightOperand_ArithExpr(visit(n)(arithExpr))
      case "NonArithScalarExpr" => ComparsionExprRightOperand_NonArithScalarExpr(visit(n)(nonArithScalarExpr))
      case "AnyOrAllExpr"       => ComparsionExprRightOperand_AnyOrAllExpr(visit(n)(anyOrAllExpr))
    }
  }

  /*-
     SimpleArithExpr
   / LParen Subquery RParen
   */
  def arithExpr(node: Node) = {
    val n = node.getNode(0)
    val expr = n.getName match {
      case "SimpleArithExpr" => Left(visit(n)(simpleArithExpr))
      case "Subquery"        => Right(visit(n)(subquery))
    }
    ArithExpr(expr)
  }

  /*-
     ArithTerm ( ArithTermPlus / ArithTermMinus )* 
   */
  def simpleArithExpr(node: Node) = {
    val term = visit(node.getNode(0))(arithTerm)
    val terms = visitList(node.getList(1)) { n =>
      n.getName match {
        case "ArithTermPlus"  => ArithTerm_Plus(visit(n.getNode(0))(arithTerm))
        case "ArithTermMinus" => ArithTerm_Minus(visit(n.getNode(0))(arithTerm))
      }
    }
    SimpleArithExpr(term, terms)
  }

  /*-
     ArithFactor ( ArithFactorMultiply / ArithFactorDivide )* 
   */
  def arithTerm(node: Node): ArithTerm = {
    val factor = visit(node.getNode(0))(arithFactor)
    val factors = visitList(node.getList(1)) { n =>
      n.getName match {
        case "ArithFactorMultiply" => ArithFactor_Multiply(visit(n.getNode(0))(arithFactor))
        case "ArithFactorDivide"   => ArithFactor_Divide(visit(n.getNode(0))(arithFactor))
      }
    }
    ArithTerm(factor, factors)
  }

  /*-
     ArithPrimaryPlus
   / ArithPrimaryMinus
   / ArithPrimary
   */
  def arithFactor(node: Node) = {
    val n = node.getNode(0)
    val primary = n.getName match {
      case "ArithPrimaryPlus"  => ArithPrimary_Plus(visit(n.getNode(0))(arithPrimary))
      case "ArithPrimaryMinus" => ArithPrimary_Minus(visit(n.getNode(0))(arithPrimary))
      case "ArithPrimary"      => ArithPrimary_Plus(visit(n)(arithPrimary)) // default "+"
    }
    ArithFactor(primary)
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
    node.get(0) match {
      case n: Node =>
        n.getName match {
          case "PathExprOrVarAccess"    => ArithPrimary_PathExprOrVarAccess(visit(n)(pathExprOrVarAccess))
          case "InputParam"             => ArithPrimary_InputParam(visit(n)(inputParam))
          case "CaseExpr"               => ArithPrimary_CaseExpr(visit(n)(caseExpr))
          case "FuncsReturningNumerics" => ArithPrimary_FuncsReturningNumerics(visit(n)(funcsReturningNumerics))
          case "SimpleArithExpr"        => ArithPrimary_SimpleArithExpr(visit(n)(simpleArithExpr))
        }
      case v: Number => ArithPrimary_LiteralNumeric(v)
    }
  }

  /*-
     SimpleArithExpr
   / NonArithScalarExpr
   */
  def scalarExpr(node: Node): ScalarExpr = {
    val n = node.getNode(0)
    n.getName match {
      case "SimpleArithExpr"    => ScalarExpr_SimpleArithExpr(visit(n)(simpleArithExpr))
      case "NonArithScalarExpr" => ScalarExpr_NonArithScalarExpr(visit(n)(nonArithScalarExpr))
    }
  }

  /*-
     ArithExpr
   / NonArithScalarExpr
   */
  def scalarOrSubselectExpr(node: Node) = {
    val n = node.getNode(0)
    n.getName match {
      case "ArithExpr"          => ScalarOrSubselectExpr_ArithExpr(visit(n)(arithExpr))
      case "NonArithScalarExpr" => ScalarOrSubselectExpr_NonArithScalarExpr(visit(n)(nonArithScalarExpr))
    }
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
    node.get(0) match {
      case n: Node =>
        n.getName match {
          case "FuncsReturningDatetime" => NonArithScalarExpr_FuncsReturningDatetime(visit(n)(funcsReturningDatetime))
          case "FuncsReturningStrings"  => NonArithScalarExpr_FuncsReturningStrings(visit(n)(funcsReturningStrings))
          case "EntityTypeExpr"         => NonArithScalarExpr_EntityTypeExpr(visit(n)(entityTypeExpr))
        }
      case v: java.lang.String            => NonArithScalarExpr_LiteralString(v)
      case v: java.lang.Boolean           => NonArithScalarExpr_LiteralBoolean(v)
      case v: java.time.temporal.Temporal => NonArithScalarExpr_LiteralTemporal(v)
    }
  }

  /*-
     ALL  LParen Subquery RParen
   / ANY  LParen Subquery RParen
   / SOME LParen Subquery RParen
   */
  def anyOrAllExpr(node: Node) = {
    val anyOrAll = node.getString(0) match {
      case "all"  => ALL
      case "any"  => ANY
      case "some" => SOME
    }
    val subq = visit(node.getNode(1))(subquery)
    AnyOrAllExpr(anyOrAll, subq)
  }

  /*-
     TypeDiscriminator
   */
  def entityTypeExpr(node: Node) = {
    val tp = visit(node.getNode(0))(typeDiscriminator)
    EntityTypeExpr(tp)
  }

  /*-
     TYPE LParen VarOrSingleValuedPath RParen
   / TYPE LParen InputParam            RParen
   */
  def typeDiscriminator(node: Node) = {
    val n = node.getNode(0)
    val expr = n.getName match {
      case "VarOrSingleValuedPath" => Left(visit(n)(varOrSingleValuedPath))
      case "InputParam"            => Right(visit(n)(inputParam))
    }
    TypeDiscriminator(expr)
  }

  /*-
     SimpleCaseExpr
   / GeneralCaseExpr
   / CoalesceExpr
   / NullifExpr
   */
  def caseExpr(node: Node) = {
    val n = node.getNode(0)
    n.getName match {
      case "SimpleCaseExpr"  => CaseExpr_SimpleCaseExpr(visit(n)(simpleCaseExpr))
      case "GeneralCaseExpr" => CaseExpr_GeneralCaseExpr(visit(n)(generalCaseExpr))
      case "CoalesceExpr"    => CaseExpr_CoalesceExpr(visit(n)(coalesceExpr))
      case "NullifExpr"      => CaseExpr_NullifExpr(visit(n)(nullifExpr))
    }
  }

  /*-
     CASE CaseOperand SimpleWhenClause SimpleWhenClause* ELSE ScalarExpr END
   */
  def simpleCaseExpr(node: Node) = {
    val operand = visit(node.getNode(0))(caseOperand)
    val when = visit(node.getNode(1))(simpleWhenClause)
    val whens = visitList(node.getList(2))(simpleWhenClause)
    val elseExpr = visit(node.getNode(3))(scalarExpr)
    SimpleCaseExpr(operand, when, whens, elseExpr)
  }

  /*-
     CASE WhenClause WhenClause* ELSE ScalarExpr END
   */
  def generalCaseExpr(node: Node) = {
    val when = visit(node.getNode(0))(whenClause)
    val whens = visitList(node.getList(1))(whenClause)
    val elseExpr = visit(node.getNode(2))(scalarExpr)
    GeneralCaseExpr(when, whens, elseExpr)
  }

  /*-
     COALESCE LParen ScalarExpr ( COMMA ScalarExpr )+ RParen
   */
  def coalesceExpr(node: Node) = {
    val expr = visit(node.getNode(0))(scalarExpr)
    val exprs = visitList(node.getList(1))(scalarExpr)
    CoalesceExpr(expr, exprs)
  }

  /*-
     NULLIF LParen ScalarExpr COMMA ScalarExpr RParen
   */
  def nullifExpr(node: Node) = {
    val expr = visit(node.getNode(0))(scalarExpr)
    val rightExpr = visit(node.getNode(1))(scalarExpr)
    NullifExpr(expr, rightExpr)
  }

  /*-
     StateFieldPathExpr
   / TypeDiscriminator
   */
  def caseOperand(node: Node) = {
    val n = node.getNode(0)
    val expr = n.getName match {
      case "StateFieldPathExpr" => Left(visit(n)(stateFieldPathExpr))
      case "TypeDiscriminator"  => Right(visit(n)(typeDiscriminator))
    }
    CaseOperand(expr)
  }

  /*-
     WHEN CondExpr THEN ScalarExpr
   */
  def whenClause(node: Node) = {
    val when = visit(node.getNode(0))(condExpr)
    val thenExpr = visit(node.getNode(1))(scalarExpr)
    WhenClause(when, thenExpr)
  }

  /*-
     WHEN ScalarExpr THEN ScalarExpr
   */
  def simpleWhenClause(node: Node) = {
    val when = visit(node.getNode(0))(scalarExpr)
    val thenExpr = visit(node.getNode(1))(scalarExpr)
    SimpleWhenClause(when, thenExpr)
  }

  /*-
     SingleValuedPathExpr
   / VarAccessOrTypeConstant
   */
  def varOrSingleValuedPath(node: Node) = {
    val n = node.getNode(0)
    val expr = n.getName match {
      case "SingleValuedPathExpr"    => Left(visit(n)(singleValuedPathExpr))
      case "VarAccessOrTypeConstant" => Right(visit(n)(varAccessOrTypeConstant))
    }
    VarOrSingleValuedPath(expr)
  }

  /*-
     LiteralString 
   / FuncsReturningStrings
   / InputParam
   / StateFieldPathExpr 
   */
  def stringPrimary(node: Node) = {
    node.get(0) match {
      case v: String => StringPrimary_LiteralString(v)
      case n: Node =>
        n.getName match {
          case "FuncsReturningStrings" => StringPrimary_FuncsReturningStrings(visit(n)(funcsReturningStrings))
          case "InputParam"            => StringPrimary_InputParam(visit(n)(inputParam))
          case "StateFieldPathExpr"    => StringPrimary_StateFieldPathExpr(visit(n)(stateFieldPathExpr))
        }
    }
  }

  /*-
     LiteralNumeric
   / LiteralBoolean
   / LiteralString
   */
  def literal(node: Node): AnyRef = {
    node.get(0) match {
      case v: java.lang.Integer => v
      case v: java.lang.Long    => v
      case v: java.lang.Float   => v
      case v: java.lang.Double  => v
      case v: java.lang.Boolean => v
      case v: java.lang.String  => v
    }
  }

  /*-
     namedInputParam 
   / positionInputParam

  String namedInputParam = void:':' v:identifier { yyValue = v; } ;
  Integer positionInputParam = void:'?' v:position   { yyValue = Integer.parseInt(v, 10); } ;
  transient String position = [1-9] [0-9]* ;
      void:':' v:identifier { yyValue = v; }
    / void:'?' v:position   { yyValue = Integer.parseInt(v, 10); }
  */
  def inputParam(node: Node) = {
    node.get(0) match {
      case v: java.lang.String  => InputParam_Named(v)
      case v: java.lang.Integer => InputParam_Position(v)
    }
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
    val n = node.getNode(0)
    n.getName match {
      case "Abs"    => visit(n)(abs)
      case "Length" => visit(n)(length)
      case "Mod"    => visit(n)(mod)
      case "Locate" => visit(n)(locate)
      case "Size"   => visit(n)(size)
      case "Sqrt"   => visit(n)(sqrt)
      case "Index"  => visit(n)(index)
      case "Func"   => visit(n)(func)
    }
  }

  /*-
     CURRENT_DATE
   / CURRENT_TIME
   / CURRENT_TIMESTAMP
   */
  def funcsReturningDatetime(node: Node) = {
    node.getString(0) match {
      case "current_date"      => CURRENT_DATE
      case "current_time"      => CURRENT_TIME
      case "current_timestamp" => CURRENT_TIMESTAMP
    }
  }

  /*-
     Concat
   / Substring
   / Trim
   / Upper
   / Lower
   */
  def funcsReturningStrings(node: Node): FuncsReturningStrings = {
    val n = node.getNode(0)
    n.getName match {
      case "Concat"    => visit(n)(concat)
      case "Substring" => visit(n)(substring)
      case "Trim"      => visit(n)(trim)
      case "Upper"     => visit(n)(upper)
      case "Lower"     => visit(n)(lower)
    }
  }

  /*-
     CONCAT LParen ScalarExpr (COMMA ScalarExpr)+ RParen
   */
  def concat(node: Node) = {
    val expr = visit(node.getNode(0))(scalarExpr)
    val exprs = visitList(node.getList(1))(scalarExpr)
    Concat(expr, exprs)
  }

  /*-
     SUBSTRING LParen ScalarExpr COMMA ScalarExpr ( COMMA ScalarExpr )? RParen
   */
  def substring(node: Node) = {
    val expr = visit(node.getNode(0))(scalarExpr)
    val expr2 = visit(node.getNode(1))(scalarExpr)
    val expr3 = visitOpt(node.getNode(2))(scalarExpr)
    Substring(expr, expr2, expr3)
  }

  /*-
     TRIM LParen TrimSpec? TrimChar? FROM? StringPrimary RParen
   */
  def trim(node: Node) = {
    val spec = visitOpt(node.getNode(0))(trimSpec) // default BOTH
    val char = visitOpt(node.getNode(1))(trimChar)
    val from = visit(node.getNode(2))(stringPrimary)
    Trim(spec, char, from)
  }

  /*-
     LEADING 
   / TRAILING
   / BOTH
   */
  def trimSpec(node: Node) = {
    node.getString(0) match {
      case "leading"  => LEADING
      case "trailing" => TRAILING
      case "both"     => BOTH
    }
  }

  /*-
     LiteralSingleQuotedString
   / InputParam
   */
  def trimChar(node: Node) = {
    node.get(0) match {
      case n: Node   => TrimChar_InputParam(visit(n)(inputParam))
      case v: String => TrimChar_String(v)
    }
  }

  /*-
     UPPER LParen ScalarExpr RParen
   */
  def upper(node: Node) = {
    val expr = visit(node.getNode(0))(scalarExpr)
    Upper(expr)
  }

  /*-
     LOWER LParen ScalarExpr RParen
   */
  def lower(node: Node) = {
    val expr = visit(node.getNode(0))(scalarExpr)
    Lower(expr)
  }

  /*-
     ABS LParen SimpleArithExpr RParen
   */
  def abs(node: Node) = {
    val expr = visit(node.getNode(0))(simpleArithExpr)
    Abs(expr)
  }

  /*-
     LENGTH LParen ScalarExpr RParen
   */
  def length(node: Node) = {
    val expr = visit(node.getNode(0))(scalarExpr)
    Length(expr)
  }

  /*-
     LOCATE LParen ScalarExpr COMMA ScalarExpr ( COMMA ScalarExpr )? RParen
   */
  def locate(node: Node) = {
    val expr = visit(node.getNode(0))(scalarExpr)
    val searchExpr = visit(node.getNode(1))(scalarExpr)
    val startExpr = visitOpt(node.getNode(2))(scalarExpr)
    Locate(expr, searchExpr, startExpr)
  }

  /*-
     SIZE LParen CollectionValuedPathExpr RParen
   */
  def size(node: Node) = {
    val expr = visit(node.getNode(0))(collectionValuedPathExpr)
    Size(expr)
  }

  /*-
     MOD LParen ScalarExpr COMMA ScalarExpr RParen
   */
  def mod(node: Node) = {
    val expr = visit(node.getNode(0))(scalarExpr)
    val divisorExpr = visit(node.getNode(1))(scalarExpr)
    Mod(expr, divisorExpr)
  }

  /*-
     SQRT LParen ScalarExpr RParen
   */
  def sqrt(node: Node) = {
    val expr = visit(node.getNode(0))(scalarExpr)
    Sqrt(expr)
  }

  /*-
     INDEX LParen VarAccessOrTypeConstant RParen
   */
  def index(node: Node) = {
    val expr = visit(node.getNode(0))(varAccessOrTypeConstant)
    Index(expr)
  }

  /*-
     FUNCTION LParen LiteralSingleQuotedString ( COMMA NewValue )* RParen
   */
  def func(node: Node) = {
    val name = node.getString(0)
    val args = visitList(node.getList(1))(newValue)
    Func(name, args)
  }

  /*-
     SimpleSelectClause SubqueryFromClause WhereClause? GroupbyClause? HavingClause?
   */
  def subquery(node: Node) = {
    val select = visit(node.getNode(0))(simpleSelectClause)
    val from = visit(node.getNode(1))(subqueryFromClause)
    val where = visitOpt(node.getNode(2))(whereClause)
    val groupby = visitOpt(node.getNode(3))(groupbyClause)
    val having = visitOpt(node.getNode(4))(havingClause)
    Subquery(select, from, where, groupby, having)
  }

  /*-
     SELECT DISTINCT? SimpleSelectExpr
   */
  def simpleSelectClause(node: Node) = {
    val isDistinct = node.get(0) ne null
    val expr = visit(node.getNode(1))(simpleSelectExpr)
    SimpleSelectClause(isDistinct, expr)
  }

  /*-
     SingleValuedPathExpr 
   / AggregateExpr
   / VarAccessOrTypeConstant
   */
  def simpleSelectExpr(node: Node) = {
    val n = node.getNode(0)
    n.getName match {
      case "SingleValuedPathExpr"    => SimpleSelectExpr_SingleValuedPathExpr(visit(n)(singleValuedPathExpr))
      case "AggregateExpr"           => SimpleSelectExpr_AggregateExpr(visit(n)(aggregateExpr))
      case "VarAccessOrTypeConstant" => SimpleSelectExpr_VarAccessOrTypeConstant(visit(n)(varAccessOrTypeConstant))
    }
  }

  /*-
     FROM SubselectIdentVarDecl ( COMMA ( SubselectIdentVarDecl 
                                        / CollectionMemberDecl
                                        ) 
                                )*
   */
  def subqueryFromClause(node: Node) = {
    val from = visit(node.getNode(0))(subselectIdentVarDecl)
    val froms = visitList(node.getList(1)) { n =>
      n.getName match {
        case "SubselectIdentVarDecl" => Left(visit(n)(subselectIdentVarDecl))
        case "CollectionMemberDecl"  => Right(visit(n)(collectionMemberDecl))
      }
    }
    SubqueryFromClause(from, froms)
  }

  /*-
     IdentVarDecl
   / AssocPathExpr AS? Ident
   / CollectionMemberDecl
   */
  def subselectIdentVarDecl(node: Node) = {
    val n = node.getNode(0)
    n.getName match {
      case "IdentVarDecl"         => SubselectIdentVarDecl_IdentVarDecl(visit(n)(identVarDecl))
      case "AssoPathExpr"         => SubselectIdentVarDecl_AssocPathExpr(visit(n)(assocPathExpr), visit(node.getNode(1))(ident))
      case "CollectionMemberDecl" => SubselectIdentVarDecl_CollectionMemberDecl(visit(n)(collectionMemberDecl))
    }
  }

  /*-
     ORDER BY OrderbyItem ( COMMA OrderbyItem )*
   */
  def orderbyClause(node: Node) = {
    val item = visit(node.getNode(0))(orderbyItem)
    val items = visitList(node.getList(1))(orderbyItem)
    OrderbyClause(item, items)
  }

  /*-
     ( SimpleArithExpr / ScalarExpr ) ( ASC / DESC )?
   */
  def orderbyItem(node: Node) = {
    val n = node.getNode(0)
    val expr = n.getName match {
      case "SimpleArithExpr" => Left(visit(n)(simpleArithExpr))
      case "ScalarExpr"      => Right(visit(n)(scalarExpr))
    }
    val isAsc = node.getString(1) match {
      case null   => true
      case "asc"  => true
      case "desc" => false
    }
    OrderbyItem(expr, isAsc)
  }

  /*-
     GROUP BY ScalarExpr ( COMMA ScalarExpr )* 
   */
  def groupbyClause(node: Node) = {
    val expr = visit(node.getNode(0))(scalarExpr)
    val exprs = visitList(node.getList(1))(scalarExpr)
    GroupbyClause(expr, exprs)
  }

  /*-
     HAVING CondExpr 
   */
  def havingClause(node: Node) = {
    val expr = visit(node.getNode(0))(condExpr)
    HavingClause(expr)
  }

  /*-
     Identifier
   */
  def ident(node: Node) = {
    val name = node.getString(0)
    Ident(name)
  }

}