package chana.jpql

import chana.jpql.rats.JPQLGrammar
import java.io.StringReader
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.WordSpecLike

class JPQLGrammarSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {

  def parse(query: String) {
    val reader = new StringReader(query)
    val grammar = new JPQLGrammar(reader, "<current>")
    val r = grammar.pJPQL(0)
    if (r.hasValue) {
      // for the signature of method: <T> T semanticValue(), we have to call
      // with at least of Any to avoid:
      //   xtc.tree.GNode$Fixed1 cannot be cast to scala.runtime.Nothing$
      val rootNode = r.semanticValue[Any]
      info("\n## " + query + " ##\n" + rootNode.toString)
    }

    assert(r.hasValue, "\n## " + query + " ##\n" + r.parseError.msg + " at " + r.parseError.index)
  }

  "JPQLGrammar" when {
    "parse jpql statement" should {

      "with Aggregation functions" in {
        val queris = List(
          "SELECT COUNT(e) FROM Employee e",
          "SELECT MAX(e.salary) FROM Employee e")

        queris foreach parse

      }

      "with Constructors" in {
        val queris = List(
          "SELECT NEW com.acme.reports.EmpReport(e.firstName, e.lastName, e.salary) FROM Employee e")
        queris foreach parse
      }

      "with FROM Clause" in {
        val queris = List(
          "SELECT e FROM Employee e",
          "SELECT e, a FROM Employee e, MailingAddress a WHERE e.address = a.address")

        queris foreach parse
      }

      "with JOIN" in {
        val queris = List(
          "SELECT e FROM Employee e JOIN e.address a WHERE a.city = :city",
          "SELECT e FROM Employee e JOIN e.projects p JOIN e.projects p2 WHERE p.name = :p1 AND p2.name = :p2")

        queris foreach parse
      }

      "with JOIN FETCH" in {
        val queris = List(
          "SELECT e FROM Employee e JOIN FETCH e.address",
          "SELECT e FROM Employee e JOIN FETCH e.address a ORDER BY a.city")

        queris foreach parse
      }

      "with LEFT JOIN" in {
        val queris = List(
          "SELECT e FROM Employee e LEFT JOIN e.address a ORDER BY a.city")

        queris foreach parse
      }

      "with ON" in {
        val queris = List(
          "SELECT e FROM Employee e LEFT JOIN e.address a ON a.city = :city",
          "SELECT e FROM Employee e LEFT JOIN MailingAddress a ON e.address = a.address")

        queris foreach parse
      }

      //"with Sub-selects in FROM clause" in {
      //  val queris = List(
      //    "SELECT e, c.city FROM Employee e, (SELECT DISTINCT a.city FROM Address a) c WHERE e.address.city = c.city"
      //  )
      //
      //  queris foreach parse
      //}

      "with ORDER BY clause" in {
        val queris = List(
          "SELECT e FROM Employee e ORDER BY e.lastName ASC, e.firstName ASC",
          "SELECT e FROM Employee e ORDER BY UPPER(e.lastName)",
          "SELECT e FROM Employee e LEFT JOIN e.manager m ORDER BY m.lastName",
          "SELECT e FROM Employee e ORDER BY e.address")

        queris foreach parse
      }

      "with GROUP BY Clause" in {
        val queris = List(
          "SELECT AVG(e.salary), e.address.city FROM Employee e GROUP BY e.address.city",
          "SELECT AVG(e.salary), e.address.city FROM Employee e GROUP BY e.address.city ORDER BY e.salary",
          "SELECT e, COUNT(p) FROM Employee e LEFT JOIN e.projects p GROUP BY e")

        queris foreach parse
      }

      "with HAVING Clause" in {
        val queris = List(
          "SELECT AVG(e.salary), e.address.city FROM Employee e GROUP BY e.address.city HAVING e.salary > 100000")

        queris foreach parse
      }

      "with WHERE Clause" in {
        val queris = List(
          "SELECT e FROM Employee e WHERE e.firstName IN (:name1, :name2, :name3)",
          "SELECT e FROM Employee e WHERE e.firstName IN (:name1)",
          "SELECT e FROM Employee e WHERE e.firstName IN :names",
          "SELECT e FROM Employee e WHERE e.firstName IN (SELECT e2.firstName FROM Employee e2 WHERE e2.lastName = 'Smith')",
          "SELECT e FROM Employee e WHERE e.firstName = (SELECT e2.firstName FROM Employee e2 WHERE e2.id = :id)",
          "SELECT e FROM Employee e WHERE e.salary < (SELECT e2.salary FROM Employee e2 WHERE e2.id = :id)",
          "SELECT e FROM Employee e WHERE e.firstName = ANY (SELECT e2.firstName FROM Employee e2 WHERE e.id <> e.id)",
          "SELECT e FROM Employee e WHERE e.salary <= ALL (SELECT e2.salary FROM Employee e2)",
          "SELECT e FROM Employee e WHERE e.manager = e2.manager",
          "SELECT e FROM Employee e WHERE e.manager = :manager",
          "SELECT e FROM Employee e WHERE e.manager <> :manager",
          "SELECT e FROM Employee e WHERE e.manager IS NULL",
          "SELECT e FROM Employee e WHERE e.manager IS NOT NULL",
          "SELECT e FROM Employee e WHERE e.manager IN (SELECT e2 FROM Employee e2 WHERE SIZE(e2.managedEmployees) < 2)",
          "SELECT e FROM Employee e WHERE e.manager NOT IN (:manager1, :manager2)")

        queris foreach parse
      }

      "with Update Querie" in {
        val queris = List(
          "UPDATE Employee e SET e.salary = 60000 WHERE e.salary = 50000")

        queris foreach parse
      }

      "with Delete Queries" in {
        val queris = List(
          "DELETE FROM Employee e WHERE e.department IS NULL")

        queris foreach parse
      }

      "with Literals" in {
        val queris = List(
          "SELECT e FROM Employee e WHERE e.name = 'Bob'",
          "SELECT e FROM Employee e WHERE e.name = 'Baie-D''Urfé'",
          "SELECT e FROM Employee e WHERE e.id = 1234",
          "SELECT e FROM Employee e WHERE e.id = 1234L",
          "SELECT s FROM Stat s WHERE s.ratio > 3.14F",
          "SELECT s FROM Stat s WHERE s.ratio > 3.14e32D",
          "SELECT e FROM Employee e WHERE e.active = TRUE",
          "SELECT e FROM Employee e WHERE e.startDate = {d'2012-01-03'}",
          "SELECT e FROM Employee e WHERE e.startTime = {t'09:00:00'}",
          "SELECT e FROM Employee e WHERE e.version = {ts'2012-01-03 09:00:00.000000001'}",
          "SELECT e FROM Employee e WHERE e.gender = org.acme.Gender.MALE",
          "UPDATE Employee e SET e.manager = NULL WHERE e.manager = :manager")

        queris foreach parse
      }

      "with Functions" in {
        val queris = List(
          "SELECT (e.salary - 1000) FROM Employee e",
          "SELECT (e.salary + 1000) FROM Employee e",
          "SELECT (e.salary * 1000) FROM Employee e",
          "SELECT (e.salary / 1000) FROM Employee e",
          "SELECT ABS(e.salary - e.manager.salary) FROM Employee e",
          "SELECT CASE e.STATUS WHEN 0 THEN 'active' WHEN 1 THEN 'consultant' ELSE 'unknown' END FROM Employee e",
          "SELECT COALESCE(e.salary, 0) FROM Employee e",
          "SELECT CONCAT(e.firstName, ' ', e.lastName) FROM Employee e",
          "SELECT CURRENT_DATE FROM Employee e WHERE e.time = CURRENT_DATE",
          "SELECT CURRENT_DATE FROM Employee e",
          "SELECT CURRENT_TIME FROM Employee e",
          "SELECT CURRENT_TIMESTAMP FROM Employee e",
          "SELECT LENGTH(e.lastName) FROM Employee e",
          "SELECT LOCATE('-', e.lastName) FROM Employee e",
          "SELECT LOWER(e.lastName) FROM Employee e",
          "SELECT MOD(e.hoursWorked, 8) FROM Employee e",
          "SELECT NULLIF(e.salary, 0) FROM Employee e",
          "SELECT SQRT(e.RESULT) FROM Employee e",
          "SELECT SUBSTRING(e.lastName, 0, 2) FROM Employee e",
          "SELECT TRIM(TRAILING FROM e.lastName), TRIM(e.lastName), TRIM(LEADING '-' FROM e.lastName) FROM Employee e",
          "SELECT UPPER(e.lastName) FROM Employee e")

        queris foreach parse
      }

      "with Special Operators" in {
        val queris = List(
          "SELECT toDo FROM Employee e JOIN e.toDoList toDo WHERE INDEX(toDo) = 1",
          "SELECT p FROM Employee e JOIN e.priorities p WHERE KEY(p) = 'high'",
          "SELECT e FROM Employee e WHERE SIZE(e.managedEmployees) < 2",
          "SELECT e FROM Employee e WHERE e.managedEmployees IS EMPTY",
          "SELECT e FROM Employee e WHERE 'write code' MEMBER OF e.responsibilities",
          "SELECT p FROM Project p WHERE TYPE(p) = LargeProject",
          "SELECT e FROM Employee e JOIN TREAT(e.projects AS LargeProject) p WHERE p.budget > 1000000",
          "SELECT p FROM Phone p WHERE FUNCTION('TO_NUMBER', p.areaCode) > 613")

        queris foreach parse
      }

    }
  }

}
