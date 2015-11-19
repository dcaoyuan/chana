package chana.avpath

import chana.avro.FromJson
import chana.avro.ToJson
import chana.avpath.Evaluator.Ctx
import chana.avro.Schemas
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.WordSpecLike

class AvPathSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {
  import chana.avro.AvroRecords._

  val jsonAccountDefault = """
{"id": "-1", "registerTime": 0, "lastLoginTime": 0, "loginRecords": [], "chargeRecords": [], "activityRecords": [], "balance": 0.0, "numFriends": 0, "numContacts": 0, "numPlayedGames": 0, "score": 0.0, "scoreActives": 0.0, "scoreFaithful": 0.0, "scoreLogins": 0.0, "scorePayments": 0.0, "scoreApps": 0.0, "scoreActions": 0.0, "devApps": {}, "devActions": {}}
""".trim

  val jsonAccountUncomplete = """
{"registerTime":1,"lastLoginTime":1}
""".trim
  val jsonAccountFilled = """
{"id": "-1", "registerTime": 1, "lastLoginTime": 1, "loginRecords": [], "chargeRecords": [], "activityRecords": [], "balance": 0.0, "numFriends": 0, "numContacts": 0, "numPlayedGames": 0, "score": 0.0, "scoreActives": 0.0, "scoreFaithful": 0.0, "scoreLogins": 0.0, "scorePayments": 0.0, "scoreApps": 0.0, "scoreActions": 0.0, "devApps": {}, "devActions": {}}
""".trim

  val jsonAccountReal = """
{"id":"12","registerTime":687557347200000,"lastLoginTime":688421347200000,"loginRecords":[{"time":688421347200000,"kind":"WDJ"},{"time":1401248164983,"kind":"GAME"},{"time":1401248164993,"kind":"GAME"}],"chargeRecords":[{"time":-17188358400000,"amount":20.0}],"activityRecords":null,"balance":100.0,"devApps":null,"devActions":{"udid121":{"actionRecords":[{"time":1401248186700,"kind":"DOWNLOAD"},{"time":1401248186700,"kind":"CLICK"},{"time":1401248186700,"kind":"QUERY"}]},"udid122":{"actionRecords":[{"time":1401248186701,"kind":"CLICK"},{"time":1401248186701,"kind":"QUERY"}]}}}
""".trim

  "AvPath" when {

    s"using GenericRecordBuilder to create new record:" should {
      val account = accountBuilder.build()
      info("Account: \n" + account)
      val fromJsonAccountDefault = FromJson.fromJsonString(jsonAccountDefault, schema)
      s"be filled with default value as:\n ${fromJsonAccountDefault}" in {
        assertResult(fromJsonAccountDefault)(account)
      }
    }

    s"an uncomplete json:\n ${jsonAccountUncomplete} \n its uncompleted fields" should {
      val fromJsonAccount1 = FromJson.fromJsonString(jsonAccountUncomplete, schema).asInstanceOf[GenericData.Record]
      val fromJsonAccountFilled1 = FromJson.fromJsonString(jsonAccountFilled, schema)
      s"be filled with default value by FromJson.fromJsonString (generic) as:\n ${fromJsonAccountFilled1}" in {
        assertResult(fromJsonAccountFilled1)(fromJsonAccount1)
      }

      val fromJsonAccount2 = FromJson.fromJsonString(jsonAccountUncomplete, schema, true)
      val fromJsonAccountFilled2 = FromJson.fromJsonString(jsonAccountFilled, schema, true)
      s"be filled with default value by FromJson.fromJsonString (specified) as:\n ${fromJsonAccountFilled2}" in {
        assertResult(fromJsonAccountFilled2)(fromJsonAccount2)
      }

      val jsonAccountUncomplete1 = ToJson.toJsonString(fromJsonAccount1, schema)
      s"be compacted to concise json string which drops all default values vice versa as:\n ${jsonAccountUncomplete1}" in {
        assertResult(jsonAccountUncomplete)(jsonAccountUncomplete1)
      }
    }

    s"a real world json:\n ${jsonAccountReal} \n " should {
      val fromJsonAccount1 = FromJson.fromJsonString(jsonAccountReal, schema)
      val fromJsonAccountReal1 = FromJson.fromJsonString(ToJson.toJsonString(fromJsonAccount1, schema), schema)
      s"be exactly the same as the one decoded from FromJson.fromJsonString (generic) as:\n ${fromJsonAccountReal1}" in {
        assertResult(fromJsonAccount1)(fromJsonAccountReal1)
      }

      val fromJsonAccount2 = FromJson.fromJsonString(jsonAccountReal, schema, true)
      val fromJspnAccountReal2 = FromJson.fromJsonString(ToJson.toJsonString(fromJsonAccount2, schema), schema, true)
      s"be exactly the same as the one decoded from FromJson.fromJsonString (specified)as:\n ${fromJspnAccountReal2}" in {
        assertResult(fromJsonAccount2)(fromJspnAccountReal2)
      }
    }

    "select/update record itself" should {
      val record = initAccount()
      val path = "."
      val ast = new Parser().parse(path)

      val res0 = Evaluator.select(record, ast)
      s"select |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        assertResult(Set(null))(res0.map(_.topLevelField).toSet)
        assertResult(List(record))(res0.map(_.value))
      }

      val record2 = initAccount()
      record2.put("registerTime", 10000L)
      Evaluator.update(record, ast, record2)
      val res1 = Evaluator.select(record, ast)
      s"update |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res1.map(_.value).mkString("\n"))

        assertResult(List(record2))(res1.map(_.value))
      }

      Evaluator.updateJson(record, ast, ToJson.toJsonString(record2))
      val res2 = Evaluator.select(record, ast)
      s"updateJson |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res2.map(_.value).mkString("\n"))

        assertResult(List(record2))(res2.map(_.value))
      }
    }

    "select/update record field" should {
      val record = initAccount()
      val path = ".registerTime"
      val ast = new Parser().parse(path)

      Evaluator.update(record, ast, 1234L)
      val res0 = Evaluator.select(record, ast)
      s"select |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        assertResult(Set("registerTime"))(res0.map(_.topLevelField).map(_.name).toSet)
        assertResult(List(1234))(res0.map(_.value))
      }
    }

    "select/update record multiple fields" should {
      val record = initAccount()
      val path = "(.registerTime| .lastLoginTime)"
      val ast = new Parser().parse(path)

      Evaluator.update(record, ast, 1234L)
      val res0 = Evaluator.select(record, ast)
      s"select |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        assertResult(Set("registerTime", "lastLoginTime"))(res0.map(_.topLevelField).map(_.name).toSet)
        assertResult(List(1234, 1234))(res0.map(_.value))
      }
    }

    "select/update record multiple fields with nonExist field" should {
      val record = initAccount()
      val path = "(.registerTime|.nonExist)"
      val ast = new Parser().parse(path)

      Evaluator.update(record, ast, 1234L)
      val res0 = Evaluator.select(record, ast)
      s"select |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        assertResult(Set("registerTime"))(res0.map(_.topLevelField).map(_.name).toSet)
        assertResult(List(1234))(res0.map(_.value))
      }
    }

    "select/update array field's first [0]" should {
      val record = initAccount()
      val path = ".chargeRecords[0].time"
      val ast = new Parser().parse(path)

      val res0 = Evaluator.select(record, ast)
      s"select |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        assertResult(Set("chargeRecords"))(res0.map(_.topLevelField.name).toSet)
        assertResult(List(1))(res0.map(_.value))
      }

      Evaluator.update(record, ast, 1000L)
      val res1 = Evaluator.select(record, ast)
      s"update |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res1.map(_.value).mkString("\n"))

        assertResult(Set("chargeRecords"))(res1.map(_.topLevelField.name).toSet)
        assertResult(List(1000))(res1.map(_.value))
      }

      Evaluator.updateJson(record, ast, "1234")
      val res2 = Evaluator.select(record, ast)
      s"updateJson |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res2.map(_.value).mkString("\n"))

        assertResult(Set("chargeRecords"))(res2.map(_.topLevelField.name).toSet)
        assertResult(List(1234))(res2.map(_.value))
      }
    }

    "select/update array field's last [-1]" should {
      val record = initAccount()
      val path = ".chargeRecords[-1].time"
      val ast = new Parser().parse(path)

      val res0 = Evaluator.select(record, ast)
      s"select |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        assertResult(Set("chargeRecords"))(res0.map(_.topLevelField.name).toSet)
        assertResult(List(2))(res0.map(_.value))
      }

      Evaluator.update(record, ast, 100L)
      val res1 = Evaluator.select(record, ast)
      s"update |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res1.map(_.value).mkString("\n"))

        assertResult(Set("chargeRecords"))(res1.map(_.topLevelField.name).toSet)
        assertResult(List(100))(res1.map(_.value))
      }

      Evaluator.updateJson(record, ast, "1234")
      val res2 = Evaluator.select(record, ast)
      s"updateJson |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res2.map(_.value).mkString("\n"))

        assertResult(Set("chargeRecords"))(res2.map(_.topLevelField.name).toSet)
        assertResult(List(1234))(res2.map(_.value))
      }
    }

    "select/update array field's all [0:1]" should {
      val record = initAccount()
      val path = ".chargeRecords[0:1]{.time > -1}.time"
      val ast = new Parser().parse(path)

      val res0 = Evaluator.select(record, ast)
      s"select |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        assertResult(Set("chargeRecords"))(res0.map(_.topLevelField.name).toSet)
        assertResult(List(1, 2))(res0.map(_.value))
      }
    }

    "select/update array field's all [*]" should {
      val record = initAccount()
      val path = ".chargeRecords[*].time"
      val ast = new Parser().parse(path)

      val res0 = Evaluator.select(record, ast)
      s"select |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        assertResult(Set("chargeRecords"))(res0.map(_.topLevelField.name).toSet)
        assertResult(List(1, 2))(res0.map(_.value))
      }
    }

    "select/update array field's not more than [0:9]" should {
      val record = initAccount()
      val path = ".chargeRecords[0:9].time"
      val ast = new Parser().parse(path)

      val res0 = Evaluator.select(record, ast)
      s"select |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        assertResult(Set("chargeRecords"))(res0.map(_.topLevelField.name).toSet)
        assertResult(List(1, 2))(res0.map(_.value))
      }
    }

    "select with logic AND" should {
      val record = initAccount()
      val path = ".{ .registerTime == 0 && .chargeRecords[0].time == 1 }"
      val ast = new Parser().parse(path)

      val res0 = Evaluator.select(record, ast)
      s"select |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        assertResult(List(0))(res0.map(_.value.asInstanceOf[GenericRecord].get("registerTime")))
      }
    }

    "select with logic OR" should {
      val record = initAccount()
      val path = ".{ .registerTime == 1 || .chargeRecords[0].time == 1 }"
      val ast = new Parser().parse(path)

      val res0 = Evaluator.select(record, ast)
      s"select |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        assertResult(List(0))(res0.map(_.value.asInstanceOf[GenericRecord].get("registerTime")))
      }
    }

    "insert to array field" should {
      val record = initAccount()
      val path = ".chargeRecords"
      val ast = new Parser().parse(path)

      val chargeRecord3 = chargeRecordBuilder.build()
      chargeRecord3.put(Schemas.TIME, 3L)
      chargeRecord3.put(Schemas.AMOUNT, -3.0)
      val jsonChargeRecord4 = """{"time": 4, "amount": -4.0}"""

      Evaluator.insert(record, ast, chargeRecord3)
      Evaluator.insertJson(record, ast, jsonChargeRecord4)
      val res0 = Evaluator.select(record, ast)
      s"insert |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        assertResult(Set("chargeRecords"))(res0.map(_.topLevelField.name).toSet)
        assertResult(4)(res0.map(_.value).head.asInstanceOf[GenericData.Array[GenericData.Record]].toArray.length)
      }
    }

    "insertAll to array field" should {
      val record = initAccount()
      val path = ".chargeRecords"
      val ast = new Parser().parse(path)

      val chargeRecord3 = chargeRecordBuilder.build()
      chargeRecord3.put(Schemas.TIME, 4L)
      chargeRecord3.put(Schemas.AMOUNT, -4.0)
      val chargeRecord4 = chargeRecordBuilder.build()
      chargeRecord4.put(Schemas.TIME, -1L)
      chargeRecord4.put(Schemas.AMOUNT, -5.0)

      val jsonChargeRecords = """[{"time": -1, "amount": -5.0}, {"time": -2, "amount": -6.0}]"""

      Evaluator.insertAll(record, ast, java.util.Arrays.asList(chargeRecord3, chargeRecord4))
      Evaluator.insertAllJson(record, ast, jsonChargeRecords)
      val res0 = Evaluator.select(record, ast)
      s"insertAll |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        assertResult(Set("chargeRecords"))(res0.map(_.topLevelField.name).toSet)
        assertResult(6)(res0.map(_.value).head.asInstanceOf[GenericData.Array[GenericData.Record]].toArray.length)
      }

      val path1 = ".chargeRecords{.time > -1}.time"
      val ast1 = new Parser().parse(path1)
      val res1 = Evaluator.select(record, ast1)
      s"select |${path1}|" in {
        info("AST:\n" + ast1)
        info("Got:\n" + res1.map(_.value).mkString("\n"))

        assertResult(Set("chargeRecords"))(res1.map(_.topLevelField.name).toSet)
        assertResult(List(1, 2, 4))(res1.map(_.value))
      }

      val path2 = ".chargeRecords{.time > 3}"
      val ast2 = new Parser().parse(path2)
      val res2 = Evaluator.select(record, ast2)
      s"select |${path2}|" in {
        info("AST:\n" + ast2)
        info("Got:\n" + res2.map(_.value).mkString("\n"))

        assertResult(Set("chargeRecords"))(res2.map(_.topLevelField.name).toSet)
        assertResult(List(chargeRecord3))(res2.map(_.value))
      }
    }

    "clear array field" should {
      val record = initAccount()
      val path = ".chargeRecords"
      val ast = new Parser().parse(path)

      Evaluator.clear(record, ast)
      val res0 = Evaluator.select(record, ast)
      s"clear |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        assertResult(Set("chargeRecords"))(res0.map(_.topLevelField.name).toSet)
        assertResult(0)(res0.map(_.value).head.asInstanceOf[GenericData.Array[GenericData.Record]].toArray.length)
      }
    }

    "select/update map field" should {
      val record = initAccount()
      val path = ".devApps(\"a\" | \"b\").numBlackApps"
      val ast = new Parser().parse(path)

      val res0 = Evaluator.select(record, ast)
      s"select |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        assertResult(Set("devApps"))(res0.map(_.topLevelField.name).toSet)
        // the order of selected map items is not guaranteed due to the implemetation of java.util.Map
        assertResult(List(1, 2))(res0.map(_.value.asInstanceOf[Int]).sorted)
      }

      Evaluator.update(record, ast, 100)
      val res1 = Evaluator.select(record, ast)
      s"update |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res1.map(_.value).mkString("\n"))

        assertResult(Set("devApps"))(res1.map(_.topLevelField.name).toSet)
        assertResult(List(100, 100))(res1.map(_.value))
      }

      Evaluator.updateJson(record, ast, "123")
      val res2 = Evaluator.select(record, ast)
      s"updateJson |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res2.map(_.value).mkString("\n"))

        assertResult(Set("devApps"))(res2.map(_.topLevelField.name).toSet)
        assertResult(List(123, 123))(res2.map(_.value))
      }
    }

    "select/update map field with regex" should {
      val record = initAccount()
      val path = ".devApps(\"a\" | ~\"b\").numBlackApps"
      val ast = new Parser().parse(path)

      val res0 = Evaluator.select(record, ast)
      s"select |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        assertResult(Set("devApps"))(res0.map(_.topLevelField.name).toSet)
        // the order of selected map items is not guaranteed due to the implemetation of java.util.Map
        assertResult(List(1, 2))(res0.map(_.value.asInstanceOf[Int]).sorted)
      }

      Evaluator.update(record, ast, 100)
      val res1 = Evaluator.select(record, ast)
      s"update |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res1.map(_.value).mkString("\n"))

        assertResult(Set("devApps"))(res1.map(_.topLevelField.name).toSet)
        assertResult(List(100, 100))(res1.map(_.value))
      }

      Evaluator.updateJson(record, ast, "123")
      val res2 = Evaluator.select(record, ast)
      s"updateJson |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res2.map(_.value).mkString("\n"))

        assertResult(Set("devApps"))(res2.map(_.topLevelField.name).toSet)
        assertResult(List(123, 123))(res2.map(_.value))
      }
    }

    "select map field with prediction" should {
      val record = initAccount()
      val path = ".devApps{.numBlackApps > 2}.numBlackApps"
      val ast = new Parser().parse(path)

      val res0 = Evaluator.select(record, ast)
      s"select |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        assertResult(Set("devApps"))(res0.map(_.topLevelField.name).toSet)
        // the order of selected map items is not guaranteed due to the implemetation of java.util.Map
        assertResult(List(3))(res0.map(_.value.asInstanceOf[Int]).sorted)
      }

      Evaluator.update(record, ast, 100)
      val res1 = Evaluator.select(record, ast)
      s"update |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res1.map(_.value).mkString("\n"))

        assertResult(Set("devApps"))(res1.map(_.topLevelField.name).toSet)
        assertResult(List(100))(res1.map(_.value))
      }

      Evaluator.updateJson(record, ast, "123")
      val res2 = Evaluator.select(record, ast)
      s"updateJson |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res2.map(_.value).mkString("\n"))

        assertResult(Set("devApps"))(res2.map(_.topLevelField.name).toSet)
        assertResult(List(123))(res2.map(_.value))
      }
    }

    "insert to map field" should {
      val record = initAccount()
      val path = ".devApps"
      val ast = new Parser().parse(path)

      val appInfo4 = appInfoBuilder.build()
      val appInfo5 = ToJson.toJsonString(appInfo4)
      val jsonAppInfoKv = """{"e" : {}}"""

      Evaluator.insert(record, ast, ("d", appInfo4))
      Evaluator.insertJson(record, ast, jsonAppInfoKv)
      val res0 = Evaluator.select(record, ast)
      s"insert |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        assertResult(Set("devApps"))(res0.map(_.topLevelField.name).toSet)
        assertResult(5)(res0.map(_.value).head.asInstanceOf[java.util.Map[_, _]].size)
      }
    }

    "insertAll to map field" should {
      val record = initAccount()
      val path = ".devApps"
      val ast = new Parser().parse(path)

      val appInfo4 = appInfoBuilder.build()
      val appInfo5 = appInfoBuilder.build()
      val appInfo6 = appInfoBuilder.build()

      val jsonAppInfos = """{"g" : {}, "h" : {"numBlackApps": 10}}"""

      Evaluator.insertAll(record, ast, java.util.Arrays.asList(("d", appInfo4), ("e", appInfo5), ("f", appInfo6)))
      Evaluator.insertAllJson(record, ast, jsonAppInfos)
      val res0 = Evaluator.select(record, ast)
      s"insertAll |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        assertResult(Set("devApps"))(res0.map(_.topLevelField.name).toSet)
        assertResult(8)(res0.map(_.value).head.asInstanceOf[java.util.Map[_, _]].size)
      }
    }

    "delete map item" should {
      val record = initAccount()
      val path = ".devApps(\"b\")"
      val ast = new Parser().parse(path)

      Evaluator.delete(record, ast)
      val res0 = Evaluator.select(record, ast)
      s"delete |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        assertResult(Set())(res0.map(_.topLevelField.name).toSet)
        assertResult(Nil)(res0.map(_.value))
      }
    }

    "clean map items" should {
      val record = initAccount()
      val path = ".devApps"
      val ast = new Parser().parse(path)

      Evaluator.clear(record, ast)
      val res0 = Evaluator.select(record, ast)
      s"clear |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        assertResult(Set("devApps"))(res0.map(_.topLevelField.name).toSet)
        assertResult(java.util.Collections.EMPTY_MAP)(res0.map(_.value).head)
      }
    }

  }

  private def selectResultStr(res: List[Ctx]): String = {
    val sb = new StringBuilder()
    sb.append(res.map(_.value).mkString("\n"))
    sb.toString
  }

}
