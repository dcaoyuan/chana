package chana.avpath

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

  "AvPath" when {

    "select/update record itself" should {
      val record = initAccount()
      val path = "."
      val ast = new Parser().parse(path)

      val res0 = Evaluator.select(record, ast)
      s"select |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        res0.map(_.topLevelField).toSet should be(Set(null))
        res0.map(_.value) should be(List(record))
      }

      val record2 = initAccount()
      record2.put("registerTime", 10000L)
      Evaluator.update(record, ast, record2) foreach { _.commit() }
      val res1 = Evaluator.select(record, ast)
      s"update |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res1.map(_.value).mkString("\n"))

        res1.map(_.value) should be(List(record2))
      }

      Evaluator.updateJson(record, ast, ToJson.toJsonString(record2)) foreach { _.commit() }
      val res2 = Evaluator.select(record, ast)
      s"updateJson |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res2.map(_.value).mkString("\n"))

        res2.map(_.value) should be(List(record2))
      }
    }

    "select/update record field" should {
      val record = initAccount()
      val path = ".registerTime"
      val ast = new Parser().parse(path)

      Evaluator.update(record, ast, 1234L) foreach { _.commit() }
      val res0 = Evaluator.select(record, ast)
      s"select |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        res0.map(_.topLevelField).map(_.name).toSet should be(Set("registerTime"))
        res0.map(_.value) should be(List(1234))
      }
    }

    "select/update record multiple fields" should {
      val record = initAccount()
      val path = "(.registerTime| .lastLoginTime)"
      val ast = new Parser().parse(path)

      Evaluator.update(record, ast, 1234L) foreach { _.commit() }
      val res0 = Evaluator.select(record, ast)
      s"select |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        res0.map(_.topLevelField).map(_.name).toSet should be(Set("registerTime", "lastLoginTime"))
        res0.map(_.value) should be(List(1234, 1234))
      }
    }

    "select/update record multiple fields with nonExist field" should {
      val record = initAccount()
      val path = "(.registerTime|.nonExist)"
      val ast = new Parser().parse(path)

      Evaluator.update(record, ast, 1234L) foreach { _.commit() }
      val res0 = Evaluator.select(record, ast)
      s"select |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        res0.map(_.topLevelField).map(_.name).toSet should be(Set("registerTime"))
        res0.map(_.value) should be(List(1234))
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

        res0.map(_.topLevelField.name).toSet should be(Set("chargeRecords"))
        res0.map(_.value) should be(List(1))
      }

      Evaluator.update(record, ast, 1000L) foreach { _.commit() }
      val res1 = Evaluator.select(record, ast)
      s"update |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res1.map(_.value).mkString("\n"))

        res1.map(_.topLevelField.name).toSet should be(Set("chargeRecords"))
        res1.map(_.value) should be(List(1000))
      }

      Evaluator.updateJson(record, ast, "1234") foreach { _.commit() }
      val res2 = Evaluator.select(record, ast)
      s"updateJson |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res2.map(_.value).mkString("\n"))

        res2.map(_.topLevelField.name).toSet should be(Set("chargeRecords"))
        res2.map(_.value) should be(List(1234))
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

        res0.map(_.topLevelField.name).toSet should be(Set("chargeRecords"))
        res0.map(_.value) should be(List(2))
      }

      Evaluator.update(record, ast, 100L) foreach { _.commit() }
      val res1 = Evaluator.select(record, ast)
      s"update |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res1.map(_.value).mkString("\n"))

        res1.map(_.topLevelField.name).toSet should be(Set("chargeRecords"))
        res1.map(_.value) should be(List(100))
      }

      Evaluator.updateJson(record, ast, "1234") foreach { _.commit() }
      val res2 = Evaluator.select(record, ast)
      s"updateJson |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res2.map(_.value).mkString("\n"))

        res2.map(_.topLevelField.name).toSet should be(Set("chargeRecords"))
        res2.map(_.value) should be(List(1234))
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

        res0.map(_.topLevelField.name).toSet should be(Set("chargeRecords"))
        res0.map(_.value) should be(List(1, 2))
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

        res0.map(_.topLevelField.name).toSet should be(Set("chargeRecords"))
        res0.map(_.value) should be(List(1, 2))
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

        res0.map(_.topLevelField.name).toSet should be(Set("chargeRecords"))
        res0.map(_.value) should be(List(1, 2))
      }
    }

    "select with logic AND" should {
      val record = initAccount()
      val path = ".{ .registerTime == 10000 && .chargeRecords[0].time == 1 }"
      val ast = new Parser().parse(path)

      val res0 = Evaluator.select(record, ast)
      s"select |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        res0.map(_.value.asInstanceOf[GenericRecord].get("registerTime")) should be(List(10000))
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

        res0.map(_.value.asInstanceOf[GenericRecord].get("registerTime")) should be(List(10000))
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

      Evaluator.insert(record, ast, chargeRecord3) foreach { _.commit() }
      Evaluator.insertJson(record, ast, jsonChargeRecord4) foreach { _.commit() }
      val res0 = Evaluator.select(record, ast)
      s"insert |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        res0.map(_.topLevelField.name).toSet should be(Set("chargeRecords"))
        res0.map(_.value).head.asInstanceOf[GenericData.Array[GenericData.Record]].toArray.length should be(4)
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

      Evaluator.insertAll(record, ast, java.util.Arrays.asList(chargeRecord3, chargeRecord4)) foreach { _.commit() }
      Evaluator.insertAllJson(record, ast, jsonChargeRecords) foreach { _.commit() }
      val res0 = Evaluator.select(record, ast)
      s"insertAll |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        res0.map(_.topLevelField.name).toSet should be(Set("chargeRecords"))
        res0.map(_.value).head.asInstanceOf[GenericData.Array[GenericData.Record]].toArray.length should be(6)
      }

      val path1 = ".chargeRecords{.time > -1}.time"
      val ast1 = new Parser().parse(path1)
      val res1 = Evaluator.select(record, ast1)
      s"select |${path1}|" in {
        info("AST:\n" + ast1)
        info("Got:\n" + res1.map(_.value).mkString("\n"))

        res1.map(_.topLevelField.name).toSet should be(Set("chargeRecords"))
        res1.map(_.value) should be(List(1, 2, 4))
      }

      val path2 = ".chargeRecords{.time > 3}"
      val ast2 = new Parser().parse(path2)
      val res2 = Evaluator.select(record, ast2)
      s"select |${path2}|" in {
        info("AST:\n" + ast2)
        info("Got:\n" + res2.map(_.value).mkString("\n"))

        res2.map(_.topLevelField.name).toSet should be(Set("chargeRecords"))
        res2.map(_.value) should be(List(chargeRecord3))
      }
    }

    "clear array field" should {
      val record = initAccount()
      val path = ".chargeRecords"
      val ast = new Parser().parse(path)

      Evaluator.clear(record, ast) foreach { _.commit() }
      val res0 = Evaluator.select(record, ast)
      s"clear |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        res0.map(_.topLevelField.name).toSet should be(Set("chargeRecords"))
        res0.map(_.value).head.asInstanceOf[GenericData.Array[GenericData.Record]].toArray.length should be(0)
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

        res0.map(_.topLevelField.name).toSet should be(Set("devApps"))
        // the order of selected map items is not guaranteed due to the implemetation of java.util.Map
        res0.map(_.value.asInstanceOf[Int]).sorted should be(List(1, 2))
      }

      Evaluator.update(record, ast, 100) foreach { _.commit() }
      val res1 = Evaluator.select(record, ast)
      s"update |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res1.map(_.value).mkString("\n"))

        res1.map(_.topLevelField.name).toSet should be(Set("devApps"))
        res1.map(_.value) should be(List(100, 100))
      }

      Evaluator.updateJson(record, ast, "123") foreach { _.commit() }
      val res2 = Evaluator.select(record, ast)
      s"updateJson |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res2.map(_.value).mkString("\n"))

        res2.map(_.topLevelField.name).toSet should be(Set("devApps"))
        res2.map(_.value) should be(List(123, 123))
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

        res0.map(_.topLevelField.name).toSet should be(Set("devApps"))
        // the order of selected map items is not guaranteed due to the implemetation of java.util.Map
        res0.map(_.value.asInstanceOf[Int]).sorted should be(List(1, 2))
      }

      Evaluator.update(record, ast, 100) foreach { _.commit() }
      val res1 = Evaluator.select(record, ast)
      s"update |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res1.map(_.value).mkString("\n"))

        res1.map(_.topLevelField.name).toSet should be(Set("devApps"))
        res1.map(_.value) should be(List(100, 100))
      }

      Evaluator.updateJson(record, ast, "123") foreach { _.commit() }
      val res2 = Evaluator.select(record, ast)
      s"updateJson |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res2.map(_.value).mkString("\n"))

        res2.map(_.topLevelField.name).toSet should be(Set("devApps"))
        res2.map(_.value) should be(List(123, 123))
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

        res0.map(_.topLevelField.name).toSet should be(Set("devApps"))
        // the order of selected map items is not guaranteed due to the implemetation of java.util.Map
        res0.map(_.value.asInstanceOf[Int]).sorted should be(List(3))
      }

      Evaluator.update(record, ast, 100) foreach { _.commit() }
      val res1 = Evaluator.select(record, ast)
      s"update |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res1.map(_.value).mkString("\n"))

        res1.map(_.topLevelField.name).toSet should be(Set("devApps"))
        res1.map(_.value) should be(List(100))
      }

      Evaluator.updateJson(record, ast, "123") foreach { _.commit() }
      val res2 = Evaluator.select(record, ast)
      s"updateJson |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res2.map(_.value).mkString("\n"))

        res2.map(_.topLevelField.name).toSet should be(Set("devApps"))
        res2.map(_.value) should be(List(123))
      }
    }

    "insert to map field" should {
      val record = initAccount()
      val path = ".devApps"
      val ast = new Parser().parse(path)

      val appInfo4 = appInfoBuilder.build()
      val appInfo5 = ToJson.toJsonString(appInfo4)
      val jsonAppInfoKv = """{"e" : {}}"""

      Evaluator.insert(record, ast, ("d", appInfo4)) foreach { _.commit() }
      Evaluator.insertJson(record, ast, jsonAppInfoKv) foreach { _.commit() }
      val res0 = Evaluator.select(record, ast)
      s"insert |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        res0.map(_.topLevelField.name).toSet should be(Set("devApps"))
        res0.map(_.value).head.asInstanceOf[java.util.Map[_, _]].size should be(5)
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

      Evaluator.insertAll(record, ast, java.util.Arrays.asList(("d", appInfo4), ("e", appInfo5), ("f", appInfo6))) foreach { _.commit() }
      Evaluator.insertAllJson(record, ast, jsonAppInfos) foreach { _.commit() }
      val res0 = Evaluator.select(record, ast)
      s"insertAll |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        res0.map(_.topLevelField.name).toSet should be(Set("devApps"))
        res0.map(_.value).head.asInstanceOf[java.util.Map[_, _]].size should be(8)
      }
    }

    "delete map item" should {
      val record = initAccount()
      val path = ".devApps(\"b\")"
      val ast = new Parser().parse(path)

      Evaluator.delete(record, ast) foreach { _.commit() }
      val res0 = Evaluator.select(record, ast)
      s"delete |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        res0.map(_.topLevelField.name).toSet should be(Set())
        res0.map(_.value) should be(Nil)
      }
    }

    "clean map items" should {
      val record = initAccount()
      val path = ".devApps"
      val ast = new Parser().parse(path)

      Evaluator.clear(record, ast) foreach { _.commit() }
      val res0 = Evaluator.select(record, ast)
      s"clear |${path}|" in {
        info("AST:\n" + ast)
        info("Got:\n" + res0.map(_.value).mkString("\n"))

        res0.map(_.topLevelField.name).toSet should be(Set("devApps"))
        res0.map(_.value).head should be(java.util.Collections.EMPTY_MAP)
      }
    }

  }

  private def selectResultStr(res: List[Ctx]): String = {
    val sb = new StringBuilder()
    sb.append(res.map(_.value).mkString("\n"))
    sb.toString
  }

}
