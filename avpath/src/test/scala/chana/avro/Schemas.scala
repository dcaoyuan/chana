package chana.avro

import org.apache.avro.Schema.Parser

object Schemas {
  val REGISTER_TIME = "registerTime"
  val LAST_LOGIN_TIME = "lastLoginTime"
  val LOGIN_RECORDS = "loginRecords"
  val TIME = "time"
  val KIND = "kind"
  val ACTIVE_RECORDS = "activityRecords"
  val CHARGE_RECORDS = "chargeRecords"
  val AMOUNT = "amount"
  val BALANCE = "balance"
  val NUM_FRIENDS = "numFriends"
  val NUM_CONTACTS = "numContacts"
  val NUM_PLAYED_GAMES = "numPlayedGames"

  private lazy val classLoader = this.getClass.getClassLoader

  private val parser = new Parser()
  private val union = parser.parse(classLoader.getResourceAsStream("avsc/Record.avsc"))
  val actionRecord = union.getTypes.get(union.getIndexNamed("chana.avro.test.ActionRecord"))
  val actionInfo = union.getTypes.get(union.getIndexNamed("chana.avro.test.ActionInfo"))

  val appInfo = union.getTypes.get(union.getIndexNamed("chana.avro.test.AppInfo"))

  val loginRecord = union.getTypes.get(union.getIndexNamed("chana.avro.test.LoginRecord"))
  val chargeRecord = union.getTypes.get(union.getIndexNamed("chana.avro.test.ChargeRecord"))

  val account = union.getTypes.get(union.getIndexNamed("chana.avro.test.Account"))

  lazy val map = Map(
    actionRecord.getFullName -> actionRecord,
    actionInfo.getFullName -> actionInfo,
    appInfo.getFullName -> appInfo,
    loginRecord.getFullName -> loginRecord,
    chargeRecord.getFullName -> chargeRecord,
    account.getFullName -> account)

  def apply(name: String) = map(name)
}
