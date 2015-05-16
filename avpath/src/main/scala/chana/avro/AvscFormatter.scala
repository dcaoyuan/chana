package chana.avro

import java.io.File
import org.apache.avro.Schema

object AvscFormatter extends App {
  val usage =
    """
      Usage: AvscFormat filePath
    """

  if (args.length == 0) {
    exitWithMsg(usage)
  }

  def exitWithMsg(msg: String) = {
    println(msg)
    sys.exit(1)
  }

  args match {
    case Array(path) =>
      try {
        val file = new File(path)
        val schema = new Schema.Parser().parse(file)
        println(schema.toString(true))
      } catch {
        case ex: Throwable => exitWithMsg(ex.getMessage)
      }
    case _ =>
      exitWithMsg(usage)
  }
}
