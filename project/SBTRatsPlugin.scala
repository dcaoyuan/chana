/*
 * This file is part of the sbt-rats plugin.
 * Copyright (c) 2012-2015 Anthony M Sloane, Macquarie University.
 * All rights reserved.
 * Distributed under the New BSD license.
 * See file LICENSE at top of distribution.
 */

import sbt._
import Keys._
import xtc.parser.Rats
import xtc.tree.Node
import xtc.util.Runtime

/**
 * TODO wait for https://bintray.com/inkytonik/sbt-plugins/sbt-rats/view
 * to fix: mkdir() -> mkdirs()
 */
object SBTRatsPluginPatched extends Plugin {

  import ast.Grammar
  import parser.{ LineColPosition, Parser }
  import org.kiama.attribution.Attribution.{ initTree, resetMemo }
  import org.kiama.util.IO.filereader
  import org.kiama.util.Message
  import org.kiama.util.Messaging.sortmessages
  import scala.collection.mutable.ListBuffer
  import scala.language.existentials
  import scala.util.matching.Regex
  import xtc.parser.ParseError

  /**
   * The file that contains the main Rats! module or main syntax
   * definition.
   */
  val ratsMainModule = SettingKey[Option[File]](
    "rats-main-module",
    "The main Rats! module. If not set, use all syntax definitions.")

  /**
   * If true, assume that the Rats!-generated parser is to be used with
   * Scala and use Scala lists for repeated constructs. Otherwise, use
   * the default Rats! pair-based lists.
   */
  val ratsUseScalaLists = SettingKey[Boolean](
    "rats-use-scala-lists",
    "Use Scala lists instead of Rats! pair-based lists for repetitions")

  /**
   * If true, assume that the Rats!-generated parser is to be used with
   * Scala and use Scala lists for repeated constructs. Otherwise, use
   * the default Rats! pair-based lists.
   */
  val ratsUseScalaOptions = SettingKey[Boolean](
    "rats-use-scala-options",
    "Use Scala options instead of Rats!-style possibly-nullable fields for options")

  /**
   * Set the locations of Scala Positional semantic values instead of
   * Rats! locations. Requires the Rats! option `withLocation` to have
   * any effect.
   */
  val ratsUseScalaPositions = SettingKey[Boolean](
    "rats-use-scala-positions",
    "Set the position of any Positional semantic values (requires Rats! withLocation option)")

  /**
   * If a syntax definition is being used, generate a default specification
   * for comments.
   */
  val ratsUseDefaultComments = SettingKey[Boolean](
    "rats-use-default-comments",
    "Use a default definition for comments (syntax mode only)")

  /**
   * If a syntax definition is being used, generate a default specification
   * for layout (i.e., whitespace and comment handling).
   */
  val ratsUseDefaultLayout = SettingKey[Boolean](
    "rats-use-default-layout",
    "Use a default definition for layout (syntax mode only)")

  /**
   * If a syntax definition is being used, generate a default specification
   * for words (i.e., letter sequences, commonly used to match identifiers
   * and the like).
   */
  val ratsUseDefaultWords = SettingKey[Boolean](
    "rats-use-default-words",
    "Use a default definition for words (syntax mode only)")

  /**
   * If a syntax definition is being used, generate definitions for
   * compatible abstract syntax trees as Scala case classes.
   */
  val ratsDefineASTClasses = SettingKey[Boolean](
    "rats-define-ast-classes",
    "Define Scala classes to represent abstract syntax trees (syntax mode only)")

  /**
   * If a syntax definition is being used and AST classes are being generated,
   * also generate definitions for a Kiama-based pretty printer for the AST.
   */
  val ratsDefinePrettyPrinter = SettingKey[Boolean](
    "rats-define-pretty-printer",
    "Define Kiama-based pretty-printer for abstract syntax trees (syntax mode only, requires ratsDefineASTClasses)")

  /**
   * If non-zero, include support in generated components to make it easy to use
   * them with Kiama. Value is the major Kiama version that is being used.
   */
  val ratsUseKiama = SettingKey[Int](
    "rats-use-kiama",
    "Major version of Kiama that should be used (default 0 means don't use Kiama)")

  /**
   * Include support for keyword handling by building a table of all of the
   * keywords from the specification.
   */
  val ratsIncludeKeywordTable = SettingKey[Boolean](
    "rats-include-keyword-table",
    "Add a table containing all keywords in the specification (syntax mode only)")

  /**
   * Include support for parsing binary formats.
   */
  val ratsIncludeBinarySupport = SettingKey[Boolean](
    "rats-include-binary-support",
    "Add extra support for using parsing binary data (syntax mode only)")

  /**
   * Aggregation of all flag settings.
   */
  val ratsFlags = SettingKey[Flags](
    "rats-flags", "All sbt-rats flags")

  /**
   * Run the generators if any of the .rats or .syntax files in the source
   * have changed or the output doesn't exist.
   */
  def runGenerators =
    (ratsFlags, ratsMainModule, scalaSource in Compile, target, sourceManaged in Compile,
      streams) map {
        (flags, main, srcDir, tgtDir, smDir, str) =>
          {

            val cache = str.cacheDirectory

            val cachedFun =
              FileFunction.cached(cache / "sbt-rats", FilesInfo.lastModified,
                FilesInfo.exists) {
                  (inFiles: Set[File]) =>
                    runGeneratorsImpl(flags, main, inFiles, srcDir, tgtDir,
                      smDir, str)
                }

            val inputFiles = (srcDir ** ("*.rats" | "*.syntax")).get.toSet
            cachedFun(inputFiles).toSeq

          }
      }

  /**
   * Run the generator(s). Use Rats! on either the main Rats! file, or on
   * the translation of all syntax definition files, whichever applies.
   */
  def runGeneratorsImpl(flags: Flags, optmain: Option[File], inFiles: Set[File],
                        srcDir: File, tgtDir: File, smDir: File,
                        str: TaskStreams): Set[File] = {

    // Set up output directories
    val genDir = tgtDir / "sbt-rats"
    IO.createDirectory(genDir)
    val outDir = smDir / "sbt-rats"
    IO.createDirectory(outDir)

    // Make an analyser for this run
    val analyser = new Analyser(flags)

    // Make a generator for this run
    val generator = new Generator(analyser)

    // Buffer of generated files
    val generatedFiles = ListBuffer[File]()

    // If some Scala support is requested, generate Scala support file
    if (flags.useScalaLists | flags.useScalaOptions | flags.useScalaPositions) {
      val supportFile = outDir / "sbtrats" / "ParserSupport.scala"
      str.log.info("Rats! generating Scala support file %s".format(
        supportFile))
      generator.generateSupportFile(flags, supportFile)
      generatedFiles.append(supportFile)
    }

    def processSyntaxFile(syntaxFile: File) {
      runSyntaxImpl(flags, syntaxFile, genDir, outDir, str,
        analyser, generator) match {
          case Some((_, mainFile, newFiles)) =>
            generatedFiles.appendAll(newFiles)
            val javaFiles = runRatsImpl(flags, mainFile, false, srcDir,
              genDir, outDir, str)
            generatedFiles.appendAll(javaFiles)
          case None =>
          // Do nothing
        }
    }

    // Check for a main Rats! module
    optmain match {

      case Some(main) =>
        str.log.info("Rats! got a .rats file %s".format(main))
        // Got one, just run Rats! on it and we're done.
        val newFiles = runRatsImpl(flags, main, true, srcDir, genDir,
          outDir, str)
        generatedFiles.appendAll(newFiles)

      case None =>
        // Otherwise, we translate all syntax definitions into their
        // own parser, with optional auxiliary files.

        // Translate each of the syntax files, then run Rats! on result
        // collect generated files
        for (inFile <- inFiles)
          if (inFile.ext == "syntax")
            processSyntaxFile(inFile)

    }

    // Return all of the generated files
    generatedFiles.result().toSet

  }

  /**
   * Convert a syntax definition into Rats! file and other supporting Scala
   * sources. Returns None if something went wrong, otherwise returns a
   * pair of the generated Rats! specification file and a list of other
   * files that were generated.
   */
  def runSyntaxImpl(flags: Flags, main: File, genDir: File, outDir: File,
                    str: TaskStreams, analyser: Analyser,
                    generator: Generator): Option[(File, File, List[File])] = {
    str.log.info("Running Syntax generation on %s, output to %s and %s".format(
      main, genDir, outDir))
    val filename = main.absolutePath
    val reader = filereader(filename)
    val p = new Parser(reader, filename)
    val pr = p.pGrammar(0)
    if (pr.hasValue) {

      // The abstract syntax tree (AST) representing the syntax
      val grammar = p.value(pr).asInstanceOf[Grammar]

      // Check AST for semantic errors
      initTree(grammar)
      val messages = analyser.errors(grammar)

      if (messages.length == 0) {

        // Make a desugarer for this run
        val desugarer = new Desugarer(analyser)

        // No errors, go on to desugaring, translation and generation
        val desugaredGrammar = desugarer.desugar(grammar)
        resetMemo()
        initTree(desugaredGrammar)

        // Make a translator for this run
        val translator = new Translator(analyser)

        // Extract basename and package directories from grammar name
        val basename = grammar.module.last
        val genSubDir = grammar.module.init.foldLeft(genDir)(_ / _)
        val outSubDir = grammar.module.init.foldLeft(outDir)(_ / _)

        // Generate the Rats! specification
        val genFile = genSubDir / s"${basename}.rats"
        str.log.info("Syntax generating Rats! file %s".format(genFile))
        translator.translate(flags, genFile, desugaredGrammar)

        // Buffer of extra generated files
        val extraFiles = ListBuffer[File]()

        // If requested, generate the AST classes
        if (flags.defineASTClasses) {
          val astFile = outSubDir / s"${basename}Syntax.scala"
          str.log.info("Syntax generating AST classes %s".format(astFile))
          generator.generateASTClasses(flags, astFile, grammar)
          extraFiles.append(astFile)
        }

        // If requested, generate the AST classes
        if (flags.defineASTClasses && flags.definePrettyPrinter) {
          val ppFile = outSubDir / s"${basename}PrettyPrinter.scala"
          str.log.info("Syntax generating pretty-printer %s".format(ppFile))
          generator.generatePrettyPrinter(flags, ppFile, grammar)
          extraFiles.append(ppFile)
        }

        Some((outSubDir, genFile, extraFiles.result()))

      } else {

        for (message <- sortmessages(messages))
          str.log.error(formatSemanticError(p, filename, message))
        sys.error("Syntax semantic analysis of %s failed".format(main))

      }

    } else {

      str.log.error(formatParseError(p, pr.parseError))
      sys.error("Syntax parsing %s failed".format(main))

    }
  }

  /**
   * Format a Rats! parser error message according to Scala compiler
   * conventions for better compatibility with error processors (e.g.,
   * editors).  Very similar to `p.format` but omits the column
   * number from the first line since the pointer carries that
   * information, and omits the word "error".
   */
  def formatParseError(p: Parser, error: ParseError): String = {
    val buf = new StringBuilder

    if (error.index == -1)
      buf.append(error.msg)
    else {
      val loc = p.location(error.index)
      buf.append(loc.file)
      buf.append(':')
      buf.append(loc.line)
      buf.append(": ")

      buf.append(error.msg)
      buf.append('\n')

      val line = p.lineAt(error.index)
      buf.append(line)
      buf.append('\n')
      buf.append(" " * (loc.column - 1))
      buf.append("^\n")
    }

    buf.result()
  }

  /**
   * Format a semantc error according to Scala compiler conventions.
   */
  def formatSemanticError(p: Parser, filename: String, message: Message): String = {
    val pos = message.pos.asInstanceOf[LineColPosition]
    s"$filename:${pos.line}: ${message.label}\n${p.lineAt(pos.index)}\n${(" " * (pos.column - 1))}^"
  }

  /**
   * Run Rats! on the `main` file.
   */
  def runRatsImpl(flags: Flags, main: File, isUserMain: Boolean,
                  srcDir: File, genDir: File, outDir: File,
                  str: TaskStreams): Set[File] = {

    // Set up paths and output directories
    val mainPath = main.absolutePath
    val mainDir = main.getParentFile
    val ratsOutDir =
      if (isUserMain) {
        val relFile = main.getParent.drop(srcDir.getPath.length)
        val ratsGenDir = genDir / relFile
        if (ratsGenDir.exists() || ratsGenDir.mkdirs())
          ratsGenDir
        else
          sys.error("Can't create Rats! output dir %s".format(ratsGenDir))
      } else
        main.getParentFile

    /**
     * Version of Rats! runtime so we can customise the error message reporting
     * to be in stanard Scala style.
     */
    class RatsRuntime extends Runtime {

      def loc(n: Node): String = {
        val loc = n.getLocation
        s"${loc.file}:${loc.line}: "
      }

      override def error(msg: String) {
        str.log.error(msg)
        errors = errors + 1
      }

      override def error(msg: String, n: Node) {
        error(loc(n) + msg)
      }

      override def warning(msg: String) {
        str.log.warn(msg)
        warnings = warnings + 1
      }

      override def warning(msg: String, n: Node) {
        warning(loc(n) + msg)
      }

    }

    /**
     * The runtime field of Rats! tool can't be overridden because it's
     * protectted final. So hack in there using reflection so we can
     * customise it.
     */
    def overrideRuntime(rats: RatsRunner): RatsRunner = {
      val toolClass = rats.getClass.getSuperclass.getSuperclass
      val runtimeField = toolClass.getDeclaredField("runtime")
      runtimeField.setAccessible(true)
      runtimeField.set(rats, new RatsRuntime)
      rats
    }

    /**
     * Helper class to enable access to the protected runtime field.
     */
    class RatsRunner extends Rats {
      def getRuntime = runtime
    }

    // Make a customised Rats! runner
    val rats = overrideRuntime(new RatsRunner)

    // Actually run Rats!
    str.log.info("Running Rats! on %s, input from %s and %s, output to %s".format(
      mainPath, mainDir, genDir, ratsOutDir))
    rats.run(Array("-silent", "-no-exit",
      "-in", mainDir.absolutePath,
      "-in", genDir.absolutePath,
      "-out", ratsOutDir.absolutePath,
      mainPath))

    // What happened?
    if (rats.getRuntime.seenError) {

      sys.error("Rats! failed")
      Set.empty

    } else {
      // Get the expected generated file
      val basename = main.getName.takeWhile(_ != '.')
      val basenameext = s"${basename}.java"
      val genFile = ratsOutDir / basenameext

      // If we've got it, process it further
      if (genFile.exists) {
        str.log.info("Rats! generated %s".format(genFile))

        val relFile = genFile.getPath.drop(genDir.getPath.length)
        val outFile = outDir / relFile

        if (flags.useScalaLists || flags.useScalaOptions || flags.useScalaPositions) {

          str.log.info("Rats! transforming %s for Scala into %s".format(
            genFile, outFile))
          transformForScala(flags, genFile, outFile)
          Set(outFile)

        } else {

          str.log.info("Rats! copying %s to %s".format(genFile, outFile))
          IO.copyFile(genFile, outFile, true)
          Set(outFile)

        }
      } else {

        sys.error("Rats!, can't find generated file %s".format(genFile))
        Set.empty

      }
    }
  }

  /**
   * Make the indicated replacements in the given string, returning the result.
   */
  def makeReplacements(contents: String, replacements: List[(Regex, String)]): String =
    replacements.foldLeft(contents) {
      case (s, r) =>
        r._1.replaceAllIn(s, r._2)
    }

  /**
   * Transform the generated file into the output file as per the flag parameters.
   *  - useScalaLists: replace xtc pairs with Scala lists
   *  - useScalaPositions: replace Rats! location code, gen LineColPosition class
   */
  def transformForScala(flags: Flags, genFile: File, outFile: File) {

    def transformPairsToLists(contents: String): String = {
      val pairsToLists =
        List(
          """import xtc\.util\.Pair;""".r ->
            """import xtc.util.Pair;
                          |import scala.collection.immutable.List;
                          |import sbtrats.ParserSupport;
                          |import sbtrats.SList;""".stripMargin,
          """Pair\.empty\(\)""".r ->
            """SList.empty()""",
          """new Pair<.*>\(""".r ->
            """SList.create(""",
          """Pair<Pair(<[^>]+>)>""".r ->
            """List<List$1>""",
          """Pair(<[^>]+>)""".r ->
            """List$1""")
      makeReplacements(contents, pairsToLists)
    }

    def transformNullablesToOptions(contents: String): String = {
      val nullablesToOptions =
        List(
          """import xtc\.util\.Pair;""".r ->
            """import xtc.util.Pair;
                          |import scala.Option;""".stripMargin)
      makeReplacements(contents, nullablesToOptions)
    }

    def transformPositions(contents: String): String = {

      val locatablesToPositionsScala =
        List(
          """import xtc\.tree\.Locatable;""".r ->
            """import scala.util.parsing.input.Positional;
                          |import scala.util.parsing.input.Position;
                          |import sbtrats.LineColPosition;""".stripMargin,
          """Locatable""".r ->
            """Positional""",
          """public final class (\w+) extends ParserBase \{""".r ->
            """
                        |public final class $1 extends ParserBase {
                        |
                        |  /** Set position of a Positional */
                        |  void setLocation(final Positional positional, final int index) {
                        |    if (null != positional) {
                        |      Column c = column(index);
                        |      positional.setPos(new LineColPosition(this, index, c.line, c.column));
                        |    }
                        |  }
                        |""".stripMargin)

      val locatablesToPositionsKiama =
        List(
          """import xtc\.tree\.Locatable;""".r ->
            """import org.kiama.util.Positions;
                          |import scala.util.parsing.input.Position;
                          |import sbtrats.LineColPosition;""".stripMargin,
          """Locatable""".r ->
            """Object""",
          """public final class (\w+) extends ParserBase \{""".r ->
            """
                        |public final class $1 extends ParserBase {
                        |
                        |  /** Set position of an Object */
                        |  void setLocation(final Object object, final int start) {
                        |    if (null != object) {
                        |      Column s = column(start);
                        |      Positions.setStart(object, new LineColPosition(this, start, s.line, s.column));
                        |      int finish = yyCount == 0 ? 0 : yyCount - 1;
                        |      Column f = column(finish);
                        |      Positions.setFinish(object, new LineColPosition(this, finish, f.line, f.column));
                        |    }
                        |  }
                        |
                        |  /** Text corresponding to a positioned value. */
                        |  String textOf(final Object object) {
                        |    if (null == object)
                        |      return null;
                        |    else {
                        |      LineColPosition start = (LineColPosition)Positions.getStart (object);
                        |      LineColPosition finish = (LineColPosition)Positions.getFinish (object);
                        |      return difference(start.index(), finish.index());
                        |    }
                        |  }
                        |""".stripMargin)

      makeReplacements(contents, if (flags.useKiama > 0)
        locatablesToPositionsKiama
      else
        locatablesToPositionsScala)

    }

    def transformMessages(contents: String): String = {
      val removeLevel0InMessage =
        List(
          " level[0-9]+ expected\"".r ->
            " expected\"")
      makeReplacements(contents, removeLevel0InMessage)
    }

    val contents = IO.read(genFile)

    val contents1 =
      if (flags.useScalaLists)
        transformPairsToLists(contents)
      else
        contents

    val contents2 =
      if (flags.useScalaOptions)
        transformNullablesToOptions(contents1)
      else
        contents1

    val contents3 =
      if (flags.useScalaPositions) {
        transformPositions(contents2)
      } else
        contents2

    val contents4 =
      transformMessages(contents3)

    IO.write(outFile, contents4)

  }

  /**
   * Settings for the plugin:
   *  - run Rats! as a source generator
   *  - add the Rats! jar to the dependent libraries
   *  - default values for settings
   *  - group settings together to pass around
   */
  val sbtRatsSettings = Seq(

    sourceGenerators in Compile <+= runGenerators,

    libraryDependencies ++= Seq(
      "xtc" % "rats" % "2.3.1"),

    ratsMainModule := None,

    ratsUseScalaLists := false,

    ratsUseScalaPositions := false,

    ratsUseScalaOptions := false,

    ratsUseDefaultLayout := true,

    ratsUseDefaultWords := true,

    ratsUseDefaultComments := true,

    ratsDefineASTClasses := false,

    ratsDefinePrettyPrinter := false,

    ratsIncludeKeywordTable := true,

    ratsIncludeBinarySupport := false,

    ratsUseKiama := 0,

    ratsFlags <<= (ratsUseScalaLists, ratsUseScalaPositions,
      ratsUseScalaPositions, ratsUseDefaultComments,
      ratsUseDefaultLayout, ratsUseDefaultWords,
      ratsDefineASTClasses, ratsDefinePrettyPrinter,
      ratsIncludeKeywordTable, ratsIncludeBinarySupport,
      ratsUseKiama) {
        (lists, options, posns, comments, layout, words, ast, pp,
        kwtable, binary, kiama) =>
          Flags(lists, options, posns, comments, layout, words, ast, pp,
            kwtable, binary, kiama)
      })

}
