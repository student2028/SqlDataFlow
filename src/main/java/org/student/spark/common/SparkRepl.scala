package org.student.spark.common

import java.io.PrintWriter
import java.nio.file.{Files, Paths}

import org.apache.spark.repl.SparkILoop

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.Results._

object SparkRepl {

  val rootDir = System.getProperty("java.io.tmpdir");
  val outputDir = Files.createTempDirectory(Paths.get(rootDir), "spark").toFile
  outputDir.deleteOnExit()

  val settings = new Settings()
  settings.processArguments(List("-Yrepl-class-based",
    "-Yrepl-outdir", s"${outputDir.getAbsolutePath}"), true)
  settings.usejavacp.value = true


  val replOut = new PrintWriter(new LogWriter())
  val sparkILoop = new SparkILoop(None, replOut)
  sparkILoop.settings = settings
  sparkILoop.createInterpreter()
  sparkILoop.initializeSpark()

  def interpret(code: String): String = {
    val ret = sparkILoop.interpret(code)
    ret match {
      case Success => "done"
      case Error => "some wrong with your code, please check"
      case _ => "unknown"
    }
  }


}
