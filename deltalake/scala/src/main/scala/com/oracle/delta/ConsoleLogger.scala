//Copyright (c) 2025 Oracle and/or its affiliates.
//The Universal Permissive License (UPL), Version 1.0

package com.oracle.delta

import org.slf4j.{Logger, LoggerFactory, Marker}

class ConsoleLogger(private val underlying: Logger) extends Logger {
  override def getName: String = underlying.getName
  override def isTraceEnabled: Boolean = underlying.isTraceEnabled
  override def isTraceEnabled(marker: Marker): Boolean = underlying.isTraceEnabled(marker)

  // --- TRACE ---

  override def trace(marker: Marker, msg: String): Unit = trace(msg)
  override def trace(marker: Marker, format: String, arg: Any): Unit = trace(format, arg)
  override def trace(format: String, arg: Any): Unit =
    trace(formatMsg(format, Seq(arg)))
  override def trace(msg: String): Unit = {
    logConsole("TRACE", msg)
    underlying.trace(msg)
  }

  override def trace(marker: Marker, format: String, arg1: Any, arg2: Any): Unit = trace(format, arg1, arg2)
  override def trace(format: String, arg1: Any, arg2: Any): Unit =
    trace(formatMsg(format, Seq(arg1, arg2)))
  override def trace(marker: Marker, format: String, argArray: AnyRef*): Unit = trace(format, argArray: _*)
  override def trace(format: String, arguments: AnyRef*): Unit =
    trace(formatMsg(format, arguments))
  override def trace(marker: Marker, msg: String, t: Throwable): Unit = trace(msg, t)
  override def trace(msg: String, t: Throwable): Unit = {
    logConsole("TRACE", s"$msg - ${t.getMessage}")
    underlying.trace(msg, t)
  }

  override def isDebugEnabled: Boolean = underlying.isDebugEnabled
  override def isDebugEnabled(marker: Marker): Boolean = underlying.isDebugEnabled(marker)

  // --- DEBUG ---

  override def debug(marker: Marker, msg: String): Unit = debug(msg)
  override def debug(msg: String): Unit = {
    logConsole("DEBUG", msg)
    underlying.debug(msg)
  }
  override def debug(marker: Marker, format: String, arg: Any): Unit = debug(format, arg)
  override def debug(format: String, arg: Any): Unit =
    debug(formatMsg(format, Seq(arg)))
  override def debug(marker: Marker, format: String, arg1: Any, arg2: Any): Unit = debug(format, arg1, arg2)
  override def debug(format: String, arg1: Any, arg2: Any): Unit =
    debug(formatMsg(format, Seq(arg1, arg2)))
  override def debug(marker: Marker, format: String, arguments: AnyRef*): Unit = debug(format, arguments: _*)
  override def debug(format: String, arguments: AnyRef*): Unit =
    debug(formatMsg(format, arguments))
  override def debug(marker: Marker, msg: String, t: Throwable): Unit = debug(msg, t)
  override def debug(msg: String, t: Throwable): Unit = {
    logConsole("DEBUG", s"$msg - ${t.getMessage}")
    underlying.debug(msg, t)
  }

  override def isInfoEnabled: Boolean = underlying.isInfoEnabled
  override def isInfoEnabled(marker: Marker): Boolean = underlying.isInfoEnabled(marker)

  // --- INFO ---

  override def info(marker: Marker, msg: String): Unit = info(msg)
  override def info(marker: Marker, format: String, arg: Any): Unit = info(format, arg)
  override def info(format: String, arg: Any): Unit =
    info(formatMsg(format, Seq(arg)))
  private def formatMsg(format: String, args: Seq[Any]): String =
    args.foldLeft(format)((msg, arg) => msg.replaceFirst("\\{\\}", arg.toString))
  override def info(msg: String): Unit = {
    logConsole("INFO", msg)
    underlying.info(msg)
  }

  private def logConsole(level: String, msg: String): Unit = println(s"[$level] $msg")
  override def info(marker: Marker, format: String, arg1: Any, arg2: Any): Unit = info(format, arg1, arg2)
  override def info(format: String, arg1: Any, arg2: Any): Unit =
    info(formatMsg(format, Seq(arg1, arg2)))
  override def info(marker: Marker, format: String, arguments: AnyRef*): Unit = info(format, arguments: _*)
  override def info(format: String, arguments: AnyRef*): Unit =
    info(formatMsg(format, arguments))
  override def info(marker: Marker, msg: String, t: Throwable): Unit = info(msg, t)
  override def info(msg: String, t: Throwable): Unit = {
    logConsole("INFO", s"$msg - ${t.getMessage}")
    underlying.info(msg, t)
  }

  // --- WARN ---

  override def isWarnEnabled: Boolean = underlying.isWarnEnabled
  override def isWarnEnabled(marker: Marker): Boolean = underlying.isWarnEnabled(marker)
  override def warn(marker: Marker, msg: String): Unit = warn(msg)
  override def warn(marker: Marker, format: String, arg: Any): Unit = warn(format, arg)
  override def warn(format: String, arg: Any): Unit =
    warn(formatMsg(format, Seq(arg)))
  override def warn(marker: Marker, format: String, arg1: Any, arg2: Any): Unit = warn(format, arg1, arg2)
  override def warn(format: String, arg1: Any, arg2: Any): Unit =
    warn(formatMsg(format, Seq(arg1, arg2)))
  override def warn(msg: String): Unit = {
    logConsole("WARN", msg)
    underlying.warn(msg)
  }

  override def warn(marker: Marker, format: String, arguments: AnyRef*): Unit = warn(format, arguments: _*)
  override def warn(format: String, arguments: AnyRef*): Unit =
    warn(formatMsg(format, arguments))
  override def warn(marker: Marker, msg: String, t: Throwable): Unit = warn(msg, t)
  override def warn(msg: String, t: Throwable): Unit = {
    logConsole("WARN", s"$msg - ${t.getMessage}")
    underlying.warn(msg, t)
  }

  // --- ERROR ---

  override def isErrorEnabled: Boolean = underlying.isErrorEnabled
  override def isErrorEnabled(marker: Marker): Boolean = underlying.isErrorEnabled(marker)
  override def error(marker: Marker, msg: String): Unit = error(msg)
  override def error(marker: Marker, format: String, arg: Any): Unit = error(format, arg)
  override def error(format: String, arg: Any): Unit =
    error(formatMsg(format, Seq(arg)))
  override def error(marker: Marker, format: String, arg1: Any, arg2: Any): Unit = error(format, arg1, arg2)
  override def error(format: String, arg1: Any, arg2: Any): Unit =
    error(formatMsg(format, Seq(arg1, arg2)))
  override def error(marker: Marker, format: String, arguments: AnyRef*): Unit = error(format, arguments: _*)
  override def error(format: String, arguments: AnyRef*): Unit =
    error(formatMsg(format, arguments))
  override def error(msg: String): Unit = {
    logConsole("ERROR", msg)
    underlying.error(msg)
  }

  override def error(marker: Marker, msg: String, t: Throwable): Unit = error(msg, t)
  override def error(msg: String, t: Throwable): Unit = {
    logConsole("ERROR", s"$msg - ${t.getMessage}")
    underlying.error(msg, t)
  }
}

object ConsoleLogger {
  def apply(cls: Class[_]): ConsoleLogger =
    new ConsoleLogger(LoggerFactory.getLogger(cls.getName))
  def apply(name: String): ConsoleLogger =
    new ConsoleLogger(LoggerFactory.getLogger(name))
}