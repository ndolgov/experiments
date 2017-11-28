package net.ndolgov.querydsl.fastparse

import net.ndolgov.querydsl.ast.DslQuery
import net.ndolgov.querydsl.parser.DslParser

/** Fastparse-based DSL parser implementation */
private final class FastparseDslParser(private val parser: FastparseParser) extends DslParser {
  override def parse(query: String): DslQuery = parser.parse(query) match {
    case Right(ast) =>
      ast

    case Left(e) =>
      //print(e.extra.traced.trace.mkString) // for syntax debugging
      throw new RuntimeException(e.msg);
  }
}

object FastparseDslParser {
  def apply() : DslParser = new FastparseDslParser(new FastparseParser())
}
