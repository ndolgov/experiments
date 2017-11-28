package net.ndolgov.querydsl.fastparse

import net.ndolgov.querydsl.ast.{DslQuery, From, Projection, Select, Where}
import net.ndolgov.querydsl.ast.expression.BinaryExpr.Op
import net.ndolgov.querydsl.ast.expression.{AttrEqLong, BinaryExpr, PredicateExpr}
import net.ndolgov.querydsl.parser.Tokens.{AND, ASTERISK, EQUALS, FROM, LPAREN, OR, RPAREN, SELECT, WHERE}

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.util.{Left, Right, Either}

/** Actual fastparse parsers combined using mostly the same structure as CFG BNF quoted in comments */
private final class FastparseParser {
  // a magical incantation that allows to automatically skip whitespaces
  private val White = fastparse.WhitespaceApi.Wrapper{
    import fastparse.all._
    NoTrace(" ".rep)
  }
  import fastparse.noApi._
  import White._

  private val lparenStr = String.valueOf(LPAREN)
  private val rparenStr = String.valueOf(RPAREN)
  private val asteriskStr = String.valueOf(ASTERISK)

  private val digit: P[Unit] = P(CharIn("0123456789"))
  private val longNumber: P[Long] = P(digit.rep(min = 1)).!.map(_.toLong)

  private val stringChars: P[String] = P(CharsWhile(c => c != '\"' && c != '\'' && c != '\\')).!
  private val quotedString : P[String] = P("\'" ~ stringChars.rep(min = 1).! ~ "\'")

  // 'ATTR_NAME' = LONG
  private val attributeEqLong: P[PredicateExpr] =
    P(quotedString ~ EQUALS ~ longNumber).
      map((t: (String, Long)) => new AttrEqLong(t._1, t._2))

  // CONDITION = 'attr' = LONG | (CONDITION && CONDITION) | (CONDITION || CONDITION)
  private val condition: P[PredicateExpr] =
    attributeEqLong |
    (nestedCondition ~ AND ~ nestedCondition).
      map((lr: (PredicateExpr, PredicateExpr)) => new BinaryExpr(lr._1, lr._2, Op.AND)) |
    (nestedCondition ~ OR ~ nestedCondition).
      map((lr: (PredicateExpr, PredicateExpr)) => new BinaryExpr(lr._1, lr._2, Op.OR))

  // (NESTED CONDITION)
  private lazy val nestedCondition: P[PredicateExpr] = P(lparenStr ~ condition ~ rparenStr)

  // WHERE CONDITION
  private val where: P[Where] =
    P(WHERE ~ condition).
      map((expr: PredicateExpr) => new Where(expr))

  // FROM 'abs-file-path'
  private val from: P[From] =
    P(FROM ~ quotedString).
      map((path: String) => new From(path))

  // '*'
  private val asterisk: P[Seq[Projection]] = P(asteriskStr).map(_ => Seq())

  // METRICS = METRIC_ID (, METRIC_ID)+
  private val metricIds: P[Seq[Projection]] =
    P(longNumber.rep(sep = ",", min = 1)).
      map((ids: Seq[Long]) => ids.map((id: Long) => new Projection(id)).toList)

  // SELECT '*' | METRICS
  private val select: P[Select] =
    P(SELECT ~ (asterisk | metricIds)).
      map((projections: Seq[Projection]) => new Select(projections.asJava))

  // SELECT FROM WHERE
  private val query: P[DslQuery] =
    P(select ~ from ~ where).
      map((sfw: (Select, From, Where)) => new DslQuery(sfw._1, sfw._2, sfw._3))

  /**
    * @param queryStr query to parse
    * @return AST built from successfully parsed query string or a data structure that can help with debugging parsing errors
    */
  def parse(queryStr: String) : Either[Parsed.Failure, DslQuery] = {
    query.parse(queryStr) match {
      case s: Parsed.Success[DslQuery] => Right(s.value)
      case f: Parsed.Failure => Left(f)
    }
  }
}
