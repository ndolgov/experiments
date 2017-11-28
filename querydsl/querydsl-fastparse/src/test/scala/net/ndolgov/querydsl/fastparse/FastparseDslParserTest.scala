package net.ndolgov.querydsl.fastparse

import net.ndolgov.querydsl.ast.expression.{AttrEqLong, BinaryExpr}
import org.scalatest.{Assertions, FlatSpec}

class FastparseDslParserTest extends FlatSpec with Assertions {
  private val path = "/tmp/target/file.par"

  private val VALUE1 = 42L
  private val VALUE2 = 24L
  private val VALUE3 = 33L

  private val METRIC1 = 123L
  private val METRIC2 = 456L

  private val COL1 = "col1"
  private val COL2 = "col2"
  private val COL3 = "col3"

  "select all" should "be parsed" in {
    val query = s"select * from '$path' where ('$COL1' = $VALUE1) || ('$COL2'=$VALUE2)"
    val dslQuery = FastparseDslParser().parse(query)

    assert(dslQuery.selectNode.projections.isEmpty)

    assert(dslQuery.fromNode.path == path)

    var disjunctive = dslQuery.whereNode.predicate.asInstanceOf[BinaryExpr]
      val left = disjunctive.left.asInstanceOf[AttrEqLong]
      assert("col1" == left.attrName)
      assert(42L == left.value)

      val right = disjunctive.right.asInstanceOf[AttrEqLong]
      assert("col2" == right.attrName)
      assert(24L == right.value)
  }

  "select multiple ids" should "be parsed" in {
    val query = s"select $METRIC1 ,$METRIC2 from  '$path' where ('$COL3'=$VALUE3) && (('$COL1'=$VALUE1) || ('$COL2'=$VALUE2))"

    val dslQuery = FastparseDslParser().parse(query)
    assert(dslQuery.selectNode.projections.size() == 2)
    assert(dslQuery.selectNode.projections.get(0).metricId == 123L)
    assert(dslQuery.selectNode.projections.get(1).metricId == 456L)

    assert(dslQuery.fromNode.path == "/tmp/target/file.par")

    val conjunctive = dslQuery.whereNode.predicate.asInstanceOf[BinaryExpr]
    val leftLeaf = conjunctive.left.asInstanceOf[AttrEqLong]
    assert(leftLeaf.attrName == COL3)
    assert(33L == leftLeaf.value)

    val disjunctive = conjunctive.right.asInstanceOf[BinaryExpr]
    val left = disjunctive.left.asInstanceOf[AttrEqLong]
    assert(left.attrName == COL1)
    assert(42L == left.value)

    val right = disjunctive.right.asInstanceOf[AttrEqLong]
    assert(right.attrName == COL2)
    assert(24L == right.value)

  }

  "select nonsense" should "fail" in {
    val parser = FastparseDslParser()
    assertThrows[RuntimeException](parser.parse("select nonsense from '/tmp/target/file.par'"))
  }
}
