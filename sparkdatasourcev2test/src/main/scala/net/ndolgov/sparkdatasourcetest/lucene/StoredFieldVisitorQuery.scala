package net.ndolgov.sparkdatasourcetest.lucene

import java.io.IOException

import org.apache.lucene.index.{LeafReaderContext, StoredFieldVisitor}
import org.apache.lucene.queries.{CustomScoreProvider, CustomScoreQuery}
import org.apache.lucene.search.Query

/**
  * Custom Lucene query using a visitor to collect values from matching Lucene documents
  */
final class StoredFieldVisitorQuery(val subQuery: Query, val processor : LuceneDocumentProcessor) extends CustomScoreQuery(subQuery) {
  @throws[IOException]
  override def getCustomScoreProvider(context : LeafReaderContext) : CustomScoreProvider = {
    new Provider(context, processor)
  }
}

trait LuceneDocumentProcessor {
  /** @return Lucene field visitor to apply to all matching documents */
  def visitor() : StoredFieldVisitor

  /** Process Lucene document fields gathered by the [[visitor]] from the last seen document */
  def onDocument()
}

@Override
private final class Provider(val leafCtx : LeafReaderContext, val processor : LuceneDocumentProcessor) extends CustomScoreProvider(leafCtx) {
  val DEFAULT_SCORE: Int = 0
  val visitor : StoredFieldVisitor = processor.visitor()

  override def customScore(docId: Int, subQueryScore: Float, valSrcScore: Float): Float = {
    leafCtx.reader().document(docId, visitor)
    processor.onDocument()

    DEFAULT_SCORE
  }
}
