package org.monarch.golr;

import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.solr.common.SolrInputDocument;
import org.hamcrest.core.StringContains;
import org.junit.Before;
import org.junit.Test;
import org.monarch.golr.beans.GolrCypherQuery;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import io.scigraph.internal.CypherUtil;
import io.scigraph.internal.GraphApi;
import io.scigraph.internal.TinkerGraphUtil;

public class OrthologTest extends GolrLoadSetup {

  GolrLoader processor;

  @Before
  public void setup() {
    EvidenceProcessorStub stub =
        new EvidenceProcessorStub(graphDb, new EvidenceAspectStub(), closureUtil, curieUtil);
    CypherUtil cypherUtil = new CypherUtil(graphDb, curieUtil);
    processor =
        new GolrLoader(graphDb, graph, new CypherUtil(graphDb, curieUtil), curieUtil, 
            stub, new GraphApi(graphDb, cypherUtil, curieUtil));
  }

  @Test
  public void orthologs_areReturned() throws ClassNotFoundException, IOException,
      ExecutionException {
    GolrCypherQuery query = new GolrCypherQuery("MATCH (n:gene) RETURN n as subject, n as object");
    SolrInputDocument existingResult = new SolrInputDocument();
    
    try (Transaction tx = graphDb.beginTx()) {

      Result result = cypherUtil.execute(query.getQuery());
      Map<String, Object> row = result.next();
      TinkerGraphUtil tguEvidenceGraph = new TinkerGraphUtil(curieUtil);
      existingResult = processor.serializerRow(row, tguEvidenceGraph, new HashSet<>(), query);
      tx.success();
      
    }
    assertThat(existingResult.toString(),
        StringContains
            .containsString("subject_ortholog_closure=[http://x.org/gene_ortholog]"));
  }
}
