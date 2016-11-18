package org.monarch.golr;

import static org.hamcrest.MatcherAssert.assertThat;
import io.scigraph.internal.CypherUtil;
import io.scigraph.internal.GraphApi;

import java.io.IOException;
import java.io.StringWriter;
import java.util.concurrent.ExecutionException;

import org.hamcrest.core.StringContains;
import org.junit.Before;
import org.junit.Test;
import org.monarch.golr.beans.GolrCypherQuery;

public class OrthologTest extends GolrLoadSetup {

  GolrLoader processor;
  StringWriter writer = new StringWriter();

  @Before
  public void setup() {
    EvidenceProcessorStub stub =
        new EvidenceProcessorStub(graphDb, new EvidenceAspectStub(), closureUtil);
    CypherUtil cypherUtil = new CypherUtil(graphDb, curieUtil);
    processor =
        new GolrLoader(graphDb, graph, new CypherUtil(graphDb, curieUtil), curieUtil,
            new ResultSerializerFactoryTestImpl(), stub, new GraphApi(graphDb, cypherUtil));
  }

  @Test
  public void orthologs_areReturned() throws ClassNotFoundException, IOException,
      ExecutionException {
    GolrCypherQuery query = new GolrCypherQuery("MATCH (n:gene) RETURN n as subject, n as object");
    processor.process(query, writer);
    assertThat(writer.toString(),
        StringContains
            .containsString("\"subject_ortholog_closure\":[\"http://x.org/gene_ortholog\"]"));
  }
}
