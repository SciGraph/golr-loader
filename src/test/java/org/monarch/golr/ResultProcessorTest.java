package org.monarch.golr;

import java.io.StringWriter;

import org.junit.Before;
import org.junit.Test;
import org.monarch.golr.beans.GolrCypherQuery;
import org.neo4j.graphdb.Result;
import org.skyscreamer.jsonassert.JSONAssert;

public class ResultProcessorTest extends GolrLoadSetup {

  ResultProcessor processor;
  StringWriter writer = new StringWriter();

  @Before
  public void setup() {
    processor = new ResultProcessor(new ResultSerializerFactoryTestImpl());
  }

  @Test
  public void allFieldsSerialize() throws Exception {
    GolrCypherQuery query = new GolrCypherQuery("MATCH (start)-[:CAUSES]->(end) RETURN *, 'foo' AS bar");
    query.getProjection().put("start", "thing");
    query.getProjection().put("end", "otherThing");
    Result result = graphDb.execute(query.getQuery());
    processor.process(query, result, writer);
    JSONAssert.assertEquals(getFixture("fixtures/simpleResult.json"), writer.toString(), false);
  }

}
