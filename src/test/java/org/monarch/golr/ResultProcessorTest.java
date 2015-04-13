package org.monarch.golr;

import java.io.StringWriter;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
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
    String query = "MATCH (start)-[:CAUSES]->(end) RETURN start as thing, end as otherThing, 'foo' AS bar";
    Collection<String> originalProjection = CypherProcessor.getProjection(query);
    Result result = graphDb.execute(CypherProcessor.injectWildcard(query));
    processor.process(result, writer, originalProjection);
    JSONAssert.assertEquals(getFixture("fixtures/simpleResult.json"), writer.toString(), false);
  }

}
