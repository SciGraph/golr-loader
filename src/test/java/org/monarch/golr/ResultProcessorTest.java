package org.monarch.golr;

import java.io.StringWriter;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Result;

public class ResultProcessorTest extends GolrLoadSetup {

  ResultProcessor processor;
  StringWriter writer = new StringWriter();
  
  @Before
  public void setup() {
    processor = new ResultProcessor(new ResultSerializerFactoryTestImpl());
  }

  @Test
  public void test() throws Exception {
    String query = "MATCH (subClass)-[:subClassOf]->(superClass) RETURN subClass, superClass";
    Result result = graphDb.execute(query);
    processor.process(result, writer, CypherProcessor.getProjection(query));
    System.out.println(writer.toString());
    
  }

}
