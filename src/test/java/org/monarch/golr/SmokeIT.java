package org.monarch.golr;

import java.io.StringWriter;
import java.util.Map;

import org.junit.Test;
import org.monarch.golr.beans.GolrCypherQuery;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.skyscreamer.jsonassert.JSONAssert;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;

import edu.sdsc.scigraph.neo4j.bindings.IndicatesCurieMapping;

public class SmokeIT extends GolrLoadSetup {

  @Test
  public void smoke() throws Exception {
    Injector i = Guice.createInjector(new GolrLoaderModule(), new AbstractModule() {
      @Override
      protected void configure() {
        bind(GraphDatabaseService.class).toInstance(graphDb);
      }

      @Provides
      @IndicatesCurieMapping
      Map<String, String> getCurieMap() {
        return curieMap;
      }
      
    });
    ResultProcessor processor = i.getInstance(ResultProcessor.class);
    GolrCypherQuery query = new GolrCypherQuery("MATCH (thing)-[:CAUSES]->(otherThing) RETURN *, 'foo' AS bar");
    Result result = graphDb.execute(query.getQuery());
    StringWriter writer = new StringWriter();
    processor.process(query, result, writer);
    JSONAssert.assertEquals(getFixture("fixtures/simpleResult.json"), writer.toString(), false);
  }
  
}
