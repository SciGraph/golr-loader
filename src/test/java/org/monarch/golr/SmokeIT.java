package org.monarch.golr;

import io.scigraph.internal.EvidenceAspect;
import io.scigraph.internal.GraphAspect;
import io.scigraph.neo4j.bindings.IndicatesCurieMapping;

import java.io.StringWriter;
import java.util.Map;

import org.junit.Test;
import org.monarch.golr.beans.GolrCypherQuery;
import org.neo4j.graphdb.GraphDatabaseService;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;

public class SmokeIT extends GolrLoadSetup {

  @Test
  public void smoke() throws Exception {
    Injector i = Guice.createInjector(new GolrLoaderModule(), new AbstractModule() {
      @Override
      protected void configure() {
        bind(GraphDatabaseService.class).toInstance(graphDb);
        bind(GraphAspect.class).to(EvidenceAspect.class);
      }

      @Provides
      @IndicatesCurieMapping
      Map<String, String> getCurieMap() {
        return curieMap;
      }
      
    });
    GolrLoader processor = i.getInstance(GolrLoader.class);
    GolrCypherQuery query = new GolrCypherQuery("MATCH (thing)-[c:CAUSES]->(otherThing) RETURN *");
    StringWriter writer = new StringWriter();
    processor.process(query, writer);
    System.out.println(writer);
    //TODO: Find a way to ignore some fields here...
    //JSONAssert.assertEquals(getFixture("fixtures/simpleResult.json"), writer.toString(), false);
  }
  
}
