package org.monarch.golr;

import io.scigraph.internal.CypherUtil;
import io.scigraph.internal.GraphApi;
import io.scigraph.neo4j.DirectedRelationshipType;

import java.io.StringWriter;

import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.monarch.golr.beans.GolrCypherQuery;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

@Ignore
public class GolrLoaderTest extends GolrLoadSetup {

  GolrLoader processor;
  StringWriter writer = new StringWriter();

  @Before
  public void setup() {
    EvidenceProcessorStub stub = new EvidenceProcessorStub(graphDb, new EvidenceAspectStub(), closureUtil);
    CypherUtil cypherUtil = new CypherUtil(graphDb, curieUtil);
    processor = new GolrLoader(graphDb, graph, new CypherUtil(graphDb, curieUtil), curieUtil, new ResultSerializerFactoryTestImpl(), stub, new GraphApi(graphDb, cypherUtil));
  }

  @Test
  public void primitiveTypesSerialize() throws Exception {
    GolrCypherQuery query = new GolrCypherQuery("RETURN 'foo' as string, true as boolean, 1 as int, 1 as long, 1 as float, 1 as double");
    processor.process(query, writer);
    JSONAssert.assertEquals(getFixture("fixtures/primitives.json"), StringUtils.strip(writer.toString(), "[]"), JSONCompareMode.NON_EXTENSIBLE);
  }

  @Test
  public void defaultClosuresSerialize() throws Exception {
    GolrCypherQuery query = new GolrCypherQuery("MATCH (thing)-[:CAUSES]->(otherThing) RETURN *");
    processor.process(query , writer);
    JSONAssert.assertEquals(getFixture("fixtures/simpleResult.json"), writer.toString(), JSONCompareMode.NON_EXTENSIBLE);
  }

  @Test
  public void relationshipClosureSerialization() throws Exception {
    GolrCypherQuery query = new GolrCypherQuery("MATCH ()-[relationship:CAUSES]->() RETURN *");
    processor.process(query, writer);
    System.out.println(getFixture("fixtures/relationshipResult.json"));
    System.out.println(writer.toString());
    JSONAssert.assertEquals(getFixture("fixtures/relationshipResult.json"), writer.toString(), JSONCompareMode.NON_EXTENSIBLE);
  }

  @Test
  public void customClosuresSerialize() throws Exception {
    GolrCypherQuery query = new GolrCypherQuery("MATCH (thing)-[:CAUSES]->(otherThing) RETURN *");
    query.getTypes().put("otherThing", new DirectedRelationshipType("partOf", "OUTGOING"));
    processor.process(query, writer);
    JSONAssert.assertEquals(getFixture("fixtures/customClosureTypeResult.json"), writer.toString(), JSONCompareMode.NON_EXTENSIBLE);
  }

}
