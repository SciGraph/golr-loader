package org.monarch.golr;

import io.scigraph.internal.CypherUtil;
import io.scigraph.internal.GraphApi;
import io.scigraph.internal.TinkerGraphUtil;
import io.scigraph.neo4j.DirectedRelationshipType;

import static org.junit.Assert.assertEquals;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.monarch.golr.beans.GolrCypherQuery;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

public class GolrLoaderTest extends GolrLoadSetup {

  GolrLoader processor;
  StringWriter writer = new StringWriter();

  @Before
  public void setup() {
    EvidenceProcessorStub stub = new EvidenceProcessorStub(graphDb, new EvidenceAspectStub(), closureUtil, curieUtil);
    CypherUtil cypherUtil = new CypherUtil(graphDb, curieUtil);
    processor =
        new GolrLoader(graphDb, graph, new CypherUtil(graphDb, curieUtil), curieUtil, 
            stub, new GraphApi(graphDb, cypherUtil, curieUtil));  }

  @Ignore
  @Test
  public void primitiveTypesSerialize() throws Exception {
    GolrCypherQuery query = new GolrCypherQuery("RETURN 'foo' as string, true as boolean, 1 as int, 1 as long, 1 as float, 1 as double");
    List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
    results = TestUtils.getResultList(query);
    
    JSONAssert.assertEquals(getFixture("fixtures/primitives.json"), StringUtils.strip(writer.toString(), "[]"), JSONCompareMode.NON_EXTENSIBLE);
  }

  @Ignore
  @Test
  public void defaultClosuresSerialize() throws Exception {
    GolrCypherQuery query = new GolrCypherQuery("MATCH (thing)-[:CAUSES]->(otherThing) RETURN *");
    List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
    results = TestUtils.getResultList(query);
    JSONAssert.assertEquals(getFixture("fixtures/simpleResult.json"), writer.toString(), JSONCompareMode.NON_EXTENSIBLE);
  }

  @Ignore
  @Test
  public void relationshipClosureSerialization() throws Exception {
    GolrCypherQuery query = new GolrCypherQuery("MATCH ()-[relationship:CAUSES]->() RETURN *");
    List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
    results = TestUtils.getResultList(query);
    System.out.println(getFixture("fixtures/relationshipResult.json"));
    System.out.println(writer.toString());
    JSONAssert.assertEquals(getFixture("fixtures/relationshipResult.json"), writer.toString(), JSONCompareMode.NON_EXTENSIBLE);
  }

  @Ignore
  @Test
  public void customClosuresSerialize() throws Exception {
    GolrCypherQuery query = new GolrCypherQuery("MATCH (thing)-[:CAUSES]->(otherThing) RETURN *");
    query.getTypes().put("otherThing", new DirectedRelationshipType("partOf", "OUTGOING"));
    List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
    results = TestUtils.getResultList(query);
    JSONAssert.assertEquals(getFixture("fixtures/customClosureTypeResult.json"), writer.toString(), JSONCompareMode.NON_EXTENSIBLE);
  }

  @Test
  public void customClosureQuery() throws Exception {
    /* TODO this is a hacky way to compare the generated solr document
     * with an expected JSON, it would be better to use 
     * an EmbeddedSolrServer for testing
     */
    GolrCypherQuery query = new GolrCypherQuery("MATCH path=(subject:gene)-[relation:`http://purl.obolibrary.org/obo/RO_0002206`]->(object:`anatomical entity`) RETURN DISTINCT path, subject, object, 'gene' AS subject_category, 'anatomy' AS object_category, 'direct' AS qualifier");
    query.setObjectClosure("rdfs:subClassOf|http://purl.obolibrary.org/obo/BFO_0000050");
    List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
    results = TestUtils.getResultList(query);
    TinkerGraphUtil tguEvidenceGraph = new TinkerGraphUtil(curieUtil);
    SolrInputDocument solrDoc = processor.serializerRow(results.get(0), tguEvidenceGraph, new HashSet<>(), query);
    SolrInputDocument testDoc = TestUtils.mapJSONToSolrDocument(getFixture("fixtures/customClosureQuery.json"));
    assertEquals(testDoc.entrySet().toString(), solrDoc.entrySet().toString());
  }

}
