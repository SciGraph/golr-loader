package org.monarch.golr;

import static com.google.common.collect.Sets.newHashSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.monarch.golr.beans.Closure;
import org.neo4j.graphdb.Result;

import com.tinkerpop.blueprints.Graph;

import edu.sdsc.scigraph.internal.TinkerGraphUtil;

public class EvidenceProcessorTest extends GolrLoadSetup {

  EvidenceProcessor processor;
  Graph graph;

  @Before
  public void setup() {
    processor = new EvidenceProcessor(graphDb, new EvidenceAspectStub(), closureUtil, curieUtil);
    Result result = graphDb.execute("MATCH (thing)-[c:CAUSES]->(otherThing) RETURN *");
    graph = TinkerGraphUtil.resultToGraph(result);
    processor.addAssociations(graph);
  }

  @Test
  public void testEvidenceIds() {
    Closure closure = processor.getEvidenceIds(graph, newHashSet(3L, 4L));
    assertThat(closure.getCuries(), contains("X:assn"));
    assertThat(closure.getLabels(), contains("assn"));
  }

  //TODO: Fix this
  @Ignore
  @Test
  public void testEvidenceClosures() {
    Closure closures = processor.entailEvidence(graph, newHashSet(3L, 4L));
    assertThat(closures.getCuries(), contains("X:assn", "X:assn_parent"));
  }

}
