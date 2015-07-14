package org.monarch.golr;

import static com.google.common.collect.Sets.newHashSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.List;

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
    processor = new EvidenceProcessor(graphDb, new EvidenceAspectStub(), closureUtil);
    Result result = graphDb.execute("MATCH (thing)-[c:CAUSES]->(otherThing) RETURN *");
    graph = TinkerGraphUtil.resultToGraph(result);
    processor.addAssociations(graph);
  }

  @Test
  public void evidence_object() {
    List<Closure> closures = processor.getEvidenceObject(graph, newHashSet(3L, 4L));
    assertThat(ClosureUtil.collectIds(closures), containsInAnyOrder("X:assn", "X:evidence"));
    assertThat(ClosureUtil.collectIdClosure(closures), containsInAnyOrder("X:assn", "X:evidence", "X:assn_parent"));
  }

  @Test
  public void evidence() {
    List<Closure> closures = processor.getEvidence(graph);
    assertThat(ClosureUtil.collectIds(closures), contains("X:evidence"));
    assertThat(ClosureUtil.collectIdClosure(closures), contains("X:evidence"));
  }

  //TODO: Fix this
  @Ignore
  @Test
  public void testEvidenceClosures() {
    //Closure closures = processor.getEvidenceObjectEntailment(graph, newHashSet(3L, 4L));
    //assertThat(closures.getCuries(), contains("X:assn", "X:assn_parent"));
  }

}
