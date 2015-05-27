package org.monarch.golr;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.monarch.golr.beans.Closure;
import org.neo4j.graphdb.GraphDatabaseService;

import com.tinkerpop.blueprints.Graph;

import edu.sdsc.scigraph.internal.GraphAspect;

public class EvidenceProcessorStub extends EvidenceProcessor {

  EvidenceProcessorStub(GraphDatabaseService graphDb, GraphAspect aspect, ClosureUtil closureUtil) {
    super(graphDb, aspect, closureUtil);
  }

  @Override
  void addAssociations(Graph graph) {
  }

  @Override
  String getEvidenceGraph(Graph graph) {
    return "foo";
  }

  @Override
  Collection<Closure> getEvidenceObject(Graph graph, Set<Long> ignoredNodes) {
    return Collections.emptySet();
  }

}
