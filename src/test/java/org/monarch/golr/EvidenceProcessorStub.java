package org.monarch.golr;

import static java.util.Collections.emptyList;
import io.scigraph.internal.GraphAspect;

import java.util.List;
import java.util.Set;

import org.monarch.golr.beans.Closure;
import org.neo4j.graphdb.GraphDatabaseService;

import com.tinkerpop.blueprints.Graph;

public class EvidenceProcessorStub extends EvidenceProcessor {

  EvidenceProcessorStub(GraphDatabaseService graphDb, GraphAspect aspect, ClosureUtil closureUtil) {
    super(graphDb, aspect, closureUtil, null);
  }

  @Override
  void addAssociations(Graph graph) {
  }

  @Override
  String getEvidenceGraph(Graph graph) {
    return "foo";
  }

  @Override
  List<Closure> getEvidenceObject(Graph graph, Set<Long> ignoredNodes) {
    return emptyList();
  }

}
