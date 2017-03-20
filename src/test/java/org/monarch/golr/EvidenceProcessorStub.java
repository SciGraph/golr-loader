package org.monarch.golr;

import static java.util.Collections.emptyList;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.monarch.golr.beans.Closure;
import org.neo4j.graphdb.GraphDatabaseService;
import org.prefixcommons.CurieUtil;

import com.tinkerpop.blueprints.Graph;

import io.scigraph.bbop.BbopGraphUtil;
import io.scigraph.internal.GraphAspect;

public class EvidenceProcessorStub extends EvidenceProcessor {

  EvidenceProcessorStub(GraphDatabaseService graphDb, GraphAspect aspect, ClosureUtil closureUtil,
      CurieUtil curieUtil) {
    super(graphDb, aspect, closureUtil, new BbopGraphUtil(curieUtil), curieUtil);
  }

  @Override
  void addAssociations(Graph graph) {}

  @Override
  String getEvidenceGraph(Graph graph) {
    return "foo";
  }

  @Override
  List<Closure> getEvidenceObject(Graph graph, Set<Long> ignoredNodes) {
    return emptyList();
  }

}
