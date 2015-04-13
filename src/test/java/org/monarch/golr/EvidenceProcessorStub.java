package org.monarch.golr;

import java.util.Set;

import org.monarch.golr.beans.Closure;
import org.neo4j.graphdb.GraphDatabaseService;

import com.tinkerpop.blueprints.Graph;

import edu.sdsc.scigraph.internal.GraphAspect;
import edu.sdsc.scigraph.owlapi.curies.CurieUtil;

public class EvidenceProcessorStub extends EvidenceProcessor {

  EvidenceProcessorStub(GraphDatabaseService graphDb, GraphAspect aspect, ClosureUtil closureUtil,
      CurieUtil curieUtil) {
    super(graphDb, aspect, closureUtil, curieUtil);
  }

  @Override
  void addAssociations(Graph graph) {
  }

  @Override
  String getEvidenceGraph(Graph graph) {
    return "foo";
  }

  @Override
  Closure getEvidenceIds(Graph graph, Set<Long> ignoredNodes) {
    return new Closure();
  }

  @Override
  Closure entailEvidence(Graph graph, Set<Long> ignoredNodes) {
    return new Closure();
  }
  
  

}
