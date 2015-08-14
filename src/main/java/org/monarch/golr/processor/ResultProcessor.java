package org.monarch.golr.processor;

import static java.util.Collections.emptySet;
import io.scigraph.internal.CypherUtil;
import io.scigraph.internal.GraphApi;
import io.scigraph.owlapi.curies.CurieUtil;

import java.util.Collection;

import javax.inject.Inject;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;

public abstract class ResultProcessor {

  @Inject
  GraphDatabaseService graphDb;

  @Inject
  CypherUtil cypherUtil;

  @Inject
  CurieUtil curieUtil;

  @Inject
  io.scigraph.neo4j.Graph graph;

  @Inject
  GraphApi api;

  public Graph produceEvidence(PropertyContainer container) {
    return new TinkerGraph();
  };

  public Graph produceEvidence(Path container) {
    return new TinkerGraph();
  };

  public void mutateEvidence(Graph graph) {};

  public Collection<Node> produceField(PropertyContainer container) {
    return emptySet();
  };

}
