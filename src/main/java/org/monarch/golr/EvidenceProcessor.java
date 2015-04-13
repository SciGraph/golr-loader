package org.monarch.golr;

import static com.google.common.collect.Iterables.getFirst;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.monarch.golr.beans.Closure;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import com.google.common.base.Charsets;
import com.google.inject.Inject;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONWriter;

import edu.sdsc.scigraph.frames.CommonProperties;
import edu.sdsc.scigraph.frames.NodeProperties;
import edu.sdsc.scigraph.internal.GraphAspect;
import edu.sdsc.scigraph.internal.TinkerGraphUtil;
import edu.sdsc.scigraph.owlapi.curies.CurieUtil;

class EvidenceProcessor {

  private final GraphDatabaseService graphDb;
  private final GraphAspect aspect;
  private final ClosureUtil closureUtil;
  private final CurieUtil curieUtil;

  @Inject
  EvidenceProcessor(GraphDatabaseService graphDb, GraphAspect aspect, ClosureUtil closureUtil, CurieUtil curieUtil) {
    this.graphDb = graphDb;
    this.aspect = aspect;
    this.closureUtil = closureUtil;
    this.curieUtil = curieUtil;
  }

  void addAssociations(Graph graph) {
    aspect.invoke(graph);
  }

  String getEvidenceGraph(Graph graph) {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    try {
      GraphSONWriter.outputGraph(graph, os, GraphSONMode.COMPACT);
    } catch (IOException e) {
      throw new IllegalStateException();
    }
    return new String(os.toByteArray(), Charsets.UTF_8);
  }

  Closure getEvidenceIds(Graph graph, Set<Long> ignoredNodes) {
    Closure closure = new Closure();
    for (Vertex vertex: graph.getVertices()) {
      if (ignoredNodes.contains(Long.parseLong((String)vertex.getId()))) {
        continue;
      }
      String iri = vertex.getProperty(CommonProperties.URI);
      closure.getCuries().add(curieUtil.getCurie(iri).or(iri));
      Collection<String> possibleLabels = TinkerGraphUtil.getProperties(vertex, NodeProperties.LABEL, String.class);
      if (!possibleLabels.isEmpty()) {
        closure.getLabels().add(getFirst(possibleLabels, null));
      }
    }
    return closure;
  }

  Closure entailEvidence(Graph graph, Set<Long> ignoredNodes) {
    Closure closures = new Closure();
    for (Vertex vertex: graph.getVertices()) {
      if (ignoredNodes.contains(Long.parseLong((String)vertex.getId()))) {
        continue;
      }
      Node node = graphDb.getNodeById(Long.parseLong((String)vertex.getId()));
      Closure closure = closureUtil.getClosure(node, ResultSerializer.DEFAULT_CLOSURE_TYPES);
      closures.getCuries().addAll(closure.getCuries());
      closures.getLabels().addAll(closure.getLabels());
    }
    return closures;
  }

}
