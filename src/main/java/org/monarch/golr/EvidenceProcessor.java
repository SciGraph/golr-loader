package org.monarch.golr;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.monarch.golr.beans.Closure;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import com.google.common.base.Charsets;
import com.google.inject.Inject;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONWriter;

import edu.sdsc.scigraph.internal.GraphAspect;

class EvidenceProcessor {

  private final GraphDatabaseService graphDb;
  private final GraphAspect aspect;
  private final ClosureUtil closureUtil;

  @Inject
  EvidenceProcessor(GraphDatabaseService graphDb, GraphAspect aspect, ClosureUtil closureUtil) {
    this.graphDb = graphDb;
    this.aspect = aspect;
    this.closureUtil = closureUtil;
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

  List<Closure> getEvidenceObject(Graph graph, Set<Long> ignoredNodes) {
    List<Closure> closures = new ArrayList<>();
    for (Vertex vertex: graph.getVertices()) {
      if (ignoredNodes.contains(Long.parseLong((String)vertex.getId()))) {
        continue;
      }
      Node node = graphDb.getNodeById(Long.parseLong((String)vertex.getId()));
      closures.add(closureUtil.getClosure(node, ResultSerializer.DEFAULT_CLOSURE_TYPES));
    }
    return closures;
  }

  List<Closure> getEvidence(Graph graph) {
    List<Closure> closures = new ArrayList<>();
    for (Edge edge: graph.getEdges()) {
      if ("evidence".equals(edge.getLabel())) {
        Vertex vertex = edge.getVertex(Direction.IN);
        Node node = graphDb.getNodeById(Long.parseLong((String)vertex.getId()));
        closures.add(closureUtil.getClosure(node, ResultSerializer.DEFAULT_CLOSURE_TYPES));
      }
    }
    return closures;
  }

  List<Closure> getSource(Graph graph) {
    List<Closure> closures = new ArrayList<>();
    for (Edge edge: graph.getEdges()) {
      if ("source".equals(edge.getLabel())) {
        Vertex vertex = edge.getVertex(Direction.IN);
        Node node = graphDb.getNodeById(Long.parseLong((String)vertex.getId()));
        closures.add(closureUtil.getClosure(node, ResultSerializer.DEFAULT_CLOSURE_TYPES));
      }
    }
    return closures;
  }

}
