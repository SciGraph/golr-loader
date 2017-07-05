package org.monarch.golr;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.singleton;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.monarch.golr.beans.Closure;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.prefixcommons.CurieUtil;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;

import io.dropwizard.jackson.Jackson;
import io.scigraph.bbop.BbopGraph;
import io.scigraph.bbop.BbopGraphUtil;
import io.scigraph.internal.GraphAspect;
import io.scigraph.internal.TinkerGraphUtil;
import io.scigraph.owlapi.OwlRelationships;

class EvidenceProcessor {

  private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

  private final GraphDatabaseService graphDb;
  private final GraphAspect aspect;
  private final ClosureUtil closureUtil;
  private final BbopGraphUtil bbopUtil;
  private final CurieUtil curieUtil;

  private String hasEvidence;
  private String source;
  private String isDefinedBy;

  @Inject
  EvidenceProcessor(GraphDatabaseService graphDb, GraphAspect aspect, ClosureUtil closureUtil,
      BbopGraphUtil bbopUtil, CurieUtil curieUtil) {
    this.graphDb = graphDb;
    this.aspect = aspect;
    this.closureUtil = closureUtil;
    this.bbopUtil = bbopUtil;
    this.curieUtil = curieUtil;

    String hasEvidenceStr = "http://purl.obolibrary.org/obo/RO_0002558";
    this.hasEvidence = curieUtil.getCurie(hasEvidenceStr).orElse(hasEvidenceStr);

    String sourceStr = "http://purl.org/dc/elements/1.1/source";
    this.source = curieUtil.getCurie(sourceStr).orElse(sourceStr);

    String isDefinedByStr = OwlRelationships.RDFS_IS_DEFINED_BY.name();
    this.isDefinedBy = curieUtil.getCurie(isDefinedByStr).orElse(isDefinedByStr);
  }

  void addAssociations(Graph graph) {
    aspect.invoke(graph);
  }

  String getEvidenceGraph(Graph graph) {
    return getEvidenceGraph(graph, Optional.empty());
  }

  String getEvidenceGraph(Graph graph, Optional<String> metaSourceQuery) {
    TinkerGraphUtil tgu = new TinkerGraphUtil(graph, curieUtil);
    tgu.project(singleton("label"));
    BbopGraph bbopGraph = bbopUtil.convertGraph(tgu.getGraph());
    if (metaSourceQuery.isPresent()) {
      Map<String, Object> currentMeta = bbopGraph.getMeta();
      currentMeta.put("query", "monarch:cypher/" + metaSourceQuery.get());
    }
    StringWriter writer = new StringWriter();
    try {
      MAPPER.writeValue(writer, bbopGraph);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return writer.toString();
  }

  List<Closure> getEvidenceObject(Graph graph, Set<Long> ignoredNodes) {
    List<Closure> closures = new ArrayList<>();
    for (Vertex vertex : graph.getVertices()) {
      if (ignoredNodes.contains(Long.parseLong((String) vertex.getId()))) {
        continue;
      }
      Node node = graphDb.getNodeById(Long.parseLong((String) vertex.getId()));
      closures.add(closureUtil.getClosure(node, SolrDocUtil.DEFAULT_CLOSURE_TYPES));
    }
    return closures;
  }

  List<Closure> getEvidence(Graph graph) {
    List<Closure> closures = new ArrayList<>();
    for (Edge edge : graph.getEdges()) {
      if (hasEvidence.equals(edge.getLabel())) {
        Vertex vertex = edge.getVertex(Direction.IN);
        Node node = graphDb.getNodeById(Long.parseLong((String) vertex.getId()));
        closures.add(closureUtil.getClosure(node, SolrDocUtil.DEFAULT_CLOSURE_TYPES));
      }
    }
    return closures;
  }

  List<Closure> getSource(Graph graph) {
    List<Closure> closures = new ArrayList<>();
    for (Edge edge : graph.getEdges()) {
      if (source.equals(edge.getLabel())) {
        Vertex vertex = edge.getVertex(Direction.IN);
        Node node = graphDb.getNodeById(Long.parseLong((String) vertex.getId()));
        closures.add(closureUtil.getClosure(node, SolrDocUtil.DEFAULT_CLOSURE_TYPES));
      }
    }
    return closures;
  }

  List<String> getDefinedBys(Graph graph) {
    Set<String> definedBys = new HashSet<>();
    for (Edge edge : graph.getEdges()) {
      definedBys.addAll(TinkerGraphUtil.getProperties(edge, isDefinedBy, String.class));
    }
    return newArrayList(definedBys);
  }

}
