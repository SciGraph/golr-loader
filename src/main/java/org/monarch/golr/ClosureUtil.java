package org.monarch.golr;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.getFirst;

import java.util.Collection;

import javax.inject.Inject;

import org.monarch.golr.beans.Closure;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;

import edu.sdsc.scigraph.frames.CommonProperties;
import edu.sdsc.scigraph.frames.NodeProperties;
import edu.sdsc.scigraph.neo4j.DirectedRelationshipType;
import edu.sdsc.scigraph.neo4j.GraphUtil;
import edu.sdsc.scigraph.owlapi.OwlLabels;
import edu.sdsc.scigraph.owlapi.curies.CurieUtil;

class ClosureUtil {

  private final GraphDatabaseService graphDb;
  private final CurieUtil curieUtil;

  @Inject
  ClosureUtil(GraphDatabaseService graphDb, CurieUtil curieUtil) {
    this.graphDb = graphDb;
    this.curieUtil = curieUtil;
  }

  String getCurieOrIri(Node node) {
    String iri = (String)checkNotNull(node).getProperty(CommonProperties.URI);
    return curieUtil.getCurie(iri).or(iri);
  }

  String getLabelOrIri(Node node) {
    return getFirst(GraphUtil.getProperties(node, NodeProperties.LABEL, String.class), getCurieOrIri(node));
  }

  Closure getClosure(Node start, Collection<DirectedRelationshipType> types) {
    Closure closure = new Closure();
    TraversalDescription description = graphDb.traversalDescription().depthFirst().uniqueness(Uniqueness.NODE_GLOBAL);
    for (DirectedRelationshipType type: checkNotNull(types)) {
      description = description.relationships(type.getType(), type.getDirection());
    }
    closure.setCurie(getCurieOrIri(checkNotNull(start)));
    closure.setLabel(getLabelOrIri(start));
    for (Path path: description.traverse(start)) {
      Node endNode = path.endNode();
      if (endNode.hasLabel(OwlLabels.OWL_ANONYMOUS)) {
        continue;
      }
      closure.getCuries().add(getCurieOrIri(endNode));
      closure.getLabels().add(getLabelOrIri(endNode));
    }
    return closure;
  }

}
