package org.monarch;

import javax.inject.Inject;

import org.monarch.beans.Closure;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;

import com.google.common.base.Optional;

import edu.sdsc.scigraph.frames.CommonProperties;
import edu.sdsc.scigraph.frames.NodeProperties;
import edu.sdsc.scigraph.neo4j.DirectedRelationshipType;
import edu.sdsc.scigraph.neo4j.GraphUtil;
import edu.sdsc.scigraph.owlapi.curies.CurieUtil;

public class ClosureUtil {

  private final GraphDatabaseService graphDb;
  private final CurieUtil curieUtil;

  @Inject
  ClosureUtil(GraphDatabaseService graphDb, CurieUtil curieUtil) {
    this.graphDb = graphDb;
    this.curieUtil = curieUtil;
  }

  public Closure getClosure(Node start, DirectedRelationshipType type) {
    Closure closure = new Closure();
    TraversalDescription description = graphDb.traversalDescription().depthFirst().uniqueness(Uniqueness.NODE_GLOBAL);
    description = description.relationships(type.getType(), type.getDirection());
    for (Path path: description.traverse(start)) {
      Node endNode = path.endNode();
      Optional<String> curie = curieUtil.getCurie((String)endNode.getProperty(CommonProperties.URI));
      if (curie.isPresent()) {
        closure.getCuries().add(curie.get());
      }
      closure.getLabels().addAll(GraphUtil.getProperties(endNode, NodeProperties.LABEL, String.class));
    }
    return closure;
  }

}
