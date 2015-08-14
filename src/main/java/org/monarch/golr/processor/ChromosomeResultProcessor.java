package org.monarch.golr.processor;

import io.scigraph.cache.Cacheable;
import io.scigraph.neo4j.DirectedRelationshipType;
import io.scigraph.owlapi.OwlRelationships;

import java.util.Collection;
import java.util.HashSet;

import javax.inject.Inject;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.TraversalDescription;

import com.google.common.base.Optional;

public class ChromosomeResultProcessor extends ResultProcessor {

  private static final  String CHROMOSOME_TYPE = "http://purl.obolibrary.org/obo/SO_0000340";

  private static final  RelationshipType location = DynamicRelationshipType.withName("location");
  private static final  RelationshipType begin = DynamicRelationshipType.withName("begin");
  private static final  RelationshipType reference = DynamicRelationshipType.withName("reference");

  private TraversalDescription chromosomeDescription;
  private Collection<Node> chromsomeEntailment;

  @Inject
  ChromosomeResultProcessor() {
    chromosomeDescription = graphDb.traversalDescription().depthFirst()
        .relationships(OwlRelationships.OWL_EQUIVALENT_CLASS, Direction.BOTH)
        .relationships(OwlRelationships.OWL_SAME_AS, Direction.BOTH)
        .relationships(OwlRelationships.RDFS_SUBCLASS_OF, Direction.OUTGOING)
        .relationships(OwlRelationships.RDF_TYPE, Direction.OUTGOING)
        .relationships(location, Direction.OUTGOING).relationships(begin, Direction.OUTGOING)
        .relationships(reference, Direction.OUTGOING);

    Optional<Long> nodeId = graph.getNode(CHROMOSOME_TYPE);
    if (!nodeId.isPresent()) {
      // TODO: Move all of this to some external configuration
      return;
    }
    Node chromsomeParent = graphDb.getNodeById(nodeId.get());

    chromsomeEntailment = api.getEntailment(chromsomeParent, new DirectedRelationshipType(
        OwlRelationships.RDFS_SUBCLASS_OF, Direction.INCOMING), true);

  }

  @Cacheable
  @Override
  public Collection<Node> produceField(PropertyContainer container) {
    Node startNode = (Node)container;
    Collection<Node> chromosomes = new HashSet<>();
    for (Path path : chromosomeDescription.traverse(startNode)) {
      if (path.length() > 0 && path.lastRelationship().isType(OwlRelationships.RDF_TYPE)) {
        if (chromsomeEntailment.contains(path.endNode())) {
          chromosomes.add(path.lastRelationship().getOtherNode(path.endNode()));
        }
      }
    }
    return chromosomes;
  }

}
