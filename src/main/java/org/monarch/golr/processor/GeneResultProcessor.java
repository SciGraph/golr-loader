package org.monarch.golr.processor;

import static com.google.common.collect.Collections2.transform;
import io.scigraph.cache.Cacheable;
import io.scigraph.owlapi.OwlRelationships;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import javax.inject.Inject;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;

import com.google.common.base.Function;

public class GeneResultProcessor extends ResultProcessor {


  private TraversalDescription geneDescription;
  
  private Collection<String> variantStrings;

  @Inject
  GeneResultProcessor() {
    String partOf = curieUtil.getIri("BFO:0000051").get();
    Collection<RelationshipType> partsOf = cypherUtil.getEntailedRelationshipTypes(Collections.singleton(partOf));
    String variantName = curieUtil.getIri("GENO:0000410").get();
    Collection<RelationshipType> variants = cypherUtil.getEntailedRelationshipTypes(Collections.singleton(variantName));
    geneDescription = graphDb.traversalDescription()
        .depthFirst()
        .evaluator(Evaluators.fromDepth(1))
        .relationships(OwlRelationships.OWL_SAME_AS, Direction.BOTH);
    for (RelationshipType part_of : partsOf) {
      geneDescription = geneDescription.relationships(part_of, Direction.OUTGOING);
    }
    for (RelationshipType variant : variants) {
      geneDescription = geneDescription.relationships(variant, Direction.INCOMING);
    }
    
    variantStrings = transform(variants, new Function<RelationshipType, String>() {
      @Override
      public String apply(RelationshipType type) {
        return type.name();
      }
    });
  }

  @Cacheable
  @Override
  public Collection<Node> produceField(PropertyContainer container) {
    Node startNode = (Node)container;
    Collection<Node> genes = new HashSet<>();
    for (Path path : geneDescription.traverse(startNode)) {
      if (variantStrings.contains(path.lastRelationship().getType().name())) {
        genes.add(path.endNode());
      }
    }
    return genes;
  }

}
