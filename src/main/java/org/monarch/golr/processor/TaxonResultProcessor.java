package org.monarch.golr.processor;

import io.scigraph.cache.Cacheable;
import io.scigraph.owlapi.OwlRelationships;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import javax.inject.Inject;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;

public class TaxonResultProcessor extends ResultProcessor {

  private final RelationshipType inTaxon;

  private TraversalDescription taxonDescription;

  @Inject
  TaxonResultProcessor() {
    String inTaxonIri = curieUtil.getIri("RO:0002162").get();
    inTaxon = DynamicRelationshipType.withName(inTaxonIri);

    this.taxonDescription = graphDb.traversalDescription().evaluator(Evaluators.fromDepth(1));
    String partOf = curieUtil.getIri("BFO:0000051").get();
    Collection<RelationshipType> partsOf = cypherUtil.getEntailedRelationshipTypes(Collections.singleton(partOf));
    String hasPart = curieUtil.getIri("RO:0002525").get();
    Collection<RelationshipType> hasParts = cypherUtil.getEntailedRelationshipTypes(Collections.singleton(hasPart));
    String variant = curieUtil.getIri("GENO:0000410").get();
    Collection<RelationshipType> variants = cypherUtil.getEntailedRelationshipTypes(Collections.singleton(variant));
    taxonDescription =
        graphDb.traversalDescription().depthFirst()
        .relationships(OwlRelationships.OWL_EQUIVALENT_CLASS, Direction.BOTH)
        .relationships(OwlRelationships.OWL_SAME_AS, Direction.BOTH)
        .relationships(OwlRelationships.RDFS_SUBCLASS_OF, Direction.OUTGOING)
        .relationships(OwlRelationships.RDF_TYPE, Direction.OUTGOING)
        .relationships(inTaxon, Direction.OUTGOING);
    for (RelationshipType part_of : partsOf) {
      taxonDescription = taxonDescription.relationships(part_of, Direction.OUTGOING);
    }
    for (RelationshipType part : hasParts) {
      taxonDescription = taxonDescription.relationships(part, Direction.INCOMING);
    }
    for (RelationshipType var : variants) {
      taxonDescription = taxonDescription.relationships(var, Direction.OUTGOING);
    }
  }

  @Cacheable
  @Override
  public Collection<Node> produceField(PropertyContainer container) {
    Node startNode = (Node)container;
    Collection<Node> taxons = new HashSet<>();
    for (Path path: taxonDescription.traverse(startNode)) {
      if (path.lastRelationship().isType(inTaxon)) {
        taxons.add(path.endNode());
      }
    }
    return taxons;
  }

}
