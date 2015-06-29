package org.monarch.golr;

import static com.google.common.collect.Collections2.transform;

import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;

import org.apache.commons.lang3.ClassUtils;
import org.monarch.golr.beans.Closure;
import org.monarch.golr.beans.GolrCypherQuery;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.TraversalDescription;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;

import edu.sdsc.scigraph.frames.CommonProperties;
import edu.sdsc.scigraph.internal.CypherUtil;
import edu.sdsc.scigraph.internal.TinkerGraphUtil;
import edu.sdsc.scigraph.neo4j.Graph;
import edu.sdsc.scigraph.neo4j.GraphUtil;
import edu.sdsc.scigraph.owlapi.OwlRelationships;

public class GolrLoader {

  private static final String EVIDENCE_GRAPH = "evidence_graph";
  private static final String EVIDENCE_FIELD = "evidence";
  private static final String SOURCE_FIELD = "source";
  private static final String EVIDENCE_OBJECT_FIELD = "evidence_object";

  private final GraphDatabaseService graphDb;
  private final ResultSerializerFactory factory;
  private final EvidenceProcessor processor;
  private final Graph graph;
  private final CypherUtil cypherUtil;

  private static final Label GENE_LABEL = DynamicLabel.label("gene");
  private static final Label VARIANT_LABEL = DynamicLabel.label("sequence feature");
  private static final Label GENOTYPE_LABEL = DynamicLabel.label("genotype");

  @Inject
  GolrLoader(GraphDatabaseService graphDb, Graph graph, CypherUtil cypherUtil, ResultSerializerFactory factory, EvidenceProcessor processor) {
    this.graphDb = graphDb;
    this.cypherUtil = cypherUtil;
    this.graph = graph;
    this.factory = factory;
    this.processor = processor;
  }

  static void writeQuad(ResultSerializer serializer, String baseName, Collection<Closure> closures) throws IOException {
    serializer.writeArray(baseName + ResultSerializer.ID_SUFFIX, ClosureUtil.collectIds(closures));
    serializer.writeArray(baseName + ResultSerializer.LABEL_SUFFIX, ClosureUtil.collectLabels(closures));
    serializer.writeArray(baseName + ResultSerializer.ID_CLOSURE_SUFFIX, ClosureUtil.collectIdClosure(closures));
    serializer.writeArray(baseName + ResultSerializer.LABEL_CLOSURE_SUFFIX, ClosureUtil.collectLabelClosure(closures));
  }

  Optional<Node> getTaxon(Node source) {
    RelationshipType inTaxon = DynamicRelationshipType.withName("RO_0002162");

    TraversalDescription description = graphDb.traversalDescription().depthFirst()
        .relationships(OwlRelationships.OWL_EQUIVALENT_CLASS, Direction.BOTH)
        .relationships(OwlRelationships.OWL_SAME_AS, Direction.BOTH)
        .relationships(OwlRelationships.RDFS_SUBCLASS_OF, Direction.OUTGOING)
        .relationships(OwlRelationships.RDF_TYPE, Direction.OUTGOING)
        .relationships(inTaxon, Direction.OUTGOING);
    Collection<RelationshipType> parts_of = cypherUtil.getEntailedRelationshipTypes(Collections.singleton("BFO_0000051"));
    for (RelationshipType part_of: parts_of) {
      description = description.relationships(part_of, Direction.OUTGOING);
    }
    Collection<RelationshipType> hasParts = cypherUtil.getEntailedRelationshipTypes(Collections.singleton("BFO_0000050"));
    for (RelationshipType hasPart: hasParts) {
      description = description.relationships(hasPart, Direction.INCOMING);
    }
    Collection<RelationshipType> variants = cypherUtil.getEntailedRelationshipTypes(Collections.singleton("GENO_0000410"));
    for (RelationshipType variant: variants) {
      description = description.relationships(variant, Direction.OUTGOING);
    }

    for (Path path: description.traverse(source)) {
      if (path.length() > 0 && path.lastRelationship().isType(inTaxon)) {
        return Optional.of(path.endNode());
      }
    }
    return Optional.absent();
  }

  Optional<Node> getChromosome(Node source) {
    final String chromosomeType = "http://purl.obolibrary.org/obo/SO_0000340";
    
    final RelationshipType location = DynamicRelationshipType.withName("location");
    final RelationshipType begin = DynamicRelationshipType.withName("begin");
    final RelationshipType reference = DynamicRelationshipType.withName("reference");

    TraversalDescription description = graphDb.traversalDescription().depthFirst()
        .relationships(OwlRelationships.OWL_EQUIVALENT_CLASS, Direction.BOTH)
        .relationships(OwlRelationships.OWL_SAME_AS, Direction.BOTH)
        .relationships(OwlRelationships.RDFS_SUBCLASS_OF, Direction.OUTGOING)
        .relationships(OwlRelationships.RDF_TYPE, Direction.OUTGOING)
        .relationships(location, Direction.OUTGOING)
        .relationships(begin, Direction.OUTGOING)
        .relationships(reference, Direction.OUTGOING);
        
    Collection<RelationshipType> parts_of = cypherUtil.getEntailedRelationshipTypes(Collections.singleton("BFO_0000051"));
    for (RelationshipType part_of: parts_of) {
      description = description.relationships(part_of, Direction.OUTGOING);
    }
    Collection<RelationshipType> hasParts = cypherUtil.getEntailedRelationshipTypes(Collections.singleton("BFO_0000050"));
    for (RelationshipType hasPart: hasParts) {
      description = description.relationships(hasPart, Direction.INCOMING);
    }
    Collection<RelationshipType> variants = cypherUtil.getEntailedRelationshipTypes(Collections.singleton("GENO_0000410"));
    for (RelationshipType variant: variants) {
      description = description.relationships(variant, Direction.OUTGOING);
    }
    
    /*System.out.println(parts_of);
    System.out.println(hasParts);
    System.out.println(variants);
    */

    for (Path path: description.traverse(source)) {
      /*for (PropertyContainer container: path) {
        
        if (container instanceof Node) {
          System.out.print("(" + container.getProperty("uri") + ")");
        } else {
          System.out.print("-[" + ((Relationship)container).getType().name() + "]-");
        }
      }
      System.out.println();*/
      if (path.length() > 0 && path.lastRelationship().isType(OwlRelationships.RDF_TYPE)) {
        String iri = (String)path.endNode().getProperty(CommonProperties.URI);
        //System.out.println(iri);
        if (iri.equals(chromosomeType)) {
          return Optional.of(path.endNode());
        }
      }
    }
    return Optional.absent();
  }

  Optional<Node> getGene(Node source) {
    TraversalDescription description = graphDb.traversalDescription().depthFirst()
        .relationships(OwlRelationships.OWL_SAME_AS, Direction.BOTH);
    Collection<RelationshipType> parts_of = cypherUtil.getEntailedRelationshipTypes(Collections.singleton("BFO_0000051"));
    for (RelationshipType part_of: parts_of) {
      description = description.relationships(part_of, Direction.OUTGOING);
    }
    Collection<RelationshipType> variants = cypherUtil.getEntailedRelationshipTypes(Collections.singleton("GENO_0000410"));
    for (RelationshipType variant: variants) {
      description = description.relationships(variant, Direction.OUTGOING);
    }

    Collection<String> variantStrings = transform(variants, new Function<RelationshipType, String>() {
      @Override
      public String apply(RelationshipType type) {
        return type.name();
      }
    });
    for (Path path: description.traverse(source)) {
      if (path.length() > 0 && variantStrings.contains(path.lastRelationship().getType().name())) {
        return Optional.of(path.endNode());
      }
    }
    return Optional.absent();
  }

  long process(GolrCypherQuery query, Writer writer) throws IOException, ExecutionException {
    long recordCount = 0;

    LoadingCache<Node, Optional<Node>> taxonCache = CacheBuilder.newBuilder()
        .maximumSize(100_000)
        .build(new CacheLoader<Node, Optional<Node>>() {
          @Override
          public Optional<Node> load(Node source) throws Exception {
            return getTaxon(source);
          }
        });

    LoadingCache<Node, Optional<Node>> chromosomeCache = CacheBuilder.newBuilder()
        .maximumSize(100_000)
        .build(new CacheLoader<Node, Optional<Node>>() {
          @Override
          public Optional<Node> load(Node source) throws Exception {
            return getChromosome(source);
          }
        });

    LoadingCache<Node, Optional<Node>> geneCache = CacheBuilder.newBuilder()
        .maximumSize(100_000)
        .build(new CacheLoader<Node, Optional<Node>>() {
          @Override
          public Optional<Node> load(Node source) throws Exception {
            return getGene(source);
          }
        });

    try (Transaction tx = graphDb.beginTx()) {
      Result result = cypherUtil.execute(query.getQuery());
      JsonGenerator generator = new JsonFactory().createGenerator(writer);
      ResultSerializer serializer = factory.create(generator);
      generator.writeStartArray();
      while (result.hasNext()) {
        recordCount++;
        generator.writeStartObject();
        Map<String, Object> row = result.next();
        com.tinkerpop.blueprints.Graph evidenceGraph = new TinkerGraph();
        Set<Long> ignoredNodes = new HashSet<>();
        for (Entry<String, Object> entry: row.entrySet()) {
          String key = entry.getKey();
          Object value = entry.getValue();

          if (null == value) {
            continue;
          }

          // Add evidence
          if (value instanceof PropertyContainer) {
            TinkerGraphUtil.addElement(evidenceGraph, (PropertyContainer) value);
          } else if (value instanceof Path) {
            TinkerGraphUtil.addPath(evidenceGraph, (Path) value);
          } 

          if (value instanceof Node) {
            ignoredNodes.add(((Node)value).getId());

            // TODO: Clean this up
            if ("subject".equals(key) || 
                "object".equals(key)) {
              Node node = (Node) value;
              Optional<Node> taxon = taxonCache.get(node);
              if (taxon.isPresent()) {
                serializer.serialize(key + "_taxon", taxon.get());
              }
              if (node.hasLabel(GENE_LABEL) ||
                  node.hasLabel(VARIANT_LABEL) ||
                  node.hasLabel(GENOTYPE_LABEL)) {
                // Attempt to add gene and chromosome for monarch-initiative/monarch-app/#746
                if (node.hasLabel(GENE_LABEL)) {
                  serializer.serialize(key + "_gene", node);
                } else {
                  Optional<Node> gene = geneCache.get(node);
                  if (gene.isPresent()) {
                    serializer.serialize(key + "_gene", gene.get());
                  }
                }

                Optional<Node> chromosome = chromosomeCache.get(node);
                if (chromosome.isPresent()) {
                  serializer.serialize(key + "_chromosome", chromosome.get());
                }

              }
            }

            if (query.getCollectedTypes().containsKey(key)) {
              serializer.serialize(key, (Node) value, query.getCollectedTypes().get(key));
            } else {
              serializer.serialize(key, value);
            }
          } else if (value instanceof Relationship) {
            String objectPropertyIri = GraphUtil.getProperty((Relationship) value, CommonProperties.URI, String.class).get();
            Node objectProperty = graphDb.getNodeById(graph.getNode(objectPropertyIri).get());
            serializer.serialize(key, objectProperty);
          } else if (ClassUtils.isPrimitiveOrWrapper(value.getClass()) ||   value instanceof String) {
            // Serialize primitive types and Strings
            serializer.serialize(key, value);
          }
        }
        processor.addAssociations(evidenceGraph);
        serializer.serialize(EVIDENCE_GRAPH, processor.getEvidenceGraph(evidenceGraph));
        Collection<Closure> evidenceObjectClosure = processor.getEvidenceObject(evidenceGraph, ignoredNodes);
        writeQuad(serializer, EVIDENCE_OBJECT_FIELD, evidenceObjectClosure);
        Collection<Closure> evidenceClosure = processor.getEvidence(evidenceGraph);
        writeQuad(serializer, EVIDENCE_FIELD, evidenceClosure);
        Collection<Closure> sourceClosure = processor.getSource(evidenceGraph);
        writeQuad(serializer, SOURCE_FIELD, sourceClosure);
        generator.writeEndObject();
      }
      generator.writeEndArray();
      generator.close();
      tx.success();
    }
    return recordCount;
  }

}
