package org.monarch.golr;

import static com.google.common.collect.Collections2.transform;
import static java.util.Collections.singleton;
import io.scigraph.frames.CommonProperties;
import io.scigraph.frames.NodeProperties;
import io.scigraph.internal.CypherUtil;
import io.scigraph.internal.GraphApi;
import io.scigraph.internal.TinkerGraphUtil;
import io.scigraph.neo4j.DirectedRelationshipType;
import io.scigraph.neo4j.Graph;
import io.scigraph.neo4j.GraphUtil;
import io.scigraph.owlapi.OwlRelationships;
import io.scigraph.owlapi.curies.CurieUtil;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;

import org.apache.commons.lang3.ClassUtils;
import org.mapdb.DB;
import org.mapdb.DBMaker;
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
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.Resources;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;

public class GolrLoader {

  private static final String EVIDENCE_GRAPH = "evidence_graph";
  private static final String EVIDENCE_FIELD = "evidence";
  private static final String SOURCE_FIELD = "source";
  private static final String EVIDENCE_OBJECT_FIELD = "evidence_object";
  private static final String DEFINED_BY = "is_defined_by";

  private final GraphDatabaseService graphDb;
  private final ResultSerializerFactory factory;
  private final EvidenceProcessor processor;
  private final Graph graph;
  private final CypherUtil cypherUtil;
  private final CurieUtil curieUtil;
  private final GraphApi api;

  private static final RelationshipType inTaxon = DynamicRelationshipType
      .withName("http://purl.obolibrary.org/obo/RO_0002162");
  private static final RelationshipType derivesFrom = DynamicRelationshipType
      .withName("http://purl.obolibrary.org/obo/RO_0001000");
  private static final RelationshipType derivesSeqFromGene = DynamicRelationshipType
      .withName("http://purl.obolibrary.org/obo/GENO_0000639");
  private static final RelationshipType hasGenotype = DynamicRelationshipType
      .withName("http://purl.obolibrary.org/obo/GENO_0000222");
  private static final String CHROMOSOME_TYPE = "http://purl.obolibrary.org/obo/SO_0000340";

  private static final RelationshipType location = DynamicRelationshipType.withName("location");
  private static final RelationshipType begin = DynamicRelationshipType.withName("begin");
  private static final RelationshipType reference = DynamicRelationshipType.withName("reference");

  private static final Label GENE_LABEL = DynamicLabel.label("gene");
  private static final Label VARIANT_LABEL = DynamicLabel.label("sequence feature");
  private static final Label GENOTYPE_LABEL = DynamicLabel.label("genotype");

  private Collection<RelationshipType> parts_of;
  private Collection<RelationshipType> subSequenceOfs;
  private Collection<RelationshipType> variants;

  private TraversalDescription taxonDescription;
  private TraversalDescription chromosomeDescription;
  private TraversalDescription diseaseDescription;
  private TraversalDescription orthologDescription;
  private TraversalDescription phenotypeDescription;
  private Collection<Node> chromsomeEntailment;
  private TraversalDescription geneDescription;
  private Collection<String> variantStrings;

  @Inject
  GolrLoader(GraphDatabaseService graphDb, Graph graph, CypherUtil cypherUtil, CurieUtil curieUtil,
      ResultSerializerFactory factory, EvidenceProcessor processor, GraphApi api) {
    this.graphDb = graphDb;
    this.cypherUtil = cypherUtil;
    this.curieUtil = curieUtil;
    this.graph = graph;
    this.factory = factory;
    this.processor = processor;
    this.api = api;
    try (Transaction tx = graphDb.beginTx()) {
      buildTraversals();
      tx.success();
    }
  }

  private void buildTraversals() {
    parts_of =
        cypherUtil.getEntailedRelationshipTypes(Collections
            .singleton("http://purl.obolibrary.org/obo/BFO_0000051"));
    subSequenceOfs =
        cypherUtil.getEntailedRelationshipTypes(Collections
            .singleton("http://purl.obolibrary.org/obo/RO_0002525"));
    variants =
        cypherUtil.getEntailedRelationshipTypes(Collections
            .singleton("http://purl.obolibrary.org/obo/GENO_0000418"));
    taxonDescription =
        graphDb.traversalDescription().breadthFirst()
            .relationships(OwlRelationships.OWL_EQUIVALENT_CLASS, Direction.BOTH)
            .relationships(OwlRelationships.OWL_SAME_AS, Direction.BOTH)
            .relationships(OwlRelationships.RDFS_SUBCLASS_OF, Direction.OUTGOING)
            .relationships(OwlRelationships.RDF_TYPE, Direction.OUTGOING)
            .relationships(inTaxon, Direction.OUTGOING).uniqueness(Uniqueness.RELATIONSHIP_GLOBAL);
    for (RelationshipType part_of : parts_of) {
      taxonDescription = taxonDescription.relationships(part_of, Direction.OUTGOING);
    }
    for (RelationshipType subSequenceOf : subSequenceOfs) {
      taxonDescription = taxonDescription.relationships(subSequenceOf, Direction.INCOMING);
    }
    for (RelationshipType variant : variants) {
      taxonDescription = taxonDescription.relationships(variant, Direction.OUTGOING);
    }
    taxonDescription = taxonDescription.relationships(hasGenotype, Direction.OUTGOING);
    taxonDescription = taxonDescription.relationships(derivesFrom, Direction.OUTGOING);


    chromosomeDescription =
        graphDb.traversalDescription().breadthFirst()
            .relationships(OwlRelationships.OWL_EQUIVALENT_CLASS, Direction.BOTH)
            .relationships(OwlRelationships.OWL_SAME_AS, Direction.BOTH)
            .relationships(OwlRelationships.RDFS_SUBCLASS_OF, Direction.OUTGOING)
            .relationships(OwlRelationships.RDF_TYPE, Direction.OUTGOING)
            .relationships(location, Direction.OUTGOING).relationships(begin, Direction.OUTGOING)
            .relationships(reference, Direction.OUTGOING);

    orthologDescription =
        graphDb
            .traversalDescription()
            .breadthFirst()
            .relationships(
                DynamicRelationshipType.withName("http://purl.obolibrary.org/obo/RO_HOM0000017"))
            .relationships(
                DynamicRelationshipType.withName("http://purl.obolibrary.org/obo/RO_HOM0000020"))
            .evaluator(Evaluators.toDepth(1));

    Optional<Long> nodeId = graph.getNode(CHROMOSOME_TYPE);
    if (!nodeId.isPresent()) {
      // TODO: Move all of this to some external configuration
      return;
    }
    Node chromsomeParent = graphDb.getNodeById(nodeId.get());

    chromsomeEntailment =
        api.getEntailment(chromsomeParent, new DirectedRelationshipType(
            OwlRelationships.RDFS_SUBCLASS_OF, Direction.INCOMING), true);

    geneDescription =
        graphDb.traversalDescription().depthFirst()
            .relationships(OwlRelationships.OWL_SAME_AS, Direction.BOTH)
            .relationships(OwlRelationships.OWL_EQUIVALENT_CLASS, Direction.BOTH);
    for (RelationshipType part_of : parts_of) {
      geneDescription = geneDescription.relationships(part_of, Direction.OUTGOING);
    }
    for (RelationshipType variant : variants) {
      geneDescription = geneDescription.relationships(variant, Direction.OUTGOING);
    }
    geneDescription = geneDescription.relationships(derivesSeqFromGene, Direction.OUTGOING);
    geneDescription = geneDescription.relationships(hasGenotype, Direction.OUTGOING);
    geneDescription = geneDescription.relationships(derivesFrom, Direction.OUTGOING);


    variantStrings = transform(variants, new Function<RelationshipType, String>() {
      @Override
      public String apply(RelationshipType type) {
        return type.name();
      }
    });

    diseaseDescription =
        graphDb
            .traversalDescription()
            .depthFirst()
            .relationships(
                DynamicRelationshipType.withName("http://purl.obolibrary.org/obo/RO_0002200"),
                Direction.OUTGOING)
            .relationships(
                DynamicRelationshipType.withName("http://purl.obolibrary.org/obo/RO_0002610"),
                Direction.OUTGOING)
            .relationships(
                DynamicRelationshipType.withName("http://purl.obolibrary.org/obo/RO_0002326"),
                Direction.OUTGOING).evaluator(Evaluators.atDepth(1));

    phenotypeDescription =
        graphDb
            .traversalDescription()
            .depthFirst()
            .relationships(
                DynamicRelationshipType.withName("http://purl.obolibrary.org/obo/RO_0002200"),
                Direction.OUTGOING)
            .relationships(
                DynamicRelationshipType.withName("http://purl.obolibrary.org/obo/RO_0002610"),
                Direction.OUTGOING)
            .relationships(
                DynamicRelationshipType.withName("http://purl.obolibrary.org/obo/RO_0002326"),
                Direction.OUTGOING).evaluator(Evaluators.fromDepth(1))
            .evaluator(Evaluators.toDepth(2));

  }

  Optional<Node> getTaxon(Node source) {
    for (Path path : taxonDescription.traverse(source)) {
      if (path.length() > 0 && path.lastRelationship().isType(inTaxon)) {
        return Optional.of(path.endNode());
      }
    }
    return Optional.absent();
  }

  Optional<Node> getChromosome(Node source) {
    for (Path path : chromosomeDescription.traverse(source)) {
      if (path.length() > 0 && path.lastRelationship().isType(OwlRelationships.RDF_TYPE)) {
        if (chromsomeEntailment.contains(path.endNode())) {
          return Optional.of(path.lastRelationship().getOtherNode(path.endNode()));
        }
      }
    }
    return Optional.absent();
  }

  // TODO return array of all found nodes
  // TODO and filter only cliqueLeaders
  Optional<Node> getGene(Node source) {
    for (Path path : geneDescription.traverse(source)) {
      if (path.endNode().hasLabel(GENE_LABEL)) {
        return Optional.of(path.endNode());
      }
    }
    return Optional.absent();
  }

  Collection<Node> getOrthologs(Node source) throws IOException {
    Collection<Node> orthologs = new HashSet<>();
    for (Path path : orthologDescription.traverse(source)) {
      if (path.endNode().hasLabel(GENE_LABEL) && path.endNode() != source) {
        orthologs.add(path.endNode());
      }
    }
    return orthologs;
  }

  Collection<Node> getDiseases(Node source) throws IOException {
    String cypher = Resources.toString(Resources.getResource("disease.cypher"), Charsets.UTF_8);
    Multimap<String, Object> params = HashMultimap.create();
    params.put("id", source.getId());
    Result result = cypherUtil.execute(cypher, params);
    Collection<Node> diseases = new HashSet<>();
    while (result.hasNext()) {
      Map<String, Object> row = result.next();
      diseases.add((Node) row.get("disease"));
    }
    return diseases;
  }

  Collection<Node> getPhenotypes(Node source) throws IOException {
    String cypher = Resources.toString(Resources.getResource("phenotype.cypher"), Charsets.UTF_8);
    Multimap<String, Object> params = HashMultimap.create();
    params.put("id", source.getId());
    Result result = cypherUtil.execute(cypher, params);
    Collection<Node> phenotypes = new HashSet<>();
    while (result.hasNext()) {
      Map<String, Object> row = result.next();
      phenotypes.add((Node) row.get("phenotype"));
    }
    return phenotypes;
  }


  long process(GolrCypherQuery query, Writer writer) throws IOException, ExecutionException,
      ClassNotFoundException {
    return process(query, writer, Optional.absent());
  }

  long process(GolrCypherQuery query, Writer writer, Optional<String> metaSourceQuery)
      throws IOException, ExecutionException, ClassNotFoundException {
    long recordCount = 0;

    LoadingCache<Node, Optional<Node>> taxonCache =
        CacheBuilder.newBuilder().maximumSize(100_000)
            .build(new CacheLoader<Node, Optional<Node>>() {
              @Override
              public Optional<Node> load(Node source) throws Exception {
                return getTaxon(source);
              }
            });

    LoadingCache<Node, Optional<Node>> chromosomeCache =
        CacheBuilder.newBuilder().maximumSize(100_000)
            .build(new CacheLoader<Node, Optional<Node>>() {
              @Override
              public Optional<Node> load(Node source) throws Exception {
                return getChromosome(source);
              }
            });

    LoadingCache<Node, Optional<Node>> geneCache =
        CacheBuilder.newBuilder().maximumSize(100_000)
            .build(new CacheLoader<Node, Optional<Node>>() {
              @Override
              public Optional<Node> load(Node source) throws Exception {
                return getGene(source);
              }
            });

    LoadingCache<Node, Collection<Node>> orthologCache =
        CacheBuilder.newBuilder().maximumSize(100_000)
            .build(new CacheLoader<Node, Collection<Node>>() {
              @Override
              public Collection<Node> load(Node source) throws Exception {
                return getOrthologs(source);
              }
            });

    DB db =
        DBMaker.newTempFileDB().closeOnJvmShutdown().deleteFilesAfterClose().transactionDisable()
            .cacheSize(1000000).make();
    ConcurrentMap<Pair, String> resultsSerializable = db.createHashMap("results").make();
    ConcurrentMap<Pair, EvidenceGraphInfo> resultsGraph = db.createHashMap("graphs").make();

    try (Transaction tx = graphDb.beginTx()) {
      JsonGenerator generator = new JsonFactory().createGenerator(writer);
      ResultSerializer serializer = factory.create(generator);
      generator.writeStartArray();

      Result result = cypherUtil.execute(query.getQuery());

      while (result.hasNext()) {
        recordCount++;

        Map<String, Object> row = result.next();

        String subjectIri = (String) ((Node) row.get("subject")).getProperty(NodeProperties.IRI);
        String objectIri = (String) ((Node) row.get("object")).getProperty(NodeProperties.IRI);

        Pair pair = new Pair(subjectIri, objectIri);

        String existingResult = resultsSerializable.get(pair);
        if (existingResult == null) {
          Set<Long> ignoredNodes = new HashSet<>();
          Writer stringWriter = new StringWriter();
          JsonGenerator stringGenerator = new JsonFactory().createGenerator(stringWriter);
          ResultSerializer stringSerializer = factory.create(stringGenerator);
          boolean emitEvidence = true;
          com.tinkerpop.blueprints.Graph evidenceGraph = new TinkerGraph();

          stringGenerator.writeStartObject();

          for (Entry<String, Object> entry : row.entrySet()) {
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
            } else if (value instanceof Node) {
              ignoredNodes.add(((Node) value).getId());
            }

            if (value instanceof Node) {

              // TODO: Clean this up
              if ("subject".equals(key) || "object".equals(key)) {
                Node node = (Node) value;
                Optional<Node> taxon = taxonCache.get(node);
                if (taxon.isPresent()) {
                  stringSerializer.serialize(key + "_taxon", taxon.get());
                }
                if (node.hasLabel(GENE_LABEL) || node.hasLabel(VARIANT_LABEL)
                    || node.hasLabel(GENOTYPE_LABEL)) {
                  // Attempt to add gene and chromosome for monarch-initiative/monarch-app/#746
                  if (node.hasLabel(GENE_LABEL)) {
                    stringSerializer.serialize(key + "_gene", node);
                  } else {
                    Optional<Node> gene = geneCache.get(node);
                    if (gene.isPresent()) {
                      stringSerializer.serialize(key + "_gene", gene.get());
                    }
                  }

                  Optional<Node> chromosome = chromosomeCache.get(node);
                  if (chromosome.isPresent()) {
                    stringSerializer.serialize(key + "_chromosome", chromosome.get());
                  }
                }
              }

              if ("subject".equals(key)) {
                Collection<Node> orthologs = getOrthologs((Node) value);
                Collection<String> orthologsId = transform(orthologs, new Function<Node, String>() {
                  @Override
                  public String apply(Node node) {
                    String iri =
                        GraphUtil.getProperty(node, NodeProperties.IRI, String.class).get();
                    return curieUtil.getCurie(iri).or(iri);
                  }
                });
                stringSerializer.writeArray("subject_ortholog_closure", new ArrayList<String>(
                    orthologsId));
              }

              if ("feature".equals(key)) {
                // Add disease and phenotype for feature
                stringSerializer.serialize("disease", getDiseases((Node) value));
                stringSerializer.serialize("phenotype", getPhenotypes((Node) value));
              }

              if (query.getCollectedTypes().containsKey(key)) {
                stringSerializer.serialize(key, singleton((Node) value), query.getCollectedTypes()
                    .get(key));
              } else {
                stringSerializer.serialize(key, value);
              }
            } else if (value instanceof Relationship) {
              String objectPropertyIri =
                  GraphUtil.getProperty((Relationship) value, CommonProperties.IRI, String.class)
                      .get();
              Node objectProperty = graphDb.getNodeById(graph.getNode(objectPropertyIri).get());
              stringSerializer.serialize(key, objectProperty);
            } else if (ClassUtils.isPrimitiveOrWrapper(value.getClass()) || value instanceof String) {
              // Serialize primitive types and Strings
              if ((key.equals("subject_category") || key.equals("object_category"))
                  && value.equals("ontology")) {
                emitEvidence = false;
              }
              stringSerializer.serialize(key, value);
            }
          }

          stringGenerator.writeEndObject();
          stringGenerator.close();

          resultsSerializable.put(pair, stringWriter.toString());

          resultsGraph.put(pair, new EvidenceGraphInfo(evidenceGraph, emitEvidence, ignoredNodes));
        } else {
          EvidenceGraphInfo pairGraph = resultsGraph.get(pair);
          com.tinkerpop.blueprints.Graph evidenceGraph =
              EvidenceGraphInfo.toGraph(pairGraph.graphBytes);
          Set<Long> ignoredNodes = pairGraph.ignoredNodes;
          for (Entry<String, Object> entry : row.entrySet()) {
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
            } else if (value instanceof Node) {
              ignoredNodes.add(((Node) value).getId());
            }
          }
          resultsGraph.put(pair, new EvidenceGraphInfo(evidenceGraph, pairGraph.emitEvidence,
              ignoredNodes));
        }

      }

      for (Entry<Pair, String> resultSerializable : resultsSerializable.entrySet()) {
        generator.writeStartObject();

        Pair p = resultSerializable.getKey();
        EvidenceGraphInfo pairGraph = resultsGraph.get(p);

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> existingJson =
            mapper.readValue(resultSerializable.getValue(), Map.class);


        for (Entry<String, Object> entry : existingJson.entrySet()) {
          serializer.serialize(entry.getKey(), entry.getValue());
        }

        if (pairGraph != null) {
          com.tinkerpop.blueprints.Graph evidenceGraph =
              EvidenceGraphInfo.toGraph(pairGraph.graphBytes);
          processor.addAssociations(evidenceGraph);
          serializer.serialize(EVIDENCE_GRAPH,
              processor.getEvidenceGraph(evidenceGraph, metaSourceQuery));

          // TODO: Hackish to remove evidence but the resulting JSON is blooming out of control
          // Don't emit evidence for ontology sources
          if (pairGraph.emitEvidence) {
            List<Closure> evidenceObjectClosure =
                processor.getEvidenceObject(evidenceGraph, pairGraph.ignoredNodes);
            serializer.writeQuint(EVIDENCE_OBJECT_FIELD, evidenceObjectClosure);
            List<Closure> evidenceClosure = processor.getEvidence(evidenceGraph);
            serializer.writeQuint(EVIDENCE_FIELD, evidenceClosure);
            List<Closure> sourceClosure = processor.getSource(evidenceGraph);
            serializer.writeQuint(SOURCE_FIELD, sourceClosure);
            serializer.writeArray(DEFINED_BY, processor.getDefinedBys(evidenceGraph));
          }
        } else {
          System.out.println("No evidence graph");
        }

        generator.writeEndObject();
        generator.writeRaw('\n');
      }

      generator.writeEndArray();
      generator.close();
      tx.success();
    }

    db.close();
    return recordCount;
  }

}
