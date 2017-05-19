package org.monarch.golr;

import static com.google.common.collect.Collections2.transform;
import static java.util.Collections.singleton;

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
import java.util.Optional;
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
import org.prefixcommons.CurieUtil;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.Resources;

import io.scigraph.frames.CommonProperties;
import io.scigraph.frames.NodeProperties;
import io.scigraph.internal.CypherUtil;
import io.scigraph.internal.GraphApi;
import io.scigraph.internal.TinkerGraphUtil;
import io.scigraph.neo4j.DirectedRelationshipType;
import io.scigraph.neo4j.Graph;
import io.scigraph.neo4j.GraphUtil;
import io.scigraph.owlapi.OwlRelationships;

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

  private static final RelationshipType inTaxon =
      RelationshipType.withName("http://purl.obolibrary.org/obo/RO_0002162");
  private static final RelationshipType derivesFrom =
      RelationshipType.withName("http://purl.obolibrary.org/obo/RO_0001000");
  private static final RelationshipType derivesSeqFromGene =
      RelationshipType.withName("http://purl.obolibrary.org/obo/GENO_0000639");
  private static final RelationshipType hasGenotype =
      RelationshipType.withName("http://purl.obolibrary.org/obo/GENO_0000222");
  private static final String CHROMOSOME_TYPE = "http://purl.obolibrary.org/obo/SO_0000340";

  private static final RelationshipType location = RelationshipType.withName("location");
  private static final RelationshipType begin = RelationshipType.withName("begin");
  private static final RelationshipType reference = RelationshipType.withName("reference");

  private static final Label GENE_LABEL = Label.label("gene");
  private static final Label VARIANT_LABEL = Label.label("sequence feature");
  private static final Label GENOTYPE_LABEL = Label.label("genotype");

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
    parts_of = cypherUtil.getEntailedRelationshipTypes(
        Collections.singleton("http://purl.obolibrary.org/obo/BFO_0000051"));
    subSequenceOfs = cypherUtil.getEntailedRelationshipTypes(
        Collections.singleton("http://purl.obolibrary.org/obo/RO_0002525"));
    variants = cypherUtil.getEntailedRelationshipTypes(
        Collections.singleton("http://purl.obolibrary.org/obo/GENO_0000418"));
    taxonDescription = graphDb.traversalDescription().breadthFirst()
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


    chromosomeDescription = graphDb.traversalDescription().breadthFirst()
        .relationships(OwlRelationships.OWL_EQUIVALENT_CLASS, Direction.BOTH)
        .relationships(OwlRelationships.OWL_SAME_AS, Direction.BOTH)
        .relationships(OwlRelationships.RDFS_SUBCLASS_OF, Direction.OUTGOING)
        .relationships(OwlRelationships.RDF_TYPE, Direction.OUTGOING)
        .relationships(location, Direction.OUTGOING).relationships(begin, Direction.OUTGOING)
        .relationships(reference, Direction.OUTGOING);

    orthologDescription = graphDb.traversalDescription().breadthFirst()
        .relationships(RelationshipType.withName("http://purl.obolibrary.org/obo/RO_HOM0000017"))
        .relationships(RelationshipType.withName("http://purl.obolibrary.org/obo/RO_HOM0000020"))
        .evaluator(Evaluators.toDepth(1));

    Optional<Long> nodeId = graph.getNode(CHROMOSOME_TYPE);
    if (!nodeId.isPresent()) {
      // TODO: Move all of this to some external configuration
      return;
    }
    Node chromsomeParent = graphDb.getNodeById(nodeId.get());

    chromsomeEntailment = api.getEntailment(chromsomeParent,
        new DirectedRelationshipType(OwlRelationships.RDFS_SUBCLASS_OF, Direction.INCOMING), true);

    geneDescription = graphDb.traversalDescription().depthFirst()
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

    diseaseDescription = graphDb.traversalDescription().depthFirst()
        .relationships(RelationshipType.withName("http://purl.obolibrary.org/obo/RO_0002200"),
            Direction.OUTGOING)
        .relationships(RelationshipType.withName("http://purl.obolibrary.org/obo/RO_0002610"),
            Direction.OUTGOING)
        .relationships(RelationshipType.withName("http://purl.obolibrary.org/obo/RO_0002326"),
            Direction.OUTGOING)
        .evaluator(Evaluators.atDepth(1));

    phenotypeDescription = graphDb.traversalDescription().depthFirst()
        .relationships(RelationshipType.withName("http://purl.obolibrary.org/obo/RO_0002200"),
            Direction.OUTGOING)
        .relationships(RelationshipType.withName("http://purl.obolibrary.org/obo/RO_0002610"),
            Direction.OUTGOING)
        .relationships(RelationshipType.withName("http://purl.obolibrary.org/obo/RO_0002326"),
            Direction.OUTGOING)
        .evaluator(Evaluators.fromDepth(1)).evaluator(Evaluators.toDepth(2));

  }

  Optional<Node> getTaxon(Node source) {
    for (Path path : taxonDescription.traverse(source)) {
      if (path.length() > 0 && path.lastRelationship().isType(inTaxon)) {
        return Optional.of(path.endNode());
      }
    }
    return Optional.empty();
  }

  Optional<Node> getChromosome(Node source) {
    for (Path path : chromosomeDescription.traverse(source)) {
      if (path.length() > 0 && path.lastRelationship().isType(OwlRelationships.RDF_TYPE)) {
        if (chromsomeEntailment.contains(path.endNode())) {
          return Optional.of(path.lastRelationship().getOtherNode(path.endNode()));
        }
      }
    }
    return Optional.empty();
  }

  // TODO return array of all found nodes
  // TODO and filter only cliqueLeaders
  Optional<Node> getGene(Node source) {
    for (Path path : geneDescription.traverse(source)) {
      if (path.endNode().hasLabel(GENE_LABEL)) {
        return Optional.of(path.endNode());
      }
    }
    return Optional.empty();
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


  LoadingCache<Node, Optional<Node>> taxonCache =
      CacheBuilder.newBuilder().maximumSize(100_000).build(new CacheLoader<Node, Optional<Node>>() {
        @Override
        public Optional<Node> load(Node source) throws Exception {
          return getTaxon(source);
        }
      });

  LoadingCache<Node, Optional<Node>> chromosomeCache =
      CacheBuilder.newBuilder().maximumSize(100_000).build(new CacheLoader<Node, Optional<Node>>() {
        @Override
        public Optional<Node> load(Node source) throws Exception {
          return getChromosome(source);
        }
      });

  LoadingCache<Node, Optional<Node>> geneCache =
      CacheBuilder.newBuilder().maximumSize(100_000).build(new CacheLoader<Node, Optional<Node>>() {
        @Override
        public Optional<Node> load(Node source) throws Exception {
          return getGene(source);
        }
      });

  LoadingCache<Node, Collection<Node>> orthologCache = CacheBuilder.newBuilder()
      .maximumSize(100_000).build(new CacheLoader<Node, Collection<Node>>() {
        @Override
        public Collection<Node> load(Node source) throws Exception {
          return getOrthologs(source);
        }
      });


  long process(GolrCypherQuery query, Writer writer)
      throws IOException, ExecutionException, ClassNotFoundException {
    return process(query, writer, Optional.empty());
  }

  long process(GolrCypherQuery query, Writer writer, Optional<String> metaSourceQuery)
      throws IOException, ExecutionException, ClassNotFoundException {
    long recordCount = 0;

    try (Transaction tx = graphDb.beginTx()) {

      Result result = cypherUtil.execute(query.getQuery());

      // Golr queries need to have the evidence graphs merged, whereas chromosome queries don't.
      boolean isGolrQuery =
          result.columns().contains("subject") && result.columns().contains("object");

      if (isGolrQuery) {
        recordCount = serializeGolrQuery(query, result, writer, metaSourceQuery);
      } else {
        recordCount = serializedFeatureQuery(query, result, writer, metaSourceQuery);
      }

      tx.success();
    }

    return recordCount;
  }

  private long serializeGolrQuery(GolrCypherQuery query, Result result, Writer writer,
      Optional<String> metaSourceQuery)
      throws IOException, ClassNotFoundException, ExecutionException {

    DB db = DBMaker.newTempFileDB().closeOnJvmShutdown().deleteFilesAfterClose()
        .transactionDisable().cacheSize(1000000).make();
    ConcurrentMap<Pair<String, String>, String> resultsSerializable = db.createHashMap("results").make();
    ConcurrentMap<Pair<String, String>, EvidenceGraphInfo> resultsGraph = db.createHashMap("graphs").make();

    JsonGenerator generator = new JsonFactory().createGenerator(writer);
    ResultSerializer serializer = factory.create(generator);
    generator.writeStartArray();
    int recordCount = 0;
    while (result.hasNext()) {
      recordCount++;

      Map<String, Object> row = result.next();

      String subjectIri = (String) ((Node) row.get("subject")).getProperty(NodeProperties.IRI);
      String objectIri = (String) ((Node) row.get("object")).getProperty(NodeProperties.IRI);

      Pair<String, String> pair = new Pair<String, String>(subjectIri, objectIri);

      String existingResult = resultsSerializable.get(pair);
      if (existingResult == null) {
        Set<Long> ignoredNodes = new HashSet<>();
        Writer stringWriter = new StringWriter();
        JsonGenerator stringGenerator = new JsonFactory().createGenerator(stringWriter);
        ResultSerializer stringSerializer = factory.create(stringGenerator);
        boolean emitEvidence = true;
        TinkerGraphUtil tguEvidenceGraph = new TinkerGraphUtil(curieUtil);

        stringGenerator.writeStartObject();

        serializerRow(row, stringSerializer, tguEvidenceGraph, ignoredNodes, query);

        stringGenerator.writeEndObject();
        stringGenerator.close();

        resultsSerializable.put(pair, stringWriter.toString());

        resultsGraph.put(pair, new EvidenceGraphInfo(tguEvidenceGraph.getGraph(), emitEvidence, ignoredNodes));
      } else {
        EvidenceGraphInfo pairGraph = resultsGraph.get(pair);
        TinkerGraphUtil tguEvidenceGraph = new TinkerGraphUtil(EvidenceGraphInfo.toGraph(pairGraph.graphBytes), curieUtil);
        Set<Long> ignoredNodes = pairGraph.ignoredNodes;
        for (Entry<String, Object> entry : row.entrySet()) {
          Object value = entry.getValue();

          if (null == value) {
            continue;
          }

          // Add evidence
          if (value instanceof PropertyContainer) {
            tguEvidenceGraph.addElement((PropertyContainer) value);
          } else if (value instanceof Path) {
            tguEvidenceGraph.addPath((Path) value);
          } else if (value instanceof Node) {
            ignoredNodes.add(((Node) value).getId());
          }
        }
        resultsGraph.put(pair,
            new EvidenceGraphInfo(tguEvidenceGraph.getGraph(), pairGraph.emitEvidence, ignoredNodes));
      }

    }

    for (Entry<Pair<String, String>, String> resultSerializable : resultsSerializable.entrySet()) {
      generator.writeStartObject();

      Pair<String, String> p = resultSerializable.getKey();
      EvidenceGraphInfo pairGraph = resultsGraph.get(p);

      ObjectMapper mapper = new ObjectMapper();
      Map<String, Object> existingJson = mapper.readValue(resultSerializable.getValue(), Map.class);


      for (Entry<String, Object> entry : existingJson.entrySet()) {
        serializer.serialize(entry.getKey(), entry.getValue());
      }

      if (pairGraph != null) {
        com.tinkerpop.blueprints.Graph evidenceGraph =
            EvidenceGraphInfo.toGraph(pairGraph.graphBytes);
        processor.addAssociations(evidenceGraph);
        /*serializer.serialize(EVIDENCE_GRAPH,
            processor.getEvidenceGraph(evidenceGraph, metaSourceQuery));*/

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

    db.close();

    return recordCount;
  }

  private long serializedFeatureQuery(GolrCypherQuery query, Result result, Writer writer,
      Optional<String> metaSourceQuery) throws IOException, ExecutionException {
    JsonGenerator generator = new JsonFactory().createGenerator(writer);
    ResultSerializer serializer = factory.create(generator);

    int recordCount = 0;

    generator.writeStartArray();
    while (result.hasNext()) {
      generator.writeStartObject();
      Set<Long> ignoredNodes = new HashSet<>();
      TinkerGraphUtil tguEvidenceGraph = new TinkerGraphUtil(curieUtil);
      recordCount++;
      Map<String, Object> row = result.next();
      serializerRow(row, serializer, tguEvidenceGraph, ignoredNodes, query);
      generator.writeEndObject();
      generator.writeRaw('\n');
    }
    generator.writeEndArray();
    generator.close();
    return recordCount;
  }

  private boolean serializerRow(Map<String, Object> row, ResultSerializer serializer,
      TinkerGraphUtil tguEvidenceGraph, Set<Long> ignoredNodes, GolrCypherQuery query)
      throws IOException, ExecutionException {
    boolean emitEvidence = true;


    for (Entry<String, Object> entry : row.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();

      if (null == value) {
        continue;
      }

      // Add evidence
      if (value instanceof PropertyContainer) {
        tguEvidenceGraph.addElement((PropertyContainer) value);
      } else if (value instanceof Path) {
        tguEvidenceGraph.addPath((Path) value);
      } else if (value instanceof Node) {
        ignoredNodes.add(((Node) value).getId());
      }

      if (value instanceof Node) {

        // TODO: Clean this up
        if ("subject".equals(key) || "object".equals(key)) {
          Node node = (Node) value;
          Optional<Node> taxon = taxonCache.get(node);
          if (taxon.isPresent()) {
            serializer.serialize(key + "_taxon", taxon.get());
          }
          if (node.hasLabel(GENE_LABEL) || node.hasLabel(VARIANT_LABEL)
              || node.hasLabel(GENOTYPE_LABEL)) {
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

        if ("subject".equals(key)) {
          Collection<Node> orthologs = orthologCache.get((Node) value);
          Collection<String> orthologsId = transform(orthologs, new Function<Node, String>() {
            @Override
            public String apply(Node node) {
              String iri = GraphUtil.getProperty(node, NodeProperties.IRI, String.class).get();
              return curieUtil.getCurie(iri).orElse(iri);
            }
          });
          serializer.writeArray("subject_ortholog_closure", new ArrayList<String>(orthologsId));
        }

        if ("feature".equals(key)) {
          // Add disease and phenotype for feature
          serializer.serialize("disease", getDiseases((Node) value));
          serializer.serialize("phenotype", getPhenotypes((Node) value));
        }

        if (query.getCollectedTypes().containsKey(key)) {
          serializer.serialize(key, singleton((Node) value), query.getCollectedTypes().get(key));
        } else {
          serializer.serialize(key, value);
        }
      } else if (value instanceof Relationship) {
        String objectPropertyIri =
            GraphUtil.getProperty((Relationship) value, CommonProperties.IRI, String.class).get();
        Node objectProperty = graphDb.getNodeById(graph.getNode(objectPropertyIri).get());
        serializer.serialize(key, objectProperty);
      } else if (ClassUtils.isPrimitiveOrWrapper(value.getClass()) || value instanceof String) {
        // Serialize primitive types and Strings
        if ((key.equals("subject_category") || key.equals("object_category"))
            && value.equals("ontology")) {
          emitEvidence = false;
        }
        serializer.serialize(key, value);
      }
    }
    return emitEvidence;
  }

}
