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
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

import org.apache.commons.lang3.ClassUtils;
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

/** Loader for the golr solr core
 *
 * Schema: https://github.com/monarch-initiative/monarch-app/blob/master/conf/golr-views/oban-config.yaml
 * Queries: https://github.com/monarch-initiative/monarch-cypher-queries/tree/master/src/main/cypher/golr-loader
 *
 * The loader iterates over the results of the queries, generates solr documents, and inserts them
 * into the solr server passed to the process method
 *
 * Note that the queries must return a minimum of path, subject, and object ordered by
 * subject and then object.  The loader aggregates subject-object pairs per query into a single
 * solr document.  Note that the process for picking a relation is unclear see
 * https://github.com/SciGraph/golr-loader/issues/35
 *
 */
public class GolrLoader {
  
  private static final Logger logger = Logger.getLogger(GolrLoader.class.getName());

  private static final String EVIDENCE_GRAPH = "evidence_graph";
  private static final String EVIDENCE_FIELD = "evidence";
  private static final String SOURCE_FIELD = "source";
  private static final String EVIDENCE_OBJECT_FIELD = "evidence_object";
  private static final String DEFINED_BY = "is_defined_by";

  private final GraphDatabaseService graphDb;
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
  
  private final static int BATCH_SIZE = 10000;

  private static final String ENTAILMENT_REGEX = "^\\[(\\w*):?([\\w:|\\.\\/#`]*)([!*\\.\\d]*)\\]$";
  private static Pattern ENTAILMENT_PATTERN = Pattern.compile(ENTAILMENT_REGEX);

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
      EvidenceProcessor processor, GraphApi api) {
    this.graphDb = graphDb;
    this.cypherUtil = cypherUtil;
    this.curieUtil = curieUtil;
    this.graph = graph;
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


  long process(GolrCypherQuery query, String solrServer, Object solrLock)
      throws IOException, ExecutionException, ClassNotFoundException, SolrServerException {
    return process(query, solrServer, solrLock, Optional.empty());
  }

  long process(GolrCypherQuery query, String solrServer, Object solrLock, Optional<String> metaSourceQuery)
      throws IOException, ExecutionException, ClassNotFoundException, SolrServerException {
    long recordCount = 0;

    try (Transaction tx = graphDb.beginTx()) {

      Result result = cypherUtil.execute(query.getQuery());

      // Golr queries need to have the evidence graphs merged, whereas chromosome queries don't.
      boolean isGolrQuery =
          result.columns().contains("subject") && result.columns().contains("object");

      if (isGolrQuery) {
        recordCount = serializeGolrQuery(query, result, solrServer, solrLock, metaSourceQuery);
      } else {
        recordCount = serializedFeatureQuery(query, result, solrServer, solrLock, metaSourceQuery);
      }

      tx.success();
    }

    return recordCount;
  }

  private long serializeGolrQuery(GolrCypherQuery query, Result result,
      String solrServer, Object solrLock, Optional<String> metaSourceQuery)
      throws IOException, ClassNotFoundException, ExecutionException, SolrServerException {

    SolrInputDocument resultDoc = new SolrInputDocument();
    EvidenceGraphInfo resultGraph = null;
    Pair<String, String> lastPair = new Pair<>("", "");

    ClosureUtil closureUtil = new ClosureUtil(graphDb, curieUtil);
    SolrDocUtil docUtil = new SolrDocUtil(closureUtil);
    Collection<SolrInputDocument> docList = new ArrayList<>();
    
    int recordCount = 0;
    int pairCount = 0;
    while (result.hasNext()) {

      Map<String, Object> row = result.next();

      String subjectIri = (String) ((Node) row.get("subject")).getProperty(NodeProperties.IRI);
      String objectIri = (String) ((Node) row.get("object")).getProperty(NodeProperties.IRI);

      Pair<String, String> pair = new Pair<>(subjectIri, objectIri);
      if (recordCount == 0) {
        lastPair = new Pair<>(subjectIri, objectIri);
      }
      if (!pair.equals(lastPair)){

        if (resultGraph != null) {
          com.tinkerpop.blueprints.Graph evidenceGraph =
                  EvidenceGraphInfo.toGraph(resultGraph.graphBytes);
          processor.addAssociations(evidenceGraph);
          String evidenceBlob = processor.getEvidenceGraph(evidenceGraph, metaSourceQuery);
          if (!resultDoc.getFieldValue("subject_category").equals("ontology")
                  || !resultDoc.getFieldValue("object_category").equals("ontology")){

            //TODO add exclude evidence to configuration
            resultDoc.addField(EVIDENCE_GRAPH, evidenceBlob);

            List<Closure> evidenceObjectClosure =
                    processor.getEvidenceObject(evidenceGraph, resultGraph.ignoredNodes);
            docUtil.writeQuint(EVIDENCE_OBJECT_FIELD, evidenceObjectClosure, resultDoc);

            List<Closure> evidenceClosure = processor.getEvidence(evidenceGraph);
            docUtil.writeQuint(EVIDENCE_FIELD, evidenceClosure, resultDoc);

          }

          List<Closure> sourceClosure = processor.getSource(evidenceGraph);
          docUtil.writeQuint(SOURCE_FIELD, sourceClosure, resultDoc);
          resultDoc.addField(DEFINED_BY, processor.getDefinedBys(evidenceGraph));
          docList.add(resultDoc);
        } else {
          docList.add(resultDoc);
          System.out.println("No evidence graph");
        }
        if (docList.size() % BATCH_SIZE == 0) {
          addAndCommitToSolr(solrServer, docList, solrLock);
          docList.clear();
        }
        // Reset
        lastPair = pair;
        resultDoc = new SolrInputDocument();
        resultGraph = null;
        pairCount = 0;
      }

      if (pairCount == 0) {
        Set<Long> ignoredNodes = new HashSet<>();
        Writer stringWriter = new StringWriter();
        JsonGenerator stringGenerator = new JsonFactory().createGenerator(stringWriter);
        boolean emitEvidence = true;
        TinkerGraphUtil tguEvidenceGraph = new TinkerGraphUtil(curieUtil);

        stringGenerator.writeStartObject();

        resultDoc = serializerRow(row, tguEvidenceGraph, ignoredNodes, query);

        resultGraph = new EvidenceGraphInfo(tguEvidenceGraph.getGraph(), emitEvidence, ignoredNodes);
      } else {
        pairCount++;
        TinkerGraphUtil tguEvidenceGraph = new TinkerGraphUtil(EvidenceGraphInfo.toGraph(resultGraph.graphBytes), curieUtil);
        Set<Long> ignoredNodes = resultGraph.ignoredNodes;
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
        resultGraph =
            new EvidenceGraphInfo(tguEvidenceGraph.getGraph(), resultGraph.emitEvidence, ignoredNodes);
      }
      recordCount++;
    }

    if (docList.size() > 0) {
      addAndCommitToSolr(solrServer, docList, solrLock);
      docList.clear();
    }

    return recordCount;
  }
  
  private static void addAndCommitToSolr(String solrServer,
      Collection<SolrInputDocument>docList, Object solrLock) throws SolrServerException, IOException {
    synchronized (solrLock) {
      HttpSolrClient solrClient = new HttpSolrClient.Builder(solrServer).build();
      // Set the socket and connect timeouts the same as solr.jetty.http.idleTimeout
      solrClient.setSoTimeout(200000);
      solrClient.setConnectionTimeout(200000);
      try {
        solrClient.add(docList);
      } catch (IOException|SolrServerException e) {
        logger.warning("Caught: " + e);
        logger.info("Retrying add");
        solrClient.add(docList);
      }
      solrClient.commit();
      solrClient.close();
    }
  }

  private long serializedFeatureQuery(GolrCypherQuery query, Result result, 
      String solrServer, Object solrLock, Optional<String> metaSourceQuery)
          throws IOException, ExecutionException, SolrServerException {

    int recordCount = 0;
    Collection<SolrInputDocument> docList = new ArrayList<SolrInputDocument>();


    while (result.hasNext()) {
      SolrInputDocument doc = new SolrInputDocument();
      Set<Long> ignoredNodes = new HashSet<>();
      TinkerGraphUtil tguEvidenceGraph = new TinkerGraphUtil(curieUtil);
      recordCount++;
      Map<String, Object> row = result.next();
      doc = serializerRow(row, tguEvidenceGraph, ignoredNodes, query);
      docList.add(doc);
      if (docList.size() != 0 && docList.size() % BATCH_SIZE == 0) {
        addAndCommitToSolr(solrServer, docList, solrLock);
        docList.clear();
      }
    }
    
    if (docList.size() > 0) {
      addAndCommitToSolr(solrServer, docList, solrLock);
      docList.clear();
    }
    
    return recordCount;
  }

  private Set<DirectedRelationshipType> resolveRelationships(String key, String value) {
    Set<DirectedRelationshipType> rels = new HashSet<>();
    String cypherIn = String.format("[%s:%s]", key, value);
    String cypherOut = cypherUtil.resolveRelationships(cypherIn);
    Matcher m = ENTAILMENT_PATTERN.matcher(cypherOut);
    while (m.find()) {
      String types = m.group(2);
      String[] cypherRels = types.split("\\|");
      for (String cypherRel : cypherRels) {
        String unquotedCypherRel = cypherRel.replaceAll("^`|`$","");
        RelationshipType relType = RelationshipType.withName(unquotedCypherRel);
        DirectedRelationshipType dirRelType = new DirectedRelationshipType(relType, Direction.OUTGOING);
        rels.add(dirRelType);
      }
    }
    return rels;
  }

  SolrInputDocument serializerRow(Map<String, Object> row,
      TinkerGraphUtil tguEvidenceGraph, Set<Long> ignoredNodes, GolrCypherQuery query)
      throws IOException, ExecutionException {
    boolean emitEvidence = true;
    SolrInputDocument doc = new SolrInputDocument();
    ClosureUtil closureUtil = new ClosureUtil(graphDb, curieUtil);
    SolrDocUtil docUtil = new SolrDocUtil(closureUtil);

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
            docUtil.addNodes(key + "_taxon", singleton((Node) taxon.get()), doc);
          }
          if (node.hasLabel(GENE_LABEL) || node.hasLabel(VARIANT_LABEL)
              || node.hasLabel(GENOTYPE_LABEL)) {
            // Attempt to add gene and chromosome for monarch-initiative/monarch-app/#746
            if (node.hasLabel(GENE_LABEL)) {
                docUtil.addNodes(key + "_gene", singleton((Node) node), doc);
            } else {
              Optional<Node> gene = geneCache.get(node);
              if (gene.isPresent()) {
                  docUtil.addNodes(key + "_gene", singleton((Node) gene.get()), doc);
              }
            }

            Optional<Node> chromosome = chromosomeCache.get(node);
            if (chromosome.isPresent()) {
                docUtil.addNodes(key + "_chromosome", singleton((Node) chromosome.get()), doc);
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
          doc.addField("subject_ortholog_closure", new ArrayList<String>(orthologsId));
        }

        if ("feature".equals(key)) {
          // Add disease and phenotype for feature
          docUtil.addNodes("disease", getDiseases((Node) value), doc);
          docUtil.addNodes("phenotype", getPhenotypes((Node) value), doc);
        }

        if (query.getCollectedTypes().containsKey(key)) {
          docUtil.addNodes(key, singleton((Node) value), query.getCollectedTypes().get(key), doc);
        }
        else if ("subject".equals(key) || "object".equals(key) || "relation".equals(key) || "evidence".equals(key)) {
          Set<DirectedRelationshipType> closureTypes = new HashSet<>();
          closureTypes.addAll(docUtil.DEFAULT_CLOSURE_TYPES);
          if ("subject".equals(key) && query.getSubjectClosure() != null) {
            Set<DirectedRelationshipType> rels = resolveRelationships("subject_closure", query.getSubjectClosure());
            closureTypes.addAll(rels);
          }
          if ("object".equals(key) && query.getObjectClosure() != null) {
            Set<DirectedRelationshipType> rels = resolveRelationships("object_closure", query.getObjectClosure());
            closureTypes.addAll(rels);
          }
          if ("relation".equals(key) && query.getRelationClosure() != null) {
            Set<DirectedRelationshipType> rels = resolveRelationships("relation_closure", query.getRelationClosure());
            closureTypes.addAll(rels);
          }
          if ("evidence".equals(key) && query.getEvidenceClosure() != null) {
            Set<DirectedRelationshipType> rels = resolveRelationships("evidence_closure", query.getEvidenceClosure());
            closureTypes.addAll(rels);
          }
          docUtil.addNodes(key, singleton((Node) value), closureTypes, doc);
        }
        else {
          docUtil.addNodes(key, singleton((Node) value), doc);
        }
      } else if (value instanceof Relationship) {
        String objectPropertyIri =
            GraphUtil.getProperty((Relationship) value, CommonProperties.IRI, String.class).get();
        Node objectProperty = graphDb.getNodeById(graph.getNode(objectPropertyIri).get());
        docUtil.addNodes(key, singleton((Node) objectProperty), doc);
      } else if (ClassUtils.isPrimitiveOrWrapper(value.getClass()) || value instanceof String) {
        // Serialize primitive types and Strings
        if ((key.equals("subject_category") || key.equals("object_category"))
            && value.equals("ontology")) {
          emitEvidence = false;
        }
        doc.addField(key, value);
      }
    }
    return doc;
  }

}
