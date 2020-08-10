package org.monarch.golr;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.Map;
import java.util.logging.Logger;

import javax.inject.Inject;

import org.neo4j.graphdb.*;
import org.prefixcommons.CurieUtil;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.scigraph.frames.Concept;
import io.scigraph.frames.NodeProperties;
import io.scigraph.internal.CypherUtil;
import io.scigraph.internal.GraphApi;
import io.scigraph.neo4j.Graph;
import io.scigraph.neo4j.GraphUtil;
import io.scigraph.owlapi.OwlRelationships;

class SimpleLoader {

  private static final Logger logger = Logger.getLogger(SimpleLoader.class.getName());
  private final Label cliqueLeaderLabel = Label.label("cliqueLeader");
  private final Set<String> unwantedLabels = Sets.newHashSet("cliqueLeader",
                                                             "Node",
                                                             "Class",
                                                             "NamedIndividual");

  GraphDatabaseService graphDb;
  Graph graph;
  CypherUtil cypherUtil;
  CurieUtil curieUtil;
  GraphApi api;

  Set<String> tmp = new HashSet<>();

  @Inject
  public SimpleLoader(GraphDatabaseService graphDb, Graph graph, CypherUtil cypherUtil,
                      CurieUtil curieUtil, GraphApi api) throws IOException {
    this.graphDb = graphDb;
    this.graph = graph;
    this.cypherUtil = cypherUtil;
    this.curieUtil = curieUtil;
    this.api = api;
  }

  private static final RelationshipType inTaxon =
      RelationshipType.withName("http://purl.obolibrary.org/obo/RO_0002162");

  // add leaf node or not
  public void generate(Writer writer,
                       Map<String, List<String>> eqCurieMap) throws IOException {

    JsonGenerator generator = new JsonFactory().createGenerator(writer);

    generator.writeStartArray();

    try (Transaction tx = graphDb.beginTx()) {

      // consider only cliqueLeaders
      // https://github.com/SciGraph/golr-loader/issues/41
      ResourceIterator<Node> cliqueLeaderNodes = graphDb.findNodes(cliqueLeaderLabel);
      while (cliqueLeaderNodes.hasNext()) {
        Node baseNode = cliqueLeaderNodes.next();
        String iri = GraphUtil.getProperty(baseNode, NodeProperties.IRI, String.class).get();
        Optional<String> curie = curieUtil.getCurie(iri);

        // Require nodes to have a label, and filter out blank nodes
        if (!baseNode.hasProperty(NodeProperties.LABEL)
                || iri.startsWith("_:")
                || iri.startsWith("https://monarchinitiative.org/.well-known/genid/")) {
          continue;
        }

        if (!curie.isPresent()) {
          logger.info("Cannot resolve IRI to curie");
          logger.info(iri);
          continue;
        }

        String[] curieParts;
        curieParts = curie.get().split(":");
        if (curieParts.length == 1) {
          logger.info("Curie does not have colon");
          logger.info(curie.get());
          continue;
        }

        String prefix = curieParts[0];
        String reference = curieParts[1];
        generator.writeStartObject();
        generator.writeStringField("id", curieUtil.getCurie(iri).orElse(iri));
        generator.writeStringField("prefix", prefix);

        try{
          writeOptionalArray("label", generator,
                  GraphUtil.getProperties(baseNode, NodeProperties.LABEL, String.class));
        } catch(Exception e) {
          logger.severe(iri);
          logger.severe(baseNode.getLabels().toString());
          logger.severe(GraphUtil.getProperties(baseNode, NodeProperties.LABEL, Double.class).toString());
          throw e;
        }
        writeOptionalArray("definition", generator,
                GraphUtil.getProperties(baseNode, Concept.DEFINITION, String.class));
        writeOptionalArray("synonym", generator,
                GraphUtil.getProperties(baseNode, Concept.SYNONYM, String.class));

        // Number of edges
        generator.writeNumberField("edges",
                getEdgeCount(baseNode.getRelationships()));

        // taxon
        Optional<Node> taxon = Optional.empty();
        for (Path path : graphDb.traversalDescription().depthFirst()
                .relationships(inTaxon, Direction.OUTGOING).traverse(baseNode)) {
          if (path.length() > 0) {
            taxon = Optional.of(path.endNode());
            break;
          }
        }

        if (taxon.isPresent()) {
          String taxonIri =
                  GraphUtil.getProperty(taxon.get(), NodeProperties.IRI, String.class).get();
          generator.writeStringField("taxon", curieUtil.getCurie(taxonIri).orElse(taxonIri));

          Collection<String> lbs =
                  GraphUtil.getProperties(taxon.get(), NodeProperties.LABEL, String.class);
          String taxonLabel = "";
          if (lbs.size() >= 1) {
            taxonLabel = lbs.iterator().next();
          }
          // https://github.com/monarch-initiative/dipper/issues/415
          if (lbs.size() > 1 && !tmp.contains(taxon.get().getProperty("iri").toString())) {
            tmp.add(taxon.get().getProperty("iri").toString());
            System.out.println("Multiple taxon labels");
            System.out.println(taxon.get().getProperty("iri").toString());
            Iterator<String> it = lbs.iterator();
            while (it.hasNext()) {
              System.out.println(it.next());
            }
          }

          generator.writeStringField("taxon_label", taxonLabel);
          writeOptionalArray("taxon_label_synonym", generator,
                  (GraphUtil.getProperties(taxon.get(), Concept.SYNONYM, String.class)));
        } else {
          // https://github.com/monarch-initiative/dipper/issues/415
          generator.writeStringField("taxon", "");
          generator.writeStringField("taxon_label", "");
          writeOptionalArray("taxon_label_synonym", generator, new ArrayList<Label>());
        }

        // Check if node is connected to a phenotype
        boolean hasPheno = isPhenotypeConnected(iri);
        generator.writeBooleanField("has_phenotype", hasPheno);

        // categories
        writeOptionalArray("category", generator,
                Lists.newArrayList(baseNode.getLabels()));

        // equivalences
        List<String> equivalences = new ArrayList<>();
        for (Path path : graphDb.traversalDescription().breadthFirst()
                .relationships(OwlRelationships.OWL_SAME_AS)
                .relationships(OwlRelationships.OWL_EQUIVALENT_CLASS).traverse(baseNode)) {
          if (path.length() > 0) {
            equivalences.add(
                    GraphUtil.getProperty(path.endNode(), NodeProperties.IRI, String.class).get());
          }
        }

        List<String> equivalentCuries = new ArrayList<>();
        if (eqCurieMap.containsKey(prefix)) {
          for (String eqPrefix : eqCurieMap.get(prefix)) {
            equivalentCuries.add(eqPrefix + ":" +  reference);
          }
        }

        for (String equivalentIri : equivalences) {
          // Get curie prefix
          Optional<String> eqCurie = curieUtil.getCurie(equivalentIri);
          if (eqCurie.isPresent()) {
            equivalentCuries.add(eqCurie.get());
            String[] eqParts = eqCurie.get().split(":");
            String eqPrefix = eqParts[0];
            String eqReference = eqParts[1];
            if (eqCurieMap.containsKey(eqPrefix)) {
              for (String prefx : eqCurieMap.get(eqPrefix)) {
                equivalentCuries.add(prefx + ":" +  eqReference);
              }
            }
          }
        }

        writeOptionalArray("equivalent_curie", generator, equivalentCuries);

        // is leaf
        if (baseNode.hasRelationship(Direction.INCOMING, OwlRelationships.RDFS_SUBCLASS_OF)) {
          generator.writeBooleanField("leaf", false);
        } else {
          generator.writeBooleanField("leaf", true);
        }

        // end of object
        generator.writeEndObject();
        generator.writeRaw('\n');
      }
      tx.success();
    }

    generator.writeEndArray();
    generator.flush();

    graphDb.shutdown();
  }

  private boolean isInLabelSet(Iterable<Label> nodeLabels, Set<String> validLabels) {
    Iterator<Label> it = nodeLabels.iterator();
    while (it.hasNext()) {
      if (validLabels.contains(it.next().name())) {
        return true;
      }
    }
    return false;
  }

  private boolean isPhenotypeConnected(String iri) {
    boolean hasPhenotype = false;
    String phenoQuery = String.format(
        "MATCH (n:Node {iri:'%s'})-[:`http://purl.obolibrary.org/obo/RO_0002200`]-(:phenotype) RETURN n LIMIT 1",
        iri);
    try (Transaction tx = graphDb.beginTx()) {

      Result result = graphDb.execute(phenoQuery);
      if (result.hasNext()) {
        hasPhenotype = true;
      }
      tx.success();
    }
    return hasPhenotype;
  }

  public void writeOptionalArray(String fieldName, JsonGenerator generator,
      Iterable<Label> collection) throws IOException {
    Iterator<Label> iterator = collection.iterator();
    generator.writeArrayFieldStart(fieldName);
    while (iterator.hasNext()) {
      // filter out unwanted labels
      String label = iterator.next().name();
      if (!unwantedLabels.contains(label)) {
        generator.writeString(label);
      }
    }
    generator.writeEndArray();
  }

  public void writeOptionalArray(String fieldName, JsonGenerator generator,
      Collection<String> collection) throws IOException {
    generator.writeArrayFieldStart(fieldName);
    for (String s : collection) {
      generator.writeString(s);
    }
    generator.writeEndArray();
  }

  public int getEdgeCount(Iterable<Relationship> relationships) {
    int size = 0;
    for(Relationship value : relationships) {
      size++;
    }
    return size;
  }
}
