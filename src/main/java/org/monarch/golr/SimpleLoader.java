package org.monarch.golr;

import static com.google.common.collect.Lists.transform;
import io.scigraph.frames.Concept;
import io.scigraph.frames.NodeProperties;
import io.scigraph.internal.CypherUtil;
import io.scigraph.internal.GraphApi;
import io.scigraph.neo4j.Graph;
import io.scigraph.neo4j.GraphUtil;
import io.scigraph.owlapi.OwlRelationships;
import io.scigraph.owlapi.curies.CurieUtil;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import javax.inject.Inject;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;

public class SimpleLoader {

  private static final Logger logger = Logger.getLogger(SimpleLoader.class.getName());
  private final String cliqueLeaderString = "cliqueLeader";
  private final Label cliqueLeaderLabel = DynamicLabel.label(cliqueLeaderString);
  private final Set<String> labels = Sets.newHashSet("Phenotype", "disease", "gene");

  GraphDatabaseService graphDb;
  Graph graph;
  CypherUtil cypherUtil;
  CurieUtil curieUtil;
  GraphApi api;
  Set<String> unwantedLabels;

  @Inject
  public SimpleLoader(GraphDatabaseService graphDb, Graph graph, CypherUtil cypherUtil,
      CurieUtil curieUtil, GraphApi api) throws IOException {
    this.graphDb = graphDb;
    this.graph = graph;
    this.cypherUtil = cypherUtil;
    this.curieUtil = curieUtil;
    this.api = api;
    unwantedLabels = new HashSet<String>();
    unwantedLabels.add(cliqueLeaderString);
  }

  private static final RelationshipType inTaxon = DynamicRelationshipType
      .withName("http://purl.obolibrary.org/obo/RO_0002162");

  // add leaf node or not
  public void generate(Optional<String> outputFile) throws IOException {

    Writer writer = null;
    if (outputFile.isPresent()) {
      writer = new FileWriter(new File(outputFile.get()));
    } else {
      writer = new StringWriter();
    }
    JsonGenerator generator = new JsonFactory().createGenerator(writer);

    generator.writeStartArray();

    try (Transaction tx = graphDb.beginTx()) {

      // consider only cliqueLeaders
      ResourceIterator<Node> cliqueLeaderNodes = graphDb.findNodes(cliqueLeaderLabel);
      while (cliqueLeaderNodes.hasNext()) {
        Node baseNode = cliqueLeaderNodes.next();
        // consider only nodes with a label property and in the category set
        if (isInLabelSet(baseNode.getLabels(), labels)
            && baseNode.hasProperty(NodeProperties.LABEL)) {
          generator.writeStartObject();
          String iri = GraphUtil.getProperty(baseNode, NodeProperties.IRI, String.class).get();
          generator.writeStringField("iri", iri);
          generator.writeStringField("id", curieUtil.getCurie(iri).or(iri));
          writeOptionalArray("label", generator,
              GraphUtil.getProperties(baseNode, NodeProperties.LABEL, String.class));
          writeOptionalArray("definition", generator,
              GraphUtil.getProperties(baseNode, Concept.SYNONYM, String.class));
          writeOptionalArray("synonym", generator,
              GraphUtil.getProperties(baseNode, Concept.DEFINITION, String.class));

          // taxon
          List<String> taxons = new ArrayList<String>();
          for (Path path : graphDb.traversalDescription().depthFirst()
              .relationships(inTaxon, Direction.OUTGOING).traverse(baseNode)) {
            if (path.length() > 0) {
              taxons.add(GraphUtil.getProperty(path.endNode(), NodeProperties.LABEL, String.class)
                  .get());
              taxons.addAll(GraphUtil.getProperties(path.endNode(), Concept.SYNONYM, String.class));
            }
          }
          writeOptionalArray("taxon", generator, taxons);

          // categories
          writeOptionalArray("category", generator, baseNode.getLabels());

          // equivalences
          List<String> equivalences = new ArrayList<String>();
          for (Path path : graphDb.traversalDescription().breadthFirst()
              .relationships(OwlRelationships.OWL_SAME_AS)
              .relationships(OwlRelationships.OWL_EQUIVALENT_CLASS).traverse(baseNode)) {
            if (path.length() > 0) {
              equivalences.add(GraphUtil.getProperty(path.endNode(), NodeProperties.IRI,
                  String.class).get());
            }
          }
          writeOptionalArray("equivalent_iri", generator, equivalences);
          writeOptionalArray("equivalent_curie", generator,
              transform(equivalences, new Function<String, String>() {
                @Override
                public String apply(String iri) {
                  return curieUtil.getCurie(iri).or(iri);
                }
              }));

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

      }
      tx.success();
    }

    generator.writeEndArray();
    generator.flush();

    graphDb.shutdown();

    if (!outputFile.isPresent()) {
      System.out.println(writer.toString());
    }
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
}
