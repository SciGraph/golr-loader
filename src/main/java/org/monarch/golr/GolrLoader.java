package org.monarch.golr;

import java.io.IOException;
import java.io.Writer;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.inject.Inject;

import org.monarch.golr.beans.GolrCypherQuery;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Result;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;

import edu.sdsc.scigraph.internal.TinkerGraphUtil;

public class GolrLoader {

  private static String EVIDENCE_GRAPH = "evidence_graph";
  private static String EVIDENCE_IDS = "evidence_ids";
  private static String EVIDENCE_LABELS = "evidence_labels";
  private static String EVIDENCE_IDS_CLOSURE = "evidence_ids_closure";
  private static String EVIDENCE_LABELS_CLOSURE = "evidence_labels_closure";
  
  private final GraphDatabaseService graphDb;
  private final ResultSerializerFactory factory;
  private final EvidenceProcessor processor;

  @Inject
  GolrLoader(GraphDatabaseService graphDb, ResultSerializerFactory factory, EvidenceProcessor processor) {
    this.graphDb = graphDb;
    this.factory = factory;
    this.processor = processor;
  }

  void process(GolrCypherQuery query, Writer writer) throws IOException {
    Result result = graphDb.execute(query.getQuery());
    JsonGenerator generator = new JsonFactory().createGenerator(writer);
    ResultSerializer serializer = factory.create(generator);
    generator.writeStartArray();
    while (result.hasNext()) {
      generator.writeStartObject();
      Map<String, Object> row = result.next();
      Graph evidenceGraph = new TinkerGraph();
      Set<Long> ignoredNodes = new HashSet<>();
      for (Entry<String, Object> entry: row.entrySet()) {
        if (entry.getValue() instanceof PropertyContainer) {
          TinkerGraphUtil.addElement(evidenceGraph, (PropertyContainer) entry.getValue());
        }
        if (query.getProjection().keySet().contains(entry.getKey())) {
          String alias = query.getProjection().get(entry.getKey());
          if (query.getCollectedTypes().containsKey(entry.getKey())) {
            ignoredNodes.add(((Node)entry.getValue()).getId());
            serializer.serialize(alias, (Node)entry.getValue(), query.getCollectedTypes().get(entry.getKey()));
          } else {
            serializer.serialize(alias, entry.getValue());
          }
        } else if (!(entry.getValue() instanceof PropertyContainer)) {
          serializer.serialize(entry.getKey(), entry.getValue());
        }
      }
      processor.addAssociations(evidenceGraph);
      serializer.serialize(EVIDENCE_GRAPH, processor.getEvidenceGraph(evidenceGraph));
      generator.writeEndObject();
    }
    generator.writeEndArray();
    generator.close();
  }

}
