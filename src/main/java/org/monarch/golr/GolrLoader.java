package org.monarch.golr;

import static java.util.Collections.singleton;
import io.scigraph.cache.Cacheable;
import io.scigraph.frames.CommonProperties;
import io.scigraph.internal.CypherUtil;
import io.scigraph.internal.TinkerGraphUtil;
import io.scigraph.neo4j.Graph;
import io.scigraph.neo4j.GraphUtil;

import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;

import org.apache.commons.lang3.ClassUtils;
import org.monarch.golr.beans.Closure;
import org.monarch.golr.beans.GolrCypherQuery;
import org.monarch.golr.beans.ProcessorSpec;
import org.monarch.golr.processor.ResultProcessor;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.Multimap;
import com.google.inject.Injector;
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
  private final Injector injector;

  @Inject
  GolrLoader(GraphDatabaseService graphDb, Graph graph, CypherUtil cypherUtil,
      ResultSerializerFactory factory, EvidenceProcessor processor, Injector injector) {
    this.graphDb = graphDb;
    this.cypherUtil = cypherUtil;
    this.graph = graph;
    this.factory = factory;
    this.processor = processor;
    this.injector = injector;
  }

  @Cacheable
  ResultProcessor getProcessor(String fqn) throws ClassNotFoundException {
    return (ResultProcessor) injector.getInstance(Class.forName(fqn));
  }

  long process(GolrCypherQuery query, Writer writer) throws IOException, ExecutionException, ClassNotFoundException {
    long recordCount = 0;

    try (Transaction tx = graphDb.beginTx()) {
      Result result = cypherUtil.execute(query.getQuery());
      JsonGenerator generator = new JsonFactory().createGenerator(writer);
      ResultSerializer serializer = factory.create(generator);
      generator.writeStartArray();
      Multimap<String, ProcessorSpec> processingMap = query.getProcessingMap();
      while (result.hasNext()) {
        recordCount++;
        generator.writeStartObject();
        Map<String, Object> row = result.next();
        com.tinkerpop.blueprints.Graph evidenceGraph = new TinkerGraph();
        Set<Long> ignoredNodes = new HashSet<>();
        boolean emitEvidence = true;
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
          }

          if (value instanceof Node) {
            Node node = (Node)value;
            ignoredNodes.add(((Node) value).getId());
            if (query.getCollectedTypes().containsKey(key)) {
              serializer.serialize(key, singleton((Node)value), query.getCollectedTypes().get(key));
            } else {
              serializer.serialize(key, value);
            }
            for (ProcessorSpec spec: processingMap.get(key)) {
              ResultProcessor processor = getProcessor(spec.getFqn());
              Collection<Node> nodes = processor.produceField(node);
              serializer.serialize(spec.getFieldName(), nodes, ResultSerializer.DEFAULT_CLOSURE_TYPES);
            }
          } else if (value instanceof Relationship) {
            String objectPropertyIri =
                GraphUtil.getProperty((Relationship) value, CommonProperties.IRI, String.class)
                .get();
            Node objectProperty = graphDb.getNodeById(graph.getNode(objectPropertyIri).get());
            serializer.serialize(key, objectProperty);
          } else if (ClassUtils.isPrimitiveOrWrapper(value.getClass()) || value instanceof String) {
            // Serialize primitive types and Strings
            if ((key.equals("subject_category") || key.equals("object_category")) && value.equals("ontology")) {
              emitEvidence = false;
            }
            serializer.serialize(key, value);
          }
        }
        processor.addAssociations(evidenceGraph);
        serializer.serialize(EVIDENCE_GRAPH, processor.getEvidenceGraph(evidenceGraph));

        // TODO: Hackish to remove evidence but the resulting JSON is blooming out of control
        // Don't emit evidence for ontology sources
        if (emitEvidence) {
          List<Closure> evidenceObjectClosure =
              processor.getEvidenceObject(evidenceGraph, ignoredNodes);
          serializer.writeQuint(EVIDENCE_OBJECT_FIELD, evidenceObjectClosure);
          List<Closure> evidenceClosure = processor.getEvidence(evidenceGraph);
          serializer.writeQuint(EVIDENCE_FIELD, evidenceClosure);
          List<Closure> sourceClosure = processor.getSource(evidenceGraph);
          serializer.writeQuint(SOURCE_FIELD, sourceClosure);
          serializer.writeArray(DEFINED_BY, processor.getDefinedBys(evidenceGraph));
        }
        generator.writeEndObject();
      }
      generator.writeEndArray();
      generator.close();
      tx.success();
    }
    return recordCount;
  }

}
