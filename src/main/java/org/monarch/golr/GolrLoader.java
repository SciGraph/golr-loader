package org.monarch.golr;

import java.io.IOException;
import java.io.Writer;
import java.util.Map;
import java.util.Map.Entry;

import javax.inject.Inject;

import org.monarch.golr.beans.GolrCypherQuery;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Result;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

public class GolrLoader {

  private final GraphDatabaseService graphDb;
  private final ResultSerializerFactory factory;

  @Inject
  GolrLoader(GraphDatabaseService graphDb, ResultSerializerFactory factory) {
    this.graphDb = graphDb;
    this.factory = factory;
  }

  void process(GolrCypherQuery query, Writer writer) throws IOException {
    Result result = graphDb.execute(query.getQuery());
    JsonGenerator generator = new JsonFactory().createGenerator(writer);
    ResultSerializer serializer = factory.create(generator);
    generator.writeStartArray();
    while (result.hasNext()) {
      generator.writeStartObject();
      Map<String, Object> row = result.next();
      for (Entry<String, Object> entry: row.entrySet()) {
        if (query.getProjection().keySet().contains(entry.getKey())) {
          String alias = query.getProjection().get(entry.getKey());
          if (query.getCollectedTypes().containsKey(entry.getKey())) {
            serializer.serialize(alias, (Node)entry.getValue(), query.getCollectedTypes().get(entry.getKey()));
          } else {
            serializer.serialize(alias, entry.getValue());
          }
        } else if (!(entry.getValue() instanceof PropertyContainer)) {
          serializer.serialize(entry.getKey(), entry.getValue());
        }
      }
      generator.writeEndObject();
    }
    generator.writeEndArray();
    generator.close();
  }

}
