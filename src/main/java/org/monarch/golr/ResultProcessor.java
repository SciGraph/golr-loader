package org.monarch.golr;

import java.io.IOException;
import java.io.Writer;
import java.util.Map;
import java.util.Map.Entry;

import javax.inject.Inject;

import org.monarch.golr.beans.GolrCypherQuery;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Result;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

public class ResultProcessor {

  private final ResultSerializerFactory factory;

  @Inject
  ResultProcessor(ResultSerializerFactory factory) {
    this.factory = factory;
  }

  void process(GolrCypherQuery query, Result result, Writer writer) throws IOException {
    JsonGenerator generator = new JsonFactory().createGenerator(writer);
    ResultSerializer serializer = factory.create(generator);
    generator.writeStartArray();
    while (result.hasNext()) {
      generator.writeStartObject();
      Map<String, Object> row = result.next();
      for (Entry<String, Object> entry: row.entrySet()) {
        if (query.getProjection().keySet().contains(entry.getKey())) {
          serializer.serialize(query.getProjection().get(entry.getKey()), entry.getValue());
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
