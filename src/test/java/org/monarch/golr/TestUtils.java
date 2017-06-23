package org.monarch.golr;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.monarch.golr.beans.GolrCypherQuery;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;


public class TestUtils extends GolrLoadSetup {
  
  static List<Map<String, Object>> getResultList(GolrCypherQuery query) {
    List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
    try (Transaction tx = graphDb.beginTx()) {
      Result result = cypherUtil.execute(query.getQuery());
      Map<String, Object> row = result.next();
      resultList.add(row);
      tx.success();
    }
    return resultList;
  }
  
  static SolrInputDocument convertJSONToSolrDocument(String json) throws JsonProcessingException, IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonParser parser = mapper.getFactory().createParser(json);
    ObjectNode jsonDoc = mapper.readTree(parser);
    
    SolrInputDocument doc = new SolrInputDocument();
    Iterator<Entry<String, JsonNode>> fieldIterator = jsonDoc.fields();
    while(fieldIterator.hasNext()) {
      Map.Entry<String, JsonNode> entry = fieldIterator.next();
      if (entry.getValue().isArray()) {
        doc.addField(entry.getKey(),
            new ObjectMapper().convertValue(entry.getValue(), ArrayList.class));
      } else { 
        doc.addField(entry.getKey(), entry.getValue().asText());
      }
    }
    return doc;
   }
  
  static Writer convertSolrToJson(SolrInputDocument doc) throws IOException {
    Writer writer = new StringWriter();
    JsonGenerator stringGenerator = new JsonFactory().createGenerator(writer);
    stringGenerator.writeStartObject();
    for (Map.Entry<String, SolrInputField> entry : doc.entrySet()) {
      String field = entry.getKey();
      SolrInputField value = entry.getValue();
      if (value.getValue() instanceof ArrayList) {
        stringGenerator.writeArrayFieldStart(field);
        Iterator iter = value.iterator();
        while (iter.hasNext()){
          stringGenerator.writeObject(iter.next());
        }
        stringGenerator.writeEndArray();
      } else {
        stringGenerator.writeObjectField(field, value.getValue());
      }
    }
    stringGenerator.writeEndObject();
    stringGenerator.close();
    return writer;
   }

}
