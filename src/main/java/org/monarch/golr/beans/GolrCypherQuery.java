package org.monarch.golr.beans;

import io.scigraph.neo4j.DirectedRelationshipType;

import java.util.Collection;
import java.util.Map;

import com.google.common.base.MoreObjects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class GolrCypherQuery {

  private String query;
  private Multimap<String, DirectedRelationshipType> types = HashMultimap.create();
  private Multimap<String, ProcessorSpec> processingMap = HashMultimap.create();

  GolrCypherQuery() {}

  public GolrCypherQuery(String query) {
    this.query = query;
  }

  public String getQuery() {
    return query;
  }

  public Multimap<String, ProcessorSpec> getProcessingMap() {
    return processingMap;
  }

  public Multimap<String, DirectedRelationshipType> getTypes() {
    return types;
  }

  public Map<String, Collection<DirectedRelationshipType>> getCollectedTypes() {
    return types.asMap();
  }
  
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("query", query)
        .add("types", types)
        .toString();
  }

}
