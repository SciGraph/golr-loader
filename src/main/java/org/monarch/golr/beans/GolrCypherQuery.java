package org.monarch.golr.beans;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import edu.sdsc.scigraph.neo4j.DirectedRelationshipType;

public class GolrCypherQuery {

  private String query;
  private Multimap<String, DirectedRelationshipType> types = HashMultimap.create();
  private Map<String, String> projection = new HashMap<>();

  GolrCypherQuery() {}
  
  public GolrCypherQuery(String query) {
    this.query = query;
  }

  public String getQuery() {
    return query;
  }

  public Multimap<String, DirectedRelationshipType> getTypes() {
    return types;
  }

  public Map<String, String> getProjection() {
    return projection;
  }

}
