package org.monarch.golr.beans;

import io.scigraph.neo4j.DirectedRelationshipType;

import java.util.Collection;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.MoreObjects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class GolrCypherQuery {

  private String query;
  private Multimap<String, DirectedRelationshipType> types = HashMultimap.create();

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

  public Map<String, Collection<DirectedRelationshipType>> getCollectedTypes() {
    return types.asMap();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("query", query).add("types", types).toString();
  }

  // TODO temporary fix
  public String getPathQuery() {
    String queryWithStart = "";
    if (StringUtils.containsIgnoreCase(query, "start")) {
      queryWithStart =
          query.replaceAll("(?i)START",
              "START subject = node:node_auto_index(iri=\"SUBJECTIRI\"), object = node:node_auto_index(iri=\"OBJECTIRI\"), ").replaceAll("(?i)MATCH",
              "MATCH path=");
    } else {
      queryWithStart =
          query.replaceAll("(?i)MATCH",
              "START subject = node:node_auto_index(iri=\"SUBJECTIRI\"), object = node:node_auto_index(iri=\"OBJECTIRI\") MATCH path=");
    }
    String queryWithLimit = queryWithStart.replaceAll("(?i)UNION", "LIMIT 10 UNION") + "LIMIT 10";
    return "PLANNER RULE " + queryWithLimit.replaceAll("(?i)RETURN DISTINCT", "RETURN DISTINCT path,");
  }

}
