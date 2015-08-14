package org.monarch.golr.processor;

import io.scigraph.cache.Cacheable;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import javax.inject.Inject;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Result;

import com.google.common.base.Charsets;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.Resources;

public class PhenotypeResultProcessor extends ResultProcessor {

  private final String cypher;
  
  @Inject
  PhenotypeResultProcessor() throws IOException {
    cypher = Resources.toString(Resources.getResource("phenotype.cypher"), Charsets.UTF_8);
  }

  @Cacheable
  @Override
  public Collection<Node> produceField(PropertyContainer container) {
    Multimap<String, Object> params = HashMultimap.create();
    params.put("id", ((Node)container).getId());
    Result result = cypherUtil.execute(cypher, params);
    Collection<Node> phenotypes = new HashSet<>();
    while (result.hasNext()) {
      Map<String, Object> row = result.next();
      phenotypes.add((Node) row.get("phenotype"));
    }
    return phenotypes;
  }

}
