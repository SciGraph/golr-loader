package org.monarch.golr;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.monarch.golr.beans.GolrCypherQuery;

public class GolrWorker implements Callable<Boolean> {

  private static final Logger logger = Logger.getLogger(GolrWorker.class.getName());

  String solrServer;
  GolrLoader loader;
  GolrCypherQuery query;
  Object solrLock;
  Optional<String> queryName;

  public GolrWorker(String solrServer, GolrLoader loader,
      GolrCypherQuery query, Object solrLock, Optional<String> queryName) {
    this.solrServer = solrServer;
    this.loader = loader;
    this.query = query;
    this.solrLock = solrLock;
    this.queryName = queryName;
    Thread.currentThread().setName("Golr processor - " + queryName.get());
  }
  
  @Override
  public Boolean call() throws Exception {
    logger.info("Processing: " + queryName.get());
    long recordCount = loader.process(query, solrServer, solrLock, queryName);
    logger.info("Wrote " + recordCount + " documents to: " + solrServer);
    logger.info(queryName.get() + " finished");
    return true;
  }
}
