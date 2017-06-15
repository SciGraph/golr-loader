package org.monarch.golr;

import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLContext;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.core.JsonFactory;


import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.commons.io.FilenameUtils;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
import org.monarch.golr.beans.GolrCypherQuery;

public class GolrWorker implements Callable<Boolean> {

  private static final Logger logger = Logger.getLogger(GolrWorker.class.getName());

  Optional<String> solrServer;
  GolrLoader loader;
  GolrCypherQuery query;
  Object solrLock;
  String queryName;

  public GolrWorker(Optional<String> solrServer, GolrLoader loader,
      GolrCypherQuery query, Object solrLock, String queryName) {
    this.solrServer = solrServer;
    this.loader = loader;
    this.query = query;
    this.solrLock = solrLock;
    this.queryName = queryName;
    Thread.currentThread().setName("Golr processor - " + query.toString());
  }
  
  @Override
  public Boolean call() throws Exception {
    SolrClient solrClient = new HttpSolrClient.Builder(solrServer.get()).build();
    logger.info("Processing: " + queryName);
    long recordCount = loader.process(query, solrClient, solrLock);
    logger.info("Wrote " + recordCount + " documents to: " + solrServer);
    logger.info(queryName + " finished");
    return true;
  }
}
