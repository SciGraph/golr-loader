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
  File outputFile;
  GolrLoader loader;
  GolrCypherQuery query;
  String solrJsonUrlSuffix;
  Object solrLock;
  boolean deleteJson;
  final static int BATCH_SIZE = 100000;

  public GolrWorker(Optional<String> solrServer, File outputFile, GolrLoader loader,
      GolrCypherQuery query, String solrJsonUrlSuffix, Object solrLock, boolean deleteJson) {
    this.solrServer = solrServer;
    this.outputFile = outputFile;
    this.loader = loader;
    this.query = query;
    this.solrJsonUrlSuffix = solrJsonUrlSuffix;
    this.solrLock = solrLock;
    this.deleteJson = deleteJson;
    Thread.currentThread().setName("Golr processor - " + outputFile.getName());
  }
  
  public static SolrInputDocument mapNodeToSolrDocument(ObjectNode node) {
      SolrInputDocument doc = new SolrInputDocument();
      Iterator<Entry<String, JsonNode>> fieldIterator = node.fields();
      while(fieldIterator.hasNext()) {
          Map.Entry<String, JsonNode> entry = fieldIterator.next();
          if (entry.getValue().isArray()) {
              doc.addField(entry.getKey(),
                      new ObjectMapper().convertValue(
                              entry.getValue(), ArrayList.class));
          } else { 
              doc.addField(entry.getKey(), entry.getValue().asText());
          }
      }
      return doc;
  }

  @Override
  public Boolean call() throws Exception {
    logger.info("Writing JSON to: " + outputFile.getAbsolutePath());
    FileWriter writer = new FileWriter(outputFile);
    long recordCount = loader.process(query, writer,
        Optional.of(FilenameUtils.removeExtension(outputFile.getName())));
    logger.info("Wrote " + recordCount + " documents to: " + outputFile.getAbsolutePath());
    logger.info(outputFile.getName() + " generated");
    if (solrServer.isPresent()) {
      synchronized (solrLock) {
        logger.info("Posting JSON " + outputFile.getName() + " to " + solrServer.get());
        try {
            
          SolrClient solr = new HttpSolrClient.Builder(solrServer.get()).build();
          
          ObjectMapper mapper = new ObjectMapper();
          JsonFactory factory = new JsonFactory();
          JsonParser parser = mapper.getFactory().createParser(outputFile);
          Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
          int documentCount = 0;
          if(parser.nextToken() != JsonToken.START_ARRAY) {
              throw new IllegalArgumentException("Expected an array");
          }

          while (parser.nextToken() == JsonToken.START_OBJECT) {
              if (documentCount == BATCH_SIZE) {
                  solr.add(docs);
                  solr.commit();
                  documentCount = 0;
                  docs.clear();
              }
              ObjectNode jsonDoc = mapper.readTree(parser);
              SolrInputDocument doc = mapNodeToSolrDocument(jsonDoc);
              docs.add(doc);
              documentCount++;
          }

          if (docs.size() > 0) {
              solr.add(docs);
              solr.commit();
          }
          solr.close();/*
          
          // ignore ssl certs because letsencrypt is not supported by Oracle yet
          SSLContext sslContext =
              new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
                @Override
                public boolean isTrusted(java.security.cert.X509Certificate[] arg0, String arg1)
                    throws java.security.cert.CertificateException {
                  return true;
                }
              }).build();

          CloseableHttpClient httpClient =
              HttpClients.custom().setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                  .setSSLContext(sslContext).build();
          Request request = Request.Post(new URI(
              solrServer.get() + (solrServer.get().endsWith("/") ? "" : "/") + solrJsonUrlSuffix))
              .bodyFile(outputFile, ContentType.APPLICATION_JSON);
          
          
       
          Executor executor = Executor.newInstance(httpClient);
          String result = executor.execute(request).returnContent().asString();
          
          logger.info(result);*/
          
          if (deleteJson) {
            logger.info("Deleting JSON " + outputFile.getName());
            outputFile.delete();
          }
        } catch (Exception e) {
          logger.log(Level.SEVERE, "Failed to post JSON " + outputFile.getName(), e);
          return false;
        }
      }
      logger.info(outputFile.getName() + " done");
    }
    return true;
  }
}
