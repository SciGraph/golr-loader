package org.monarch.golr;

import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLContext;

import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
import org.monarch.golr.beans.GolrCypherQuery;

import com.google.common.base.Optional;

public class GolrWorker implements Callable<Boolean> {

  private static final Logger logger = Logger.getLogger(GolrWorker.class.getName());

  Optional<String> solrServer;
  File outputFile;
  GolrLoader loader;
  GolrCypherQuery query;
  String solrJsonUrlSuffix;
  Object solrLock;

  public GolrWorker(Optional<String> solrServer, File outputFile, GolrLoader loader, GolrCypherQuery query, String solrJsonUrlSuffix, Object solrLock) {
    this.solrServer = solrServer;
    this.outputFile = outputFile;
    this.loader = loader;
    this.query = query;
    this.solrJsonUrlSuffix = solrJsonUrlSuffix;
    this.solrLock = solrLock;
    Thread.currentThread().setName("Golr processor - " + outputFile.getName());
  }

  @Override
  public Boolean call() throws Exception {
    logger.info("Writing JSON to: " + outputFile.getAbsolutePath());
    FileWriter writer = new FileWriter(outputFile);
    long recordCount = loader.process(query, writer);
    logger.info("Wrote " + recordCount + " documents to: " + outputFile.getAbsolutePath());
    logger.info(outputFile.getName() + " generated");
    if (solrServer.isPresent()) {
      synchronized (solrLock) {
        logger.info("Posting JSON " + outputFile.getName() + " to " + solrServer.get());
        try {
          // ignore ssl certs because letsencrypt is not supported by Oracle yet
          SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
            @Override
            public boolean isTrusted(java.security.cert.X509Certificate[] arg0, String arg1) throws java.security.cert.CertificateException {
              // TODO Auto-generated method stub
              return true;
            }
          }).build();

          CloseableHttpClient httpClient =
              HttpClients.custom().setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE).setSSLContext(sslContext).build();
          Request request =
              Request.Post(new URI(solrServer.get() + (solrServer.get().endsWith("/") ? "" : "/") + solrJsonUrlSuffix)).bodyFile(outputFile,
                  ContentType.APPLICATION_JSON);

          Executor executor = Executor.newInstance(httpClient);
          String result = executor.execute(request).returnContent().asString();

          logger.info(result);
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
