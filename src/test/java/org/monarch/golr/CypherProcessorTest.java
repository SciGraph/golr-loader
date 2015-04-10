package org.monarch.golr;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

import org.junit.Test;

public class CypherProcessorTest {

  @Test
  public void injectWildcard() {
    assertThat(CypherProcessor.injectWildcard("MATCH (n)-[r]-(m) RETURN n, m"), is("MATCH (n)-[r]-(m) RETURN *, n, m"));
    assertThat(CypherProcessor.injectWildcard("MATCH (m)-[r]-(n) RETURN m, n"), is("MATCH (m)-[r]-(n) RETURN *, m, n"));
  }
  
  @Test
  public void injectWildcardWithSpace() {
    assertThat(CypherProcessor.injectWildcard("MATCH (m)-[r]-(n) RETURN\t\t  m, n"), is("MATCH (m)-[r]-(n) RETURN *, m, n"));
  }

}
