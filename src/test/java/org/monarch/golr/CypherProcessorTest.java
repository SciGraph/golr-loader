package org.monarch.golr;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

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

  @Test
  public void injectWildcardMixedCase() {
    assertThat(CypherProcessor.injectWildcard("MATCH (m)-[r]-(n) rETuRn\t\t  m, n"), is("MATCH (m)-[r]-(n) RETURN *, m, n"));
  }

  @Test
  public void getNonAliasedProjection() {
    assertThat(CypherProcessor.getProjection("MATCH (m)-[r]-(n) RETURN m, n"), containsInAnyOrder("m", "n"));
  }

  @Test
  public void getAliasedProjection() {
    assertThat(CypherProcessor.getProjection("MATCH (m)-[r]-(n) RETURN m AS M, n"), containsInAnyOrder("M", "n"));
  }

  @Test
  public void getMixedCaseProjection() {
    assertThat(CypherProcessor.getProjection("MATCH (m)-[r]-(n) rETuRn m As M, n"), containsInAnyOrder("M", "n"));
  }

}
