package org.apache.solr.handler.dataimport;

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.handler.dataimport.DataConfig.Entity;
import org.junit.Test;

public class GmailServiceUserMailEntityProcessorTest {

  @Test
  public void test() {
    GmailServiceUserMailEntityProcessor p = new GmailServiceUserMailEntityProcessor();
    Entity entity = new Entity();
    entity.allAttributes = new HashMap<String, String>();
    entity.allAttributes.put("serviceAccountId", System.getProperty("serviceAccountId"));
    entity.allAttributes.put("domain", System.getProperty("domain"));
    entity.allAttributes.put("oldestDate", System.getProperty("oldestDate"));
    entity.allAttributes.put("users", System.getProperty("users"));
    entity.allAttributes.put("ignoreFrom", System.getProperty("ignoreFrom"));
    entity.allAttributes.put("processAttachements", System.getProperty("processAttachements"));
    Context ctx = new ContextImpl(entity, new VariableResolverImpl(), null, null, null, null, null);

    p.firstInit(ctx);
    p.init(ctx);
    Map<String, Object> row = null;
    while((row = p.nextRow()) != null) {
      System.out.println(row);
    }
    
  }

}
