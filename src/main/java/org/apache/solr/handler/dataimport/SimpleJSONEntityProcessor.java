package org.apache.solr.handler.dataimport;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.io.BufferedReader;

/**
 * Process "simple" JSON from an API.
 */
public class SimpleJSONEntityProcessor extends EntityProcessorBase {
    private BufferedReader reader;
    private String url;
    private ListIterator<Map<String, Object>> rowIterator;
    private List<Map<String, Object>> result;
    public static final String URL = "url";
    private static final Logger LOG = LoggerFactory.getLogger(SqlEntityProcessor.class);

    public SimpleJSONEntityProcessor() {
    }

    /**
     * Reference: https://github.com/apache/lucene-solr/blob/master/solr/contrib/dataimporthandler/src/java/org/apache/solr/handler/dataimport/LineEntityProcessor.java
     * @param context
     */
    @Override
    public void init(Context context) {
        super.init(context);

        url = context.getResolvedEntityAttribute(URL);
        if (url == null) {
            throw new DataImportHandlerException(DataImportHandlerException.SEVERE, "'" + URL + "' is a required attribute");
        }
        reader = new BufferedReader((Reader) context.getDataSource().getData(url));
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            Map response = objectMapper.readValue(reader, Map.class);
            System.out.println("---------response---------"+response);
            result = (List)response.get("docs");
            context.setSessionAttribute("count","22",Context.SCOPE_ENTITY);
            context.setSessionAttribute("viewIndex","1",Context.SCOPE_ENTITY);

            rowIterator = result.listIterator();
        } catch (IOException e) {
            System.out.println("------exception------"+e);
        }
    }

    @Override
    public Map<String, Object> nextRow() {
        System.out.println("----nextRow----");
        if (!rowIterator.hasNext()) {
            fetchNextRow(null);
        }

        if (rowIterator != null)
            return rowIterator.next();
        else
            return null;
    }

    @Override
    public Map<String, Object> nextModifiedRowKey() {
        LOG.debug("=======inside nextModifiedRowKey ============");
        if (rowIterator == null) {
            String deltaImportQuery = getQuery();
            System.out.println("=========="+deltaImportQuery+"===========");
//            fetchNextRow(deltaQuery);
        }
        if (rowIterator != null && rowIterator.hasNext())
            return rowIterator.next();
        else
            return null;
    }

    public String getQuery() {
        if (Context.DELTA_DUMP.equals(context.currentProcess())) {
            String deltaImportQuery = context.getEntityAttribute("deltaImportQuery");
            System.out.println(deltaImportQuery);
            if(deltaImportQuery != null) return deltaImportQuery;
        }
        LOG.warn("'deltaImportQuery' attribute is not specified for entity : "+ entityName);
        return null;
    }
    private void fetchNextRow(String nextUrl)
    {
        int count  = Integer.parseInt((String)context.getSessionAttribute("count", Context.SCOPE_ENTITY));
        Integer viewIndex  = Integer.parseInt((String)context.getSessionAttribute("viewIndex", Context.SCOPE_ENTITY));


        System.out.println("------fetchNextRow------");
        if (viewIndex < count) {
            if (nextUrl == null ) {
                nextUrl = "https://dev-hc.hotwax.io/api/products?viewIndex=" + viewIndex;
            }
            if (nextUrl == null) {
                throw new DataImportHandlerException(DataImportHandlerException.SEVERE, "'" + nextUrl + "' is a required attribute");
            }
            System.out.println("Fetching next records for" + nextUrl);
            reader = new BufferedReader((Reader) context.getDataSource().getData(nextUrl));
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                Map response = objectMapper.readValue(reader, Map.class);
                System.out.println("------fetchNextRow------");
                result = (List) response.get("docs");
                if (result != null)
                    rowIterator = result.listIterator();
            } catch (IOException e) {
                System.out.println("------exception------" + e);
            }
            viewIndex = viewIndex+1;
            context.setSessionAttribute("viewIndex",viewIndex.toString(),Context.SCOPE_ENTITY);
        } else {
            rowIterator = null;
        }
    }

    @Override
    public void destroy () {
        try {
            reader.close();
        } catch (Exception e) {
            // do nothing
        }
        super.destroy();
    }

}
