package org.apache.solr.handler.dataimport;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    protected List<String> placeHolderVariables;
    public static final String URL = "url";

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
            System.out.println("=response======"+response);
            result = (List)response.get("docs");
            rowIterator = result.listIterator();
        } catch (IOException e) {
            System.out.println("===exception===="+e);
        }
    }

    @Override
    public Map<String, Object> nextRow() {

        if (!rowIterator.hasNext()) {
            return null;
        }
        return rowIterator.next();
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