package datawave.microservice.metadata;

import com.google.common.collect.Maps;
import datawave.data.ColumnFamilyConstants;
import datawave.marking.MarkingFunctions;
import datawave.microservice.dictionary.config.ConnectionConfig;
import datawave.microservice.dictionary.config.ResponseObjectFactory;
import datawave.security.util.ScannerHelper;
import datawave.webservice.query.result.metadata.DefaultMetadataField;
import datawave.webservice.results.datadictionary.DefaultDataDictionary;
import datawave.webservice.results.datadictionary.DefaultDescription;
import datawave.webservice.results.datadictionary.DefaultDictionaryField;
import datawave.webservice.results.datadictionary.DefaultFields;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.SortedMap;
import java.util.stream.Collectors;

public class DefaultMetadataFieldScanner {
    
    private static final Logger log = LoggerFactory.getLogger(DefaultMetadataFieldScanner.class);
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    
    private final MarkingFunctions markingFunctions;
    private final ResponseObjectFactory<DefaultDescription,?,DefaultMetadataField,?,?> responseObjectFactory;
    private final Map<String,String> normalizationMap;
    private final ConnectionConfig connectionConfig;
    private final int numThreads;
    
    public DefaultMetadataFieldScanner(MarkingFunctions markingFunctions,
                    ResponseObjectFactory<DefaultDescription,DefaultDataDictionary,DefaultMetadataField,DefaultDictionaryField,DefaultFields> responseObjectFactory,
                    Map<String,String> normalizationMap, ConnectionConfig connectionConfig, int numThreads) {
        this.markingFunctions = markingFunctions;
        this.responseObjectFactory = responseObjectFactory;
        this.normalizationMap = normalizationMap;
        this.connectionConfig = connectionConfig;
        this.numThreads = numThreads;
    }
    
    public Collection<DefaultMetadataField> getFields(Map<String,String> aliases, Collection<String> datatypeFilters) throws TableNotFoundException {
        BatchScanner scanner = createScanner();
        Transformer transformer = new Transformer(scanner.iterator(), aliases, datatypeFilters);
        Collection<DefaultMetadataField> fields = transformer.transform();
        scanner.close();
        return fields;
    }
    
    private BatchScanner createScanner() throws TableNotFoundException {
        BatchScanner scanner = ScannerHelper.createBatchScanner(connectionConfig.getConnector(), connectionConfig.getMetadataTable(),
                        connectionConfig.getAuths(), numThreads);
        // Ensure rows for the same field are grouped into a single iterator entry.
        scanner.addScanIterator(new IteratorSetting(21, WholeRowIterator.class));
        // Do not limit the scanner based on ranges.
        scanner.setRanges(Collections.singletonList(new Range()));
        scanner.fetchColumnFamily(ColumnFamilyConstants.COLF_E);
        scanner.fetchColumnFamily(ColumnFamilyConstants.COLF_I);
        scanner.fetchColumnFamily(ColumnFamilyConstants.COLF_RI);
        scanner.fetchColumnFamily(ColumnFamilyConstants.COLF_DESC);
        scanner.fetchColumnFamily(ColumnFamilyConstants.COLF_H);
        scanner.fetchColumnFamily(ColumnFamilyConstants.COLF_T);
        scanner.fetchColumnFamily(ColumnFamilyConstants.COLF_TF);
        return scanner;
    }
    
    private class Transformer {
        private final Iterator<Map.Entry<Key,Value>> iterator;
        private final Map<String,String> aliases;
        private final Collection<String> dataTypeFilters;
        private final boolean acceptAllDataTypes;
        
        private final Map<String,Map<String,DefaultMetadataField>> fields;
        private final Text currRow = new Text();
        private final Text currColumnFamily = new Text();
        
        private Key currKey;
        private Value currValue;
        private String currColumnQualifier;
        private DefaultMetadataField currField;
        
        private Transformer(Iterator<Map.Entry<Key,Value>> iterator, Map<String,String> aliases, Collection<String> dataTypeFilters) {
            this.iterator = iterator;
            this.aliases = aliases;
            this.dataTypeFilters = dataTypeFilters;
            this.acceptAllDataTypes = dataTypeFilters.isEmpty();
            fields = new HashMap<>();
        }
        
        private Collection<DefaultMetadataField> transform() {
            while (iterator.hasNext()) {
                Map.Entry<Key,Value> entry = iterator.next();
                try {
                    // Handle batch scanner bug
                    if (entry.getKey() == null && entry.getValue() == null)
                        return null;
                    if (null == entry.getKey() || null == entry.getValue()) {
                        throw new IllegalArgumentException("Null key or value. Key:" + entry.getKey() + ", Value: " + entry.getValue());
                    }
                    transformEntry(entry);
                } catch (IOException e) {
                    throw new IllegalStateException("Unable to decode row " + entry.getKey());
                } catch (MarkingFunctions.Exception e) {
                    throw new IllegalStateException("Unable to decode visibility " + entry.getKey(), e);
                }
            }
            return fields.values().stream().map(Map::values).flatMap(Collection::stream).collect(Collectors.toCollection(LinkedList::new));
        }
        
        private void transformEntry(Map.Entry<Key,Value> currEntry) throws IOException, MarkingFunctions.Exception {
            SortedMap<Key,Value> rowEntries = WholeRowIterator.decodeRow(currEntry.getKey(), currEntry.getValue());
            
            for (Map.Entry<Key,Value> entry : rowEntries.entrySet()) {
                currKey = entry.getKey();
                currValue = entry.getValue();
                currKey.getRow(currRow);
                currKey.getColumnFamily(currColumnFamily);
                currColumnQualifier = currKey.getColumnQualifier().toString();
                
                // If this row is a hidden event, do not continue transforming it or include it in the final results.
                if (isColumnFamly(ColumnFamilyConstants.COLF_H)) {
                    currField = null;
                    fields.remove(currRow.toString());
                    break;
                }
                
                // Verify that the row should not be filtered out due to its datatype.
                String dataType = getDataType();
                if (hasAllowedDataType(dataType)) {
                    
                    setOrCreateCurrField(dataType);
                    
                    if (isColumnFamly(ColumnFamilyConstants.COLF_E)) {
                        currField.setIndexOnly(false);
                        setFieldNameAndAlias();
                        setLastUpdated();
                    } else if (isColumnFamly(ColumnFamilyConstants.COLF_I)) {
                        currField.setForwardIndexed(true);
                    } else if (isColumnFamly(ColumnFamilyConstants.COLF_RI)) {
                        currField.setReverseIndexed(true);
                    } else if (isColumnFamly(ColumnFamilyConstants.COLF_DESC)) {
                        setDescriptions();
                    } else if (isColumnFamly(ColumnFamilyConstants.COLF_T)) {
                        setType();
                    } else if (isColumnFamly(ColumnFamilyConstants.COLF_TF)) {
                        currField.setTokenized(true);
                    } else {
                        log.warn("Unknown entry with key={}, value={}", currKey, currValue);
                    }
                    
                    // If the field name is null, this is potentially an index-only field which has no "e" entry.
                    // Set the field name, alias, and last updated. These values will be replaced if an "e" entry
                    // is encountered later on.
                    if (currField.getFieldName() == null) {
                        setFieldNameAndAlias();
                        setLastUpdated();
                    }
                }
            }
        }
        
        private boolean isColumnFamly(final Text columnFamily) {
            return columnFamily.equals(currColumnFamily);
        }
        
        private boolean hasAllowedDataType(String dataType) {
            return acceptAllDataTypes || dataTypeFilters.contains(dataType);
        }
        
        private String getDataType() {
            int nullPos = currColumnQualifier.indexOf('\0');
            return (nullPos < 0) ? currColumnQualifier : currColumnQualifier.substring(0, nullPos);
        }
        
        private void setOrCreateCurrField(final String dataType) {
            Map<String,DefaultMetadataField> dataTypes = fields.computeIfAbsent(currRow.toString(), k -> Maps.newHashMap());
            currField = dataTypes.get(dataType);
            
            // If a field does not yet exist for the field name and data type, create a new one that
            // defaults to index-only, which shall be cleared if we see an event entry.
            if (currField == null) {
                currField = new DefaultMetadataField();
                currField.setIndexOnly(true);
                currField.setDataType(dataType);
                dataTypes.put(dataType, currField);
            }
        }
        
        private void setFieldNameAndAlias() {
            String fieldName = currKey.getRow().toString();
            if (aliases.containsKey(fieldName)) {
                currField.setFieldName(aliases.get(fieldName));
                currField.setInternalFieldName(fieldName);
            } else {
                currField.setFieldName(fieldName);
            }
        }
        
        private void setDescriptions() throws MarkingFunctions.Exception {
            DefaultDescription description = responseObjectFactory.getDescription();
            description.setDescription(currValue.toString());
            description.setMarkings(getMarkings(currKey));
            currField.getDescriptions().add(description);
        }
        
        private Map<String,String> getMarkings(Key key) throws MarkingFunctions.Exception {
            ColumnVisibility columnVisibility = key.getColumnVisibilityParsed();
            return markingFunctions.translateFromColumnVisibility(columnVisibility);
        }
        
        private void setType() {
            int nullPos = currColumnQualifier.indexOf('\0');
            String type = currColumnQualifier.substring(nullPos + 1);
            String normalizedType = normalizationMap.get(type);
            currField.addType(normalizedType != null ? normalizedType : "Unknown");
        }
        
        private void setLastUpdated() {
            LocalDateTime dateTime = Instant.ofEpochMilli(currKey.getTimestamp()).atZone(ZoneId.systemDefault()).toLocalDateTime();
            currField.setLastUpdated(dateTime.format(dateFormatter));
        }
    }
}
