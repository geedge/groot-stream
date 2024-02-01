package com.geedgenetworks.core.utils.cn.csv;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.*;

public class HighCsvReader {

    private final Logger logger = LoggerFactory.getLogger(HighCsvReader.class);

    private CSVReader csvReader;

    private String[] header = null;

    private final Map<String, Integer> columnNames = new HashMap<>();

    private final List<String[]> data = new ArrayList<>();

    public HighCsvReader(Reader reader) {
        init(reader, new ArrayList<>());
    }

    public HighCsvReader(Reader reader, List<String> columns) {
        init(reader, columns);
    }

    private void init(Reader reader, List<String> columns) {
        try {
            this.csvReader = new CSVReader(reader, CSVParser.DEFAULT_SEPARATOR,
                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER);
            String[] allHeader = csvReader.readNext();
            if (allHeader == null) {
                return;
            }
            for (int i = 0; i < allHeader.length; i++) {
                String columnName = allHeader[i];
                columnNames.put(columnName, i);
            }
            if (columns.size() == 0) {
                columns.addAll(columnNames.keySet());
            } else {
                columns.removeIf(next -> !columnNames.containsKey(next));
            }
            header = new String[columns.size()];
            columns.toArray(header);

            for (String[] strings : csvReader) {
                String[] lineData = new String[header.length];
                for (int i = 0; i < lineData.length; i++) {
                    lineData[i] = strings[columnNames.get(header[i])];
                }
                data.add(lineData);
            }
        } catch (Exception e) {
            logger.error("csv parse error, error exception: " + e.getMessage(), e);
        } finally {
            if (csvReader != null) {
                try {
                    csvReader.close();
                } catch (Exception ignored) {

                }
            }
        }
    }

    public CsvIterator getIterator() {
        return new CsvIterator(header, data);
    }

    public int getLineNumber() {
        return data.size();
    }

    public Integer getFieldIndex(String fieldName) {
        return columnNames.get(fieldName);
    }

    private InputStreamReader getReader(byte[] bytes) {
        return new InputStreamReader(new ByteArrayInputStream(bytes));
    }

    public class CsvIterator implements Iterator<Map<String, String>> {

        private String[] header;

        private List<String[]> data;

        private int total = 0;

        private int currentLineNumber = 0;

        public CsvIterator(String[] header, List<String[]> data) {
            this.header = header;
            this.data = data;
            this.total = data.size();
        }

        @Override
        public boolean hasNext() {
            return currentLineNumber < total;
        }

        @Override
        public Map<String, String> next() {
            HashMap<String, String> lineMap = new HashMap<String, String>(header.length);
            String[] line = data.get(currentLineNumber);
            for (int i = 0; i < line.length; i++) {
                lineMap.put(header[i], line[i]);
            }
            currentLineNumber++;
            return lineMap;
        }
    }


}
