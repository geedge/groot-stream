package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.core.utils.cn.csv.HighCsvReader;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

class HighCsvReaderTest {

    @Test
    void inputTest() {
        List<String> needColumns = new ArrayList<>();
        needColumns.add("test");
        byte[] content = new byte[0];
        HighCsvReader highCsvReader = new HighCsvReader(new InputStreamReader(new ByteArrayInputStream(content)), needColumns);
        System.out.println(highCsvReader.getLineNumber());
        HighCsvReader.CsvIterator iterator = highCsvReader.getIterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
    }
}