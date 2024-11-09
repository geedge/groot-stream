package com.geedgenetworks.core.connector.schema;

import com.geedgenetworks.spi.table.schema.SchemaParser;
import com.geedgenetworks.spi.table.type.StructType;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class SchemaParserTest {

    public static void main(String[] args) throws Exception{
        String str = FileUtils.readFileToString(new File("D:\\WorkSpace\\groot-stream\\session_record_schema.json"), StandardCharsets.UTF_8);
        SchemaParser.Parser parser = SchemaParser.PARSER_AVRO;

        StructType structType = parser.parser(str);
        System.out.println(structType.treeString());

    }

}