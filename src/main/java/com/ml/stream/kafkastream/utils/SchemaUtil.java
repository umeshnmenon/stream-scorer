package com.ml.stream.kafkastream.utils;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * 
 * @author Umesh.Menon
 *	This class contains all the Schema related utility function
 */
public class SchemaUtil {
    final static Logger logger = LoggerFactory.getLogger(SchemaUtil.class);

    /**
     * Returns avro schema from a file
     *
     * @param absoluteFilePath
     * @return
     * @throws IOException
     */
    public Schema getSchemaFromFile(String absoluteFilePath) throws IOException {
        return this.getSchemaFromFile(new File(absoluteFilePath));
    }

    /**
     * Returns avro schema from a file
     *
     * @param schemaFile
     * @return
     * @throws IOException
     */
    public Schema getSchemaFromFile(File schemaFile) throws IOException {
        logger.debug("Loading schema from {}", schemaFile.getAbsolutePath());
        return new Schema.Parser().parse(schemaFile);
    }

    /**
     * Returns avro schema from a file
     *
     * @param schemaStream
     * @return
     * @throws IOException
     */
    public Schema getSchemaFromInputStream(InputStream schemaStream) throws IOException {
        logger.debug("Loading schema from inputStream.");
        return new Schema.Parser().parse(schemaStream);
    }
}
