/*
 * Copyright 2014 FullContact, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.fullcontact.sstable.driver;

import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Read and sstable index. This comes in the from of -CompressionInfo.db
 * <p/>
 * This is a POC just to play with the sstable compression info files.
 *
 * @author ben <ben.vanberg@fullcontact.com>
 */
public class ReadCompressionMetadata {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadCompressionMetadata.class);

    private static final String COMPRESSION_INFO_FILE = "/profile_identities-results-ic-83679-CompressionInfo.db";
    private static final String SSTABLE_FILE = "/Users/ben/code/hadoop-sstable/src/main/resources/profile_identities-results-ic-83679-Data.db";

    public static void main(String[] args) {
        readUsingMine();

        readUsingCassandra();

        readUsingHadoopBased();
    }

    private static void readUsingMine() {
        ReadCompressionInfoCompletely compressionMetaData = ReadCompressionInfoCompletely.create(SSTABLE_FILE);
    }

    private static void readUsingCassandra() {
        CompressionMetadata compressionMetadata = CompressionMetadata.create(SSTABLE_FILE);
        CompressionMetadata.Chunk firstChunk = compressionMetadata.chunkFor(300000);
        System.out.println(firstChunk.toString());
    }

    private static void readUsingHadoopBased() {

        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.getLocal(new Configuration());
        } catch (IOException e) {
            e.printStackTrace();
        }

        com.fullcontact.cassandra.io.compress.CompressionMetadata compressionMetadata = com.fullcontact.cassandra.io.compress.CompressionMetadata.create(SSTABLE_FILE, fileSystem);
        com.fullcontact.cassandra.io.compress.CompressionMetadata.Chunk firstChunk = compressionMetadata.chunkFor(300000);
        System.out.println(firstChunk.toString());
    }
}
