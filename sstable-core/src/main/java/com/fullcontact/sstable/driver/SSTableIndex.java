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

import com.fullcontact.sstable.hadoop.IndexOffsetScanner;
import gnu.trove.list.array.TLongArrayList;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOError;

/**
 * Holds the data from an sstable index. For our purposes we only really care about the file offsets.
 *
 * @author ben <ben.vanberg@fullcontact.com>
 */
public class SSTableIndex {

    private static final Logger LOGGER = LoggerFactory.getLogger(SSTableIndex.class);

    private TLongArrayList offsets;

    public SSTableIndex() {
        offsets = new TLongArrayList();
    }

    public static SSTableIndex readIndex(FileSystem fs, Path file) {
        final SSTableIndex index = new SSTableIndex();

        IndexOffsetScanner indexScanner = null;

        try {
            indexScanner = new IndexOffsetScanner(file.toString(), fs);
            while (indexScanner.hasNext()) {
                index.addOffset(indexScanner.next());
            }
        } catch (IOError e) {
            LOGGER.error("Error reading index file: {}", file, e);
            throw e;
        } finally {
            if (indexScanner != null) {
                indexScanner.close();
            }
        }

        return index;
    }

    public static SSTableIndex readIndex(Path file) {
        final SSTableIndex index = new SSTableIndex();

        IndexOffsetScanner indexScanner = null;

        try {
            indexScanner = new IndexOffsetScanner(file.toString());
            while (indexScanner.hasNext()) {
                index.addOffset(indexScanner.next());
            }
        } catch (IOError e) {
            LOGGER.error("Error reading index file: {}", file, e);
            throw e;
        } finally {
            if (indexScanner != null) {
                indexScanner.close();
            }
        }

        return index;
    }

    private void addOffset(long offset) {
        this.offsets.add(offset);
    }

    public long[] getOffsets() {
        return this.offsets.toArray();
    }
}
