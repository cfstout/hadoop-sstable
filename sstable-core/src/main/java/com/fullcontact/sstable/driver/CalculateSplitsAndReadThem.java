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
import com.fullcontact.sstable.hadoop.mapreduce.SSTableSplit;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Simple split calc and reading of said splits.
 *
 * @author ben <ben.vanberg@fullcontact.com>
 */
public class CalculateSplitsAndReadThem {

    private static final String INDEX_FILE = "/Users/ben/code/hadoop-sstable/sstable-core/src/main/resources/searchtarget-results-jb-36177-Index.db";
    private static final String DATA_FILE = "/Users/ben/code/hadoop-sstable/sstable-core/src/main/resources/searchtarget-results-jb-36177-Data.db";

    public static void main(String[] args) {

        // splits
        final List<SSTableSplit> splits = Lists.newArrayList();

        // offsets
        final List<Long> offsets = Lists.newArrayList(); // Keep an eye on Long creation here.

        // Split size (Arbitrary for now)
        final long splitSize = 67108864; // 64MB

        // Index files contain offsets into the uncompressed data.
        // Use the hadoop file system here.
        FileSystem fileSystem = null;
        try {
            // Driver just uses a local file system.
            fileSystem = FileSystem.getLocal(new Configuration());
        } catch (IOException e) {
            e.printStackTrace();  // The driver doesn't care a whole lot about error handling.
        }

        final IndexOffsetScanner indexScanner = new IndexOffsetScanner(INDEX_FILE, fileSystem);

        // Make some bricks!
        long currentStart = 0;
        long currentEnd = 0;

        try {
            while (indexScanner.hasNext()) {

                // TODO: This does not give an exact size of this split in bytes but a rough estimate. This should be good enough since it's only used for sorting splits by size in hadoop land.
                while (currentEnd - currentStart < splitSize && indexScanner.hasNext()) {
                    currentEnd = indexScanner.next();
                    offsets.add(currentEnd);
                }

                // Record the split
//                final SSTableSplit split = new SSTableSplit(new Path(DATA_FILE), Longs.toArray(offsets), currentEnd - currentStart, new String[]{});
                final SSTableSplit split = new SSTableSplit(); // just to make this compile...

                // TODO: add the length to the split
                splits.add(split);

                // Clear the offsets
                offsets.clear();

                // TODO: Hey look we have to read the next value anyway so we can probably use this to calculate an accurate length of the previous split
                if (indexScanner.hasNext()) {
                    currentStart = indexScanner.next();
                    currentEnd = currentStart;
                    offsets.add(currentStart);
                }
            }
        } finally {
            if (indexScanner != null) {
                indexScanner.close();
            }
        }

        validateSplits(splits);
    }

    private static void validateSplits(List<SSTableSplit> splits) {

        long offsetSum = 0;
        int indexOffsetCount;
        final Set<Long> indexOffsets = Sets.newHashSet();
        IndexOffsetScanner indexScanner = null;
        try {
            indexScanner = new IndexOffsetScanner(INDEX_FILE);
            indexOffsetCount = 0;
            while (indexScanner.hasNext()) {
                long indexEntry = indexScanner.next();
                offsetSum = indexEntry;
                indexOffsetCount++;
                indexOffsets.add(indexEntry);
            }
        } finally {
            if (indexScanner != null) {
                indexScanner.close();
            }
        }

        int splitOffsetSum = 0;
        Set<Long> splitOffsets = Sets.newHashSet();
        for (SSTableSplit split : splits) {
            System.out.println(split);
            splitOffsetSum += split.getOffsetCount();
            // TODO: fix this to account for new split with start/end only.
//            splitOffsets.addAll(Longs.asList(split.getOffsets()));
        }

        System.out.println("Total splits: " + splits.size());
        System.out.println("Index offset sum: " + offsetSum);
        System.out.println("Splits offset sum: " + splitOffsetSum);

//        Set<Long> missing = Sets.difference(indexOffsets, splitOffsets);
//        System.out.println("Total missing offsets: " + missing.size());
//        System.out.println("Missing offsets: " + missing);
    }
}
