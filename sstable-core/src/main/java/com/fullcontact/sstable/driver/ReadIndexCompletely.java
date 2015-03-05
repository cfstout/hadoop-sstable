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
import gnu.trove.list.array.TLongArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.List;

/**
 * Read a C* -Index.db file with custom hadoop based code.
 *
 * @author ben <ben.vanberg@fullcontact.com>
 */
public class ReadIndexCompletely {

    //    private static final String INDEX_FILE = "/Users/ben/code/hadoop-sstable/src/main/resources/profile_identities-results-ic-83679-Index.db";
    private static final String INDEX_FILE = "/Users/ben/code/hadoop-sstable/sstable-core/src/main/resources/searchtarget-results-jb-36177-Index.db";
//private static final String INDEX_FILE = "/Users/ben/sstables/profile_sample/profile_identities-results-ic-83679-Index.db";
//    private static final String INDEX_FILE = "/tmp/profile_identities-results-ic-121991-Index.db";

    public static void main(String[] args) {

        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.getLocal(new Configuration());
        } catch (IOException e) {
            e.printStackTrace();
        }

        final long startTrove = System.currentTimeMillis();

        int i = readWithTrove(fileSystem);

        final long endTrove = System.currentTimeMillis();

        System.out.println("Trove Total entries: " + i);
        System.out.println("Trove Time: " + (endTrove - startTrove));

//        final long startFastutil = System.currentTimeMillis();
//
//        int i = readWithFastutil(fileSystem);
//
//        final long endFastutil = System.currentTimeMillis();
//
//        System.out.println("Fastutil Total entries: " + i);
//        System.out.println("Fastutil Time: " + (endFastutil - startFastutil));
    }

    private static int readWithFastutil(FileSystem fileSystem) {
        List<SSTableSplit> splits = Lists.newArrayList();
        LongArrayList longArrayList = new LongArrayList(10000);

        IndexOffsetScanner indexScanner = new IndexOffsetScanner(INDEX_FILE, fileSystem);
        int i = 0;
        while (indexScanner.hasNext()) {
            if (longArrayList.size() == 10000) {
//                splits.add(new SSTableSplit(new Path(INDEX_FILE), longArrayList.toLongArray(), 10000, new String[]{}));
                splits.add(new SSTableSplit()); // just to compile...
                longArrayList.clear();
            }

            long offset = indexScanner.next();
            longArrayList.add(offset);
//            System.out.println("Index entry: " + offset);
            i++;
        }

        indexScanner.close();
        return i;
    }

    private static int readWithTrove(FileSystem fileSystem) {
        List<SSTableSplit> splits = Lists.newArrayList();
        TLongArrayList longArrayList = new TLongArrayList(10000);

        IndexOffsetScanner indexScanner = new IndexOffsetScanner(INDEX_FILE, fileSystem);
        int i = 0;
        while (indexScanner.hasNext()) {
            if (longArrayList.size() == 10000) {
//                splits.add(new SSTableSplit(new Path(INDEX_FILE), longArrayList.toArray(), 10000, new String[]{}));
                splits.add(new SSTableSplit()); // just to compile...
                longArrayList.clear();
            }

            long offset = indexScanner.next();
            longArrayList.add(offset);
            System.out.println("Index entry: " + offset);
            i++;
        }

        indexScanner.close();
        return i;
    }
}
