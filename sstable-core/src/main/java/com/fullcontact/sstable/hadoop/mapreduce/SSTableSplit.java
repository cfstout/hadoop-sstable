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

package com.fullcontact.sstable.hadoop.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * SSTable split definition.
 *
 * @author ben <ben.vanberg@fullcontact.com>
 */
public class SSTableSplit extends InputSplit implements Writable {

    private long[] offsets = new long[0];
    private long length;
    private long endOffset;
    private Path file;
    private String[] hosts;

    public SSTableSplit() {
    }

    /**
     * Constructs a split with host information
     *
     * @param file  the file name
     * @param hosts the list of hosts containing the block, possibly null
     */
    public SSTableSplit(final Path file, final long[] offsets, final long endOffset, final long length, final String[] hosts) {
        this.file = file;
        this.offsets = offsets;
        this.endOffset = endOffset;
        this.length = length;
        this.hosts = hosts;
    }

    @Override
    public String toString() {
        return "SSTableSplit{" +
                "offsets=" + Arrays.toString(offsets) +
                ", length=" + length +
                ", end=" + endOffset +
                ", file=" + file +
                ", hosts=" + Arrays.toString(hosts) +
                '}';
    }

    public long getOffsetCount() {
        return offsets.length;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return length;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, file.toString());
        out.writeLong(length);
        out.writeInt(offsets.length);
        for (int i = 0; i < offsets.length; i++) {
            out.writeLong(offsets[i]);
        }
        out.writeLong(endOffset);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        file = new Path(Text.readString(in));
        length = in.readLong();
        final int numberOffsets = in.readInt();
        offsets = new long[numberOffsets];
        for (int i = 0; i < numberOffsets; i++) {
            offsets[i] = in.readLong();
        }
        endOffset = in.readLong();
        hosts = null;
    }

    @Override
    public String[] getLocations() throws IOException {
        if (this.hosts == null) {
            return new String[]{};
        } else {
            return this.hosts;
        }
    }

    public Path getPath() {
        return file;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public int getSize() {
        return (int) (offsets[offsets.length - 1] - offsets[0]);
    }

    public long[] getOffsets() {
        return offsets;
    }

    public long getFirstOffset() {
        return offsets[0];
    }

    public long getLastOffset() {
        return offsets[offsets.length - 1];
    }

    public long getOffset(final int index) {
        return this.offsets[index];
    }

    public long getEndForOffset(final int index) {
        final int endIndex = index + 1;
        if(endIndex > offsets.length) {
            throw new IllegalArgumentException("End index cannot be larger than the total number of offsets");
        }
        if(endIndex < offsets.length) {
            return this.offsets[index];
        } else {
            return this.endOffset;
        }
    }
}
