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

import com.fullcontact.cassandra.io.sstable.SSTableIdentityIterator;
import com.google.common.collect.Lists;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.CFDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.CounterColumn;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletedColumn;
import org.apache.cassandra.db.ExpiringColumn;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.io.compress.CompressedRandomAccessReader;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Simple driver to test out reading data from the sstable in a manual fashion.
 *
 * @author ben <ben.vanberg@fullcontact.com>
 */
public class ReadSomeSSTableData {

    private static final String INDEX_FILE = "/Users/ben/code/hadoop-sstable/src/main/resources/profile_identities-results-ic-83679-Index.db";
    private static final String DATA_FILE = "/Users/ben/code/hadoop-sstable/src/main/resources/profile_identities-results-ic-83679-Data.db";

    // TODO: OMG: This whole class makes my eyes bleed!

    // TODO: Hardcoded hack to get things working.
    private static final String CQL = "CREATE TABLE results (storage_type text, storage_value text, version text, indexed_value text, last_update timestamp, PRIMARY KEY ((storage_type, storage_value), version)) WITH bloom_filter_fp_chance=0.010000 AND caching='KEYS_ONLY' AND comment='' AND dclocal_read_repair_chance=0.000000 AND gc_grace_seconds=864000 AND read_repair_chance=0.100000 AND replicate_on_write='true' AND populate_io_cache_on_flush='false' AND compaction={'class': 'SizeTieredCompactionStrategy'} AND compression={'sstable_compression': 'SnappyCompressor'}";

    // TODO: These hard coded types should be configured.
    private final AbstractType keyType = CompositeType.getInstance(Lists.<AbstractType<?>>newArrayList(UTF8Type.instance, UTF8Type.instance));
    private final AbstractType columnNameConvertor = CompositeType.getInstance(Lists.<AbstractType<?>>newArrayList(UTF8Type.instance, UTF8Type.instance, UTF8Type.instance));

    public static void main(String[] args) throws IOException {

        ReadSomeSSTableData readSomeSSTableData = new ReadSomeSSTableData();
        readSomeSSTableData.readSomeDatasWoohoo();

    }

    private void readSomeDatasWoohoo() throws IOException {
        // Get the index
        SSTableIndex index = SSTableIndex.readIndex(new Path(INDEX_FILE));
        long[] offsets = index.getOffsets();

        // Create the reader
        CompressionMetadata compressionMetadata = CompressionMetadata.create(DATA_FILE);
        CompressedRandomAccessReader reader = CompressedRandomAccessReader.open(DATA_FILE, compressionMetadata);

        // Iterate the index reading keys and values as we go.
        // For now lets just read the first 10
        for (int i = 0; i < 10; i++) {
            long offset = offsets[i];

            reader.seek(offset);

            ByteBuffer keyBytes = ByteBufferUtil.readWithShortLength(reader);
            System.out.println("Bytes[" + i + "]:\n" + keyBytes);

            String key = keyType.getString(keyBytes);

            // Oh fuck this is just...
            DecoratedKey decoratedKey = new RandomPartitioner().decorateKey(keyBytes);

            long dataSize = reader.readLong();

            // I hate doing this
            CreateTableStatement statement = null;
            try {
                statement = (CreateTableStatement) QueryProcessor.parseStatement(CQL).prepare().statement;
            } catch (RequestValidationException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            CFMetaData cfMetaData = new CFMetaData("assess", "kvs_strict", ColumnFamilyType.Standard, statement.comparator, null);
            try {
                statement.applyPropertiesTo(cfMetaData);
            } catch (RequestValidationException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            cfMetaData.rebuild();

            System.out.println("Index offset: " + offset + " Key: " + key + " Data size: " + dataSize);

            // Try with C* tools
            SSTableIdentityIterator ssTableIdentityIterator = new SSTableIdentityIterator(cfMetaData, reader, DATA_FILE, decoratedKey, dataSize, false, null, ColumnSerializer.Flag.LOCAL);

            String jsonProbably = getMapperValue(ssTableIdentityIterator, null, cfMetaData).toString();

            System.out.println("JSON Probably: \n" + jsonProbably);
        }
    }

    protected Text getMapperValue(SSTableIdentityIterator rowIter, Mapper.Context context, CFMetaData cfMetaData) {
        final StringBuilder sb = new StringBuilder(); // TODO don't like creating a ton of these!
        sb.append("{");
        insertKey(sb, keyType.getString(rowIter.getKey().key));
        sb.append("{");
//        insertKey(sb, "deletedAt");
//        sb.append("DELETED_AT_VALUE" keyType.);
//        sb.append(", ");
        insertKey(sb, "columns");
        sb.append("[");
        serializeColumns(sb, rowIter, context, cfMetaData);
        sb.append("]");
        sb.append("}}\n");
        return new Text(sb.toString());
    }

    private void serializeColumns(StringBuilder sb, SSTableIdentityIterator rowIter, Mapper.Context context, CFMetaData cfMetaData) {
        while (rowIter.hasNext()) {
            OnDiskAtom atom = rowIter.next();
            if (atom instanceof Column) {
                Column column = (Column) atom;
                String cn = getSanitizedName(column.name(), columnNameConvertor);
                sb.append("[\"");
                sb.append(cn);
                sb.append("\", \"");
                sb.append(JSONObject.escape(getColumnValueConvertor(handleCompositeColumnName(cn), BytesType.instance, cfMetaData).getString(column.value())));
                sb.append("\", ");
                sb.append(column.timestamp());

                if (column instanceof DeletedColumn) {
                    sb.append(", ");
                    sb.append("\"d\"");
                } else if (column instanceof ExpiringColumn) {
                    sb.append(", ");
                    sb.append("\"e\"");
                    sb.append(", ");
                    sb.append(((ExpiringColumn) column).getTimeToLive());
                    sb.append(", ");
                    sb.append(column.getLocalDeletionTime());
                } else if (column instanceof CounterColumn) {
                    sb.append(", ");
                    sb.append("\"c\"");
                    sb.append(", ");
                    sb.append(((CounterColumn) column).timestampOfLastDelete());
                }
                sb.append("]");
                if (rowIter.hasNext()) {
                    sb.append(", ");
                }
            } else if (atom instanceof RangeTombstone) {
                context.getCounter("Columns", "Range Tombstone Found").increment(1L);
            }
        }
    }

    protected static void insertKey(StringBuilder bf, String value) {
        bf.append("\"").append(value).append("\": ");
    }

    /**
     * Return the actual column name from a composite column name.
     * <p/>
     * i.e. key_1:key_2:key_3:column_name
     *
     * @param columnName
     * @return
     */
    private static String handleCompositeColumnName(final String columnName) {
        final String columnKey = columnName.substring(columnName.lastIndexOf(":") + 1);
        return columnKey.equals("") ? columnName : columnKey;
    }

    private static String getSanitizedName(ByteBuffer name, AbstractType convertor) {
        return convertor.getString(name)
                .replaceAll("[\\s\\p{Cntrl}]", " ")
                .replace("\\", "\\\\");
    }

    private static AbstractType getColumnValueConvertor(final String columnName, final AbstractType defaultType, CFMetaData cfMetaData) {
        final ColumnIdentifier colId = new ColumnIdentifier(columnName, false);
        final CFDefinition.Name name = cfMetaData.getCfDef().get(colId);

        if (name == null) {
            return defaultType;
        }

        final AbstractType<?> type = name.type;
        return type != null ? type : defaultType;
    }
}
