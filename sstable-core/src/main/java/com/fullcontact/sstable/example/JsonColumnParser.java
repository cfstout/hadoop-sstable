package com.fullcontact.sstable.example;

import com.fullcontact.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.CFDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;

import java.nio.ByteBuffer;

/**
 * Simple JSON column parser. Returns a C* row in JSON format.
 *
 * Adapted from Aegisthus JSON parsing.
 */
public class JsonColumnParser {
    private final CFDefinition cfd;
    private final AbstractType columnNameConverter;

    public JsonColumnParser(final CFMetaData cfMetaData) {
        this.cfd = cfMetaData.getCfDef();
        this.columnNameConverter = cfMetaData.comparator;
    }

    protected Text getJson(SSTableIdentityIterator rowIterator, Mapper.Context context) {
        final StringBuilder sb = new StringBuilder();
//        sb.append("{");
//        insertKey(sb, getColumnName(rowIterator.getKey().key, UTF8Type.instance));
        sb.append("{");
        insertKey(sb, "columns");
        sb.append("[");
        serializeColumns(sb, rowIterator, context);
        sb.append("]");
        sb.append("}\n");
        return new Text(sb.toString());
    }

    protected void insertKey(StringBuilder sb, String value) {
        sb.append("\"").append(value).append("\": ");
    }

    private String getColumnName(ByteBuffer name, AbstractType convertor) {
        return convertor.getString(name)
                .replaceAll("[\\s\\p{Cntrl}]", " ")
                .replace("\\", "\\\\");
    }

    private AbstractType getColumnValueConvertor(final String columnName, final AbstractType defaultType) {
        final ColumnIdentifier colId = new ColumnIdentifier(columnName, false);
        final CFDefinition.Name name = cfd.get(colId);

        if (name == null) {
            return defaultType;
        }
        if (colId.toString().equals("brands")) {
            return UTF8Type.instance;
        } else if (colId.toString().equals("interests")) {
            return UTF8Type.instance;
        } else if (colId.toString().equals("edrclient")) {
            return BooleanType.instance;
        }

        final AbstractType<?> type = name.type;
        return type != null ? type : defaultType;
    }

    /**
     * Return the actual column name from a composite column name.
     * <p/>
     * i.e. key_1:key_2:key_3:column_name
     *
     * for us it's timestamp_with_colon:column_name:bvid
     *
     * @param columnName
     * @return
     */
    private String handleCompositeColumnName(final String columnName) {
        if (columnName.contains(":")) {
            String columnKey = columnName.substring(columnName.indexOf(":") + 1);
            if (columnKey.contains(":") && columnKey.indexOf(":") != columnKey.lastIndexOf(":")) {
                final String ck = columnKey.substring(columnKey.indexOf(":") + 1, columnKey.lastIndexOf(":"));
                return ck.equals("") ? columnName : ck;
            }
        }
        return columnName;

    }

    private void serializeColumns(StringBuilder sb, SSTableIdentityIterator rowIterator, Mapper.Context context) {
        while (rowIterator.hasNext()) {
            OnDiskAtom atom = rowIterator.next();
            if (atom instanceof Column) {
                Column column = (Column) atom;
                String cn = handleCompositeColumnName(getColumnName(column.name(), columnNameConverter));
                sb.append("[\"");
                sb.append(cn);
                sb.append("\", \"");
                sb.append(JSONObject.escape(getColumnValueConvertor(handleCompositeColumnName(cn), BytesType.instance).getString(column.value())));
                sb.append("\"");
//                sb.append(column.timestamp());

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
                if (rowIterator.hasNext()) {
                    sb.append(", ");
                }
            } else if (atom instanceof RangeTombstone) {
                context.getCounter("Columns", "Range Tombstone Found").increment(1L);
            }
        }
    }
}
