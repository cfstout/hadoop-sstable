package com.fullcontact.sstable.bazaarvoice;

import com.google.common.collect.Lists;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.CFDefinition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.commons.lang.builder.CompareToBuilder;

import java.io.Serializable;
import java.util.List;

public class BvBrandInterest implements Serializable, Cloneable, Comparable<BvBrandInterest> {

    private String bvid;
    private List<String> rows;
    private final CFDefinition cfd;
    private final AbstractType columnNameConverter;


    public BvBrandInterest(final CFMetaData cfMetaData) {
        rows = Lists.newArrayList();
        this.cfd = cfMetaData.getCfDef();
        this.columnNameConverter = cfMetaData.comparator;
    }

    public BvBrandInterest(String id, List<String> rows, final CFMetaData cfMetaData) {
        this.bvid = id;
        this.rows = rows;
        this.cfd = cfMetaData.getCfDef();
        this.columnNameConverter = cfMetaData.comparator;
    }

    /**
     * Performs a deep copy on <i>other</i>
     */
    public BvBrandInterest(BvBrandInterest other) {
        this.bvid = other.bvid;
        this.rows = Lists.newArrayList(other.rows);
        this.cfd = other.cfd;
        this.columnNameConverter = other.columnNameConverter;
    }

    public BvBrandInterest deepCopy() {
        return new BvBrandInterest(this);
    }

    public String getBvid() {
        return bvid;
    }

    public void setBvid(String bvid) {
        this.bvid = bvid;
    }

    public List<String> getRows() {
        return rows;
    }

    public void setRows(List<String> rows) {
        this.rows = rows;
    }

    public void addRow(String row) {
        this.rows.add(row);
    }

    @Override
    public boolean equals(Object that) {
        if (that == null) {
            return false;
        }
        if (that instanceof BvBrandInterest) {
            return this.equals((BvBrandInterest) that);
        }
        return false;
    }

    public boolean equals(BvBrandInterest that) {
        if (that == null) {
            return false;
        }
        if (!this.bvid.equals(that.bvid)) {
            return false;
        } if (!this.rows.equals(that.rows)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = bvid != null ? bvid.hashCode() : 0;
        result = 31 * result + rows.hashCode();
        return result;
    }

    @Override
    public int compareTo(BvBrandInterest other) {
        return new CompareToBuilder()
                .append(this.bvid, other.bvid)
                .append(this.rows, other.rows)
                .toComparison();
    }

    @Override
    public String toString() {
        return "BvBrandInterest(" +
                "bvid:'" + bvid + '\'' +
                ", rows=" + rows.toString() +
                ')';
    }
}
