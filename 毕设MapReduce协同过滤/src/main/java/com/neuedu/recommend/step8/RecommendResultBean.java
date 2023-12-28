package com.neuedu.recommend.step8;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
public class RecommendResultBean implements DBWritable, WritableComparable<RecommendResultBean> {
    private Integer uid;    //用户id
    private Integer gid;    //商品id
    private Double nums;   //推荐度

    public RecommendResultBean() {
    }

    public RecommendResultBean(int uid, int gid, Double nums) {
        this.uid = uid;
        this.gid = gid;
        this.nums = nums;
    }

    public int getUid() {
        return uid;
    }

    public void setUid(int uid) {
        this.uid = uid;
    }

    public int getGid() {
        return gid;
    }

    public void setGid(int gid) {
        this.gid = gid;
    }

    public Double getNums() {
        return nums;
    }

    public void setNums(Double nums) {
        this.nums = nums;
    }

    //序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(uid);
        dataOutput.writeInt(gid);
        dataOutput.writeDouble(nums);

    }

    //反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.uid = dataInput.readInt();
        this.gid = dataInput.readInt();
        this.nums = dataInput.readDouble();
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setInt(1,uid);
        statement.setInt(2,gid);
        statement.setDouble(3,nums);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.uid = resultSet.getInt(1);
        this.gid = resultSet.getInt(2);
        this.nums = resultSet.getDouble(3);
    }

    @Override
    public int compareTo(RecommendResultBean o) {
        int n = this.uid.compareTo(o.uid);
        if (n != 0){
            return n;
        }else return this.gid.compareTo(o.gid);
    }
}