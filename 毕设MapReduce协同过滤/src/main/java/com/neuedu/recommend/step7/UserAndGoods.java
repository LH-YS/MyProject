package com.neuedu.recommend.step7;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.io.WritableComparable;

public class UserAndGoods implements WritableComparable<UserAndGoods>{
	//bean类
	private String userId;
	private String goodsId;
	//flag 为1表示来自源数据，为0表示来自第六步的结果
	private int flag;
	public UserAndGoods() {
		
	}
	public UserAndGoods(String userId,String goodsId,int flag) {
		this.userId = userId;
		this.goodsId = goodsId;
		this.flag = flag;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public String getGoodsId() {
		return goodsId;
	}
	public void setGoodsId(String goodsId) {
		this.goodsId = goodsId;
	}
	public int getFlag() {
		return flag;
	}
	public void setFlag(int flag) {
		this.flag = flag;
	}
	@Override
    public int compareTo(UserAndGoods o) {

        int i = this.getUserId().compareTo(o.getUserId());
        //当用户i不相同时
        if (i != 0) {
            return i;
        } else return this.getGoodsId().compareTo(o.getGoodsId());
    }

    @Override
    public String toString() {
        return "UserAndGoods{" +
                "userId='" + userId + '\'' +
                ", goodsId='" + goodsId + '\'' +
                ", flag=" + flag +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(userId);
        dataOutput.writeUTF(goodsId);
        dataOutput.writeInt(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.userId = dataInput.readUTF();
        this.goodsId = dataInput.readUTF();
        this.flag = dataInput.readInt();
    }

	
}
