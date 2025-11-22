package com.atguigu.mapreduce.shfulle.partitionercomparable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {
    private Long upFlow;
    private Long downFlow;
    private Long sumFlow;

    public FlowBean() {
    }

    public Long getUpFlow() {
        return upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = this.upFlow + this.downFlow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    @Override
    public String toString() {
        return  upFlow +
                "\t" + downFlow +
                "\t" + sumFlow ;
    }

    @Override
    public int compareTo(FlowBean o) {

        // 总流量降序
        if (this.sumFlow != o.getSumFlow()) {
            return this.sumFlow > o.getSumFlow() ? -1 : 1;
        }

        // 上行流量降序
        if (this.upFlow != o.getUpFlow()) {
            return this.upFlow > o.getUpFlow() ? -1 : 1;
        }

        // 下行流量降序
        if (this.downFlow != o.getDownFlow()) {
            return this.downFlow > o.getDownFlow() ? -1 : 1;
        }

        return 0;
    }
}
