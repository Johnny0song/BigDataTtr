package com.atguigu.pojos;

public class WaterSensor {
    public String id;
    public Long ts;
    public Integer vc;

    public WaterSensor() {
    }

    public WaterSensor(String id, Long ts, Integer vc) {
        this.id = id;
        this.ts = ts;
        this.vc = vc;
    }

    public String getId() {
        return id;
    }

    public Long getTs() {
        return ts;
    }

    public Integer getVc() {
        return vc;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public void setVc(Integer vc) {
        this.vc = vc;
    }

    @Override
    public int hashCode() {
        return id.hashCode()+ts.hashCode()+vc.hashCode();
    }

    @Override
    public String toString() {
        return "WaterSensor{" +
                "id='" + id + '\'' +
                ", ts=" + ts +
                ", vc=" + vc +
                '}';
    }
}
