package com.geedgenetworks.core.pojo;


public class OnlineStatistics {
    private long n;
    private double mean;
    private double aggregate;

    public OnlineStatistics add(Number val) {
        ++n;
        double delta = val.doubleValue() - mean;
        mean += delta / n;
        aggregate += (delta * (val.doubleValue() - mean));
        return this;
    }
    //计算总体标准差
    public double stddevp() {
        return Math.sqrt(variancep());
    }
    //计算总体方差
    public double variancep() {
        return aggregate / n;
    }
    //计算样本标准差
    public double stddev() {
        return Math.sqrt(variance());
    }
    //计算样本方差
    public double variance() {
        return aggregate / (n - 1);
    }

    public double mean() {
        return mean;
    }

}
