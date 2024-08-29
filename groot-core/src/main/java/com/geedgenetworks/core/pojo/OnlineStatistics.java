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

    public OnlineStatistics merge(OnlineStatistics other) {
        if (other.n == 0) {
            return this; // Nothing to merge
        }
        if (this.n == 0) {
            this.n = other.n;
            this.mean = other.mean;
            this.aggregate = other.aggregate;
            return this;
        }

        // Combine counts
        long newN = this.n + other.n;

        // Calculate the new mean
        double delta = other.mean - this.mean;
        this.mean += delta * (other.n / (double) newN);

        // Update the aggregate
        this.aggregate += other.aggregate +
                (this.n * delta * delta) / newN;

        // Update the count
        this.n = newN;

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
