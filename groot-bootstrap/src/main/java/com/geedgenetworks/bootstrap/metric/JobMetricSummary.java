package com.geedgenetworks.bootstrap.metric;

import com.geedgenetworks.common.utils.StringFormatUtil;
import com.geedgenetworks.utils.DateUtils;
import org.apache.flink.api.common.JobExecutionResult;

import java.time.Duration;
public final class JobMetricSummary {
    private final JobExecutionResult jobExecutionResult;
    private final Long jobStartTime;
    private final Long jobEndTime;

    JobMetricSummary(
            JobExecutionResult jobExecutionResult,
            long jobStartTime,
            long jobEndTime) {
        this.jobExecutionResult = jobExecutionResult;
        this.jobStartTime = jobStartTime;
        this.jobEndTime = jobEndTime;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private JobExecutionResult jobExecutionResult;

        private long jobStartTime;

        private long jobEndTime;

        private Builder() {}

        public Builder jobExecutionResult(JobExecutionResult jobExecutionResult) {
            this.jobExecutionResult = jobExecutionResult;
            return this;
        }

        public Builder jobStartTime(long jobStartTime) {
            this.jobStartTime = jobStartTime;
            return this;
        }

        public Builder jobEndTime(long jobEndTime) {
            this.jobEndTime = jobEndTime;
            return this;
        }

        public JobMetricSummary build() {
            return new JobMetricSummary(
                    jobExecutionResult,
                    jobStartTime,
                    jobEndTime);
        }
    }

    @Override
    public String toString() {
        return StringFormatUtil.formatTable(
                "Job Statistic Information",
                "Start Time",
                DateUtils.convertTimestampToString(jobStartTime, DateUtils.YYYY_MM_DD_HH_MM_SS),
                "End Time",
                DateUtils.convertTimestampToString(jobEndTime, DateUtils.YYYY_MM_DD_HH_MM_SS),
                "Total Time(s)",
                (jobEndTime - jobStartTime)/1000,
                "Total Read Count",
                jobExecutionResult
                        .getAllAccumulatorResults()
                        .get(MetricNames.SOURCE_RECEIVED_COUNT),
                "Total Write Count",
                jobExecutionResult.getAllAccumulatorResults().get(MetricNames.SINK_WRITE_COUNT),
                "Total Read Bytes",
                jobExecutionResult
                        .getAllAccumulatorResults()
                        .get(MetricNames.SOURCE_RECEIVED_BYTES),
                "Total Write Bytes",
                jobExecutionResult.getAllAccumulatorResults().get(MetricNames.SINK_WRITE_BYTES));
    }
}
