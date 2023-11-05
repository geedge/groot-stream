package com.geedgenetworks.common.config;

public class CheckResult {
    private static final CheckResult SUCCESS = new CheckResult(true, "");
    private boolean success;
    private String msg;

    private CheckResult(boolean success, String msg) {
        this.success = success;
        this.msg = msg;
    }

    public static CheckResult success() {
        return SUCCESS;
    }

    public static CheckResult error(String msg) {
        return new CheckResult(false, msg);
    }

    public boolean isSuccess() {
        return this.success;
    }

    public String getMsg() {
        return this.msg;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof CheckResult)) {
            return false;
        } else {
            CheckResult other = (CheckResult)o;
            if (!other.canEqual(this)) {
                return false;
            } else if (this.isSuccess() != other.isSuccess()) {
                return false;
            } else {
                Object this$msg = this.getMsg();
                Object other$msg = other.getMsg();
                if (this$msg == null) {
                    if (other$msg != null) {
                        return false;
                    }
                } else if (!this$msg.equals(other$msg)) {
                    return false;
                }

                return true;
            }
        }
    }

    protected boolean canEqual(Object other) {
        return other instanceof CheckResult;
    }

    public int hashCode() {
        int PRIME = 59;
        int result = 1;
        result = result * PRIME + (this.isSuccess() ? 79 : 97);
        Object $msg = this.getMsg();
        result = result * PRIME + ($msg == null ? 43 : $msg.hashCode());
        return result;
    }

    public String toString() {
        return "CheckResult(success=" + this.isSuccess() + ", msg=" + this.getMsg() + ")";
    }
}
