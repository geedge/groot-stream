package com.geedgenetworks.utils;

public class KnowLedgeEntity {

    private String sha256;
    private byte[] downloadBytes;
    private String id;
    private int isValid;
    private String format;


    public String getSha256() {
        return sha256;
    }

    public void setSha256(String sha256) {
        this.sha256 = sha256;
    }

    public byte[] getDownloadBytes() {
        return downloadBytes;
    }

    public void setDownloadBytes(byte[] downloadBytes) {
        this.downloadBytes = downloadBytes;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getIsValid() {
        return isValid;
    }

    public void setIsValid(int isValid) {
        this.isValid = isValid;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }
}
