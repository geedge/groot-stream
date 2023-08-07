package com.geedgenetworks.utils;

public class KnowLedgeEntity {

    private String sha256;
    private byte[] downloadBytes;
    private String tagType;
    private int isValid;
    private String fileUrl;

    public String  getSha256() {
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

    public String getTagType() {
        return tagType;
    }

    public void setTagType(String tagType) {
        this.tagType = tagType;
    }

    public int getIsValid() {
        return isValid;
    }

    public void setIsValid(int isValid) {
        this.isValid = isValid;
    }

    public String getFileUrl() {
        return fileUrl;
    }

    public void setFileUrl(String fileUrl) {
        this.fileUrl = fileUrl;
    }
}
