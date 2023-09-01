package com.geedgenetworks.pojo.ipfix;

import java.util.List;

public class Elements {
    private int version;
    private int PEN_number;
    private int domain_id;
    private List<Element> BASE_elements;
    private List<Element> SSL_elements;
    private List<Element> HTTP_elements;
    private List<Element> MAIL_elements;
    private List<Element> DNS_elements;
    private List<Element> DTLS_elements;
    private List<Element> QUIC_elements;
    private List<Element> FTP_elements;
    private List<Element> SIP_elements;
    private List<Element> RTP_elements;
    private List<Element> SSH_elements;
    private List<Element> BGP_elements;
    private List<Element> RDP_elements;
    private List<Element> Tunnels_elements;
    private List<Element> Stratum_elements;

    public Elements() {
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getPEN_number() {
        return PEN_number;
    }

    public void setPEN_number(int PEN_number) {
        this.PEN_number = PEN_number;
    }

    public int getDomain_id() {
        return domain_id;
    }

    public void setDomain_id(int domain_id) {
        this.domain_id = domain_id;
    }

    public List<Element> getBASE_elements() {
        return BASE_elements;
    }

    public void setBASE_elements(List<Element> BASE_elements) {
        this.BASE_elements = BASE_elements;
    }

    public List<Element> getSSL_elements() {
        return SSL_elements;
    }

    public void setSSL_elements(List<Element> SSL_elements) {
        this.SSL_elements = SSL_elements;
    }

    public List<Element> getHTTP_elements() {
        return HTTP_elements;
    }

    public void setHTTP_elements(List<Element> HTTP_elements) {
        this.HTTP_elements = HTTP_elements;
    }

    public List<Element> getMAIL_elements() {
        return MAIL_elements;
    }

    public void setMAIL_elements(List<Element> MAIL_elements) {
        this.MAIL_elements = MAIL_elements;
    }

    public List<Element> getDNS_elements() {
        return DNS_elements;
    }

    public void setDNS_elements(List<Element> DNS_elements) {
        this.DNS_elements = DNS_elements;
    }

    public List<Element> getDTLS_elements() {
        return DTLS_elements;
    }

    public void setDTLS_elements(List<Element> DTLS_elements) {
        this.DTLS_elements = DTLS_elements;
    }

    public List<Element> getQUIC_elements() {
        return QUIC_elements;
    }

    public void setQUIC_elements(List<Element> QUIC_elements) {
        this.QUIC_elements = QUIC_elements;
    }

    public List<Element> getFTP_elements() {
        return FTP_elements;
    }

    public void setFTP_elements(List<Element> FTP_elements) {
        this.FTP_elements = FTP_elements;
    }

    public List<Element> getSIP_elements() {
        return SIP_elements;
    }

    public void setSIP_elements(List<Element> SIP_elements) {
        this.SIP_elements = SIP_elements;
    }

    public List<Element> getRTP_elements() {
        return RTP_elements;
    }

    public void setRTP_elements(List<Element> RTP_elements) {
        this.RTP_elements = RTP_elements;
    }

    public List<Element> getSSH_elements() {
        return SSH_elements;
    }

    public void setSSH_elements(List<Element> SSH_elements) {
        this.SSH_elements = SSH_elements;
    }

    public List<Element> getBGP_elements() {
        return BGP_elements;
    }

    public void setBGP_elements(List<Element> BGP_elements) {
        this.BGP_elements = BGP_elements;
    }

    public List<Element> getRDP_elements() {
        return RDP_elements;
    }

    public void setRDP_elements(List<Element> RDP_elements) {
        this.RDP_elements = RDP_elements;
    }

    public List<Element> getTunnels_elements() {
        return Tunnels_elements;
    }

    public void setTunnels_elements(List<Element> tunnels_elements) {
        Tunnels_elements = tunnels_elements;
    }

    public List<Element> getStratum_elements() {
        return Stratum_elements;
    }

    public void setStratum_elements(List<Element> stratum_elements) {
        Stratum_elements = stratum_elements;
    }

    @Override
    public String toString() {
        return "Elements{" +
                "version=" + version +
                ", PEN_number=" + PEN_number +
                ", domain_id=" + domain_id +
                ", BASE_elements=" + BASE_elements +
                ", SSL_elements=" + SSL_elements +
                ", HTTP_elements=" + HTTP_elements +
                ", MAIL_elements=" + MAIL_elements +
                ", DNS_elements=" + DNS_elements +
                ", DTLS_elements=" + DTLS_elements +
                ", QUIC_elements=" + QUIC_elements +
                ", FTP_elements=" + FTP_elements +
                ", SIP_elements=" + SIP_elements +
                ", RTP_elements=" + RTP_elements +
                ", SSH_elements=" + SSH_elements +
                ", BGP_elements=" + BGP_elements +
                ", RDP_elements=" + RDP_elements +
                ", Tunnels_elements=" + Tunnels_elements +
                ", Stratum_elements=" + Stratum_elements +
                '}';
    }
}
