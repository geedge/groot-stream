package com.geedgenetworks.core.utils.cn.common;

import inet.ipaddr.AddressSection;
import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.format.AddressDivisionSeries;
import inet.ipaddr.ipv4.IPv4AddressSection;
import inet.ipaddr.ipv6.IPv6AddressSection;

public class IPAddress implements Comparable<IPAddress> {

    private inet.ipaddr.IPAddress ipAddress;

    public IPAddress(String ip) throws AddressStringException {
        ipAddress = new IPAddressString(ip).getAddress();
    }

    public IPAddress(inet.ipaddr.IPAddress ipAddress) {
        this.ipAddress = ipAddress;
    }

    public inet.ipaddr.IPAddress getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(inet.ipaddr.IPAddress ipAddress) {
        this.ipAddress = ipAddress;
    }

    @Override
    public int compareTo(IPAddress that) {
        return compare(this.ipAddress.getSection(), that.ipAddress.getSection());
    }

    private int compare(AddressSection one, AddressSection two) {
        if (one == two) {
            return 0;
        }
        if (!one.getClass().equals(two.getClass())) {
            int result = mapGrouping(one) - mapGrouping(two);
            if (result != 0) {
                return result;
            }
        }
        if (one instanceof IPv6AddressSection) {
            IPv6AddressSection o1 = (IPv6AddressSection) one;
            IPv6AddressSection o2 = (IPv6AddressSection) two;
            int result = o2.addressSegmentIndex - o1.addressSegmentIndex;
            if (result != 0) {
                return result;
            }
        }

        return compareParts(one, two);
    }

    private int mapGrouping(AddressDivisionSeries series) {
        if (series instanceof IPv6AddressSection) {
            return 1;
        } else if (series instanceof IPv4AddressSection) {
            return -1;
        }
        return 0;
    }

    private int compareParts(AddressSection one, AddressSection two) {
        int segCount = one.getSegmentCount();
        for (int i = 0; i < segCount; i++) {
            int oneLower = one.getSegment(i).getSegmentValue();
            int twoLower = two.getSegment(i).getSegmentValue();
            int result = compareValues(oneLower, twoLower);
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }


    protected int compareValues(int oneLower, int twoLower) {
        return oneLower - twoLower;
    }

}
