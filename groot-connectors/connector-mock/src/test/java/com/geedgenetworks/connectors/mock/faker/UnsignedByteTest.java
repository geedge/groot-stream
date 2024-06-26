package com.geedgenetworks.connectors.mock.faker;

public class UnsignedByteTest {

    public static void main(String args[]) {
        int a = -127 & 0xff;
        System.out.println(a);

        short[] shorts=new short[256];
        for(int i=0;i<shorts.length;i++)
            shorts[i]=(short)i;
        byte[] bytes=new byte[256];
        for(int i=0;i<bytes.length;i++)
            //将short数组的数据存到byte数组中
            bytes[i]=toUnsignedByte(shorts[i]);
        for(int i=0;i<bytes.length;i++)
            //从byte数组中取出无符号的byte
            System.out.println(bytes[i] + "-" + getValue(bytes[i]));
    }

    public static short getValue(byte i) {
        short li = (short) (i & 0xff);
        return li;
    }
    public static byte toUnsignedByte(short i) {
        return (byte) (i & 0xff);
    }
}
