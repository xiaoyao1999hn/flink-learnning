package cn.chengjie.flink.common.utils;

import org.roaringbitmap.RoaringBitmap;

import java.io.*;

public class BitMapUtil {
    public static byte[] serialize(RoaringBitmap bitmap) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(512);
        DataOutputStream out = new DataOutputStream(byteArrayOutputStream);
        try {
            bitmap.runOptimize();
            bitmap.serialize(out);
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize bitmap ", e);
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static RoaringBitmap deserialize(byte[] bytes) {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(byteArrayInputStream);
        RoaringBitmap bitmap = new RoaringBitmap();
        try {
            bitmap.deserialize(in);
            return bitmap;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            return bitmap;
        }
    }
}
