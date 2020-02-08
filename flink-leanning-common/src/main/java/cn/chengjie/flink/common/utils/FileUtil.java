package cn.chengjie.flink.common.utils;

import java.io.*;
import java.util.UUID;

public class FileUtil {

    public static void uploadFile(byte[] file, String filePath, String fileName) throws Exception {
        File targetFile = new File(filePath);
        if (!targetFile.exists()) {
            targetFile.mkdirs();
        }
        FileOutputStream out = new FileOutputStream(filePath + fileName);
        out.write(file);
        out.flush();
        out.close();
    }

    public static boolean deleteFile(  File file) {
        // 如果文件路径所对应的文件存在，并且是一个文件，则直接删除
        if (file.exists()) {
            File files[] = file.listFiles();
            if (files != null) {
                for (File f : files) {
                    if (f.isDirectory()) { // 判断是否为文件夹
                        deleteFile(f);
                        try {
                            f.delete();
                        } catch (Exception e) {
                        }
                    } else {
                        if (f.exists()) { // 判断是否存在
                            deleteFile(f);
                            try {
                                f.delete();
                            } catch (Exception e) {
                            }
                        }
                    }
                }
            }
            return true;
        } else {
            return false;
        }
    }

    public static boolean deleteFileAndCreate(String fileName) {
        File file = new File(fileName);
        deleteFile(file);
        return file.mkdirs();
    }

    public static String renameToUUID(String fileName) {
        return UUID.randomUUID() + "." + fileName.substring(fileName.lastIndexOf(".") + 1);
    }

    public static void readToBuffer(StringBuffer buffer, File file) {
        InputStream in = null;
        String line; // 用来保存每行读取的内容
        BufferedReader reader = null;
        try {
            in = new FileInputStream(file);
            reader = new BufferedReader(new InputStreamReader(in));
            line = reader.readLine(); // 读取第一行
            while (line != null) { // 如果 line 为空说明读完了
                buffer.append(line); // 将读到的内容添加到 buffer 中
                buffer.append("\n"); // 添加换行符
                line = reader.readLine(); // 读取下一行
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                if (reader != null) {
                    in.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
