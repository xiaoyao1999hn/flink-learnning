package cn.chengjie.flink.common.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * hdfs
 *
 * @author ChengJie
 * @desciption
 * @date 2019/8/28 21:54
 **/
@Slf4j
public class HdfsFileUtils {

    private static final String HIVE_TEMP_FILE = ".hive-staging";

    /**
     * hdfs上创建文件
     *
     * @param conf
     * @param dst
     * @param contents
     */
    public static void createFile(Configuration conf, String dst, byte[] contents) {
        FileSystem fs = null;
        FSDataOutputStream outputStream = null;
        try {
            fs = FileSystem.get(conf);
            //目标路径
            Path dstPath = new Path(dst);
            //打开一个输出流
            outputStream = fs.create(dstPath);
            outputStream.write(contents);
            log.info("创建文件成功!");
        } catch (Exception e) {
            log.error("创建文件失败: {}", e.getMessage());
        } finally {
            IOUtils.closeStream(outputStream);
            closeFs(fs);
        }
    }

    /**
     * 上传本地文件到hdfs
     *
     * @param conf
     * @param src
     * @param dst
     */
    public static void uploadFile(Configuration conf, String src, String dst) {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            //本地上传文件路径
            Path srcPath = new Path(src);
            //hdfs目标路径
            Path dstPath = new Path(dst);
            //调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认为false
            fs.copyFromLocalFile(false, srcPath, dstPath);

            //打印文件路径
            log.info("上传成功,Upload to {}", conf.get("fs.default.name"));
        } catch (Exception e) {
            log.error("上传文件失败: {}", e.getMessage());
        } finally {
            closeFs(fs);
        }
    }

    /**
     * 重命名文件
     *
     * @param conf
     * @param oldName
     * @param newName
     */
    public static void rename(Configuration conf, String oldName, String newName) {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            Path oldPath = new Path(oldName);
            Path newPath = new Path(newName);
            boolean isok = fs.rename(oldPath, newPath);
            if (isok) {
                log.info("rename success!");
            } else {
                log.info("rename failure");
            }
        } catch (Exception e) {
            log.error("rename failure : ", e.getMessage());
        } finally {
            closeFs(fs);
        }
    }


    /**
     * 删除hdfs上的文件
     *
     * @param conf
     * @param filePath
     * @throws IOException
     */
    public static void delete(Configuration conf, String filePath) throws IOException {

        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            Path path = new Path(filePath);
            boolean isok = fs.deleteOnExit(path);
            if (isok) {
                log.info("delete ok!");
            } else {
                log.info("delete failure");
            }
        } catch (Exception e) {
            log.error("delete failure : {}", e.getMessage());
        } finally {
            closeFs(fs);
        }
    }

    /**
     * 在hdfs上创建文件夹
     *
     * @param conf
     * @param path
     */
    public static void mkdir(Configuration conf, String path) {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            Path srcPath = new Path(path);
            boolean isok = fs.mkdirs(srcPath);
            if (isok) {
                log.info("create " + path + " dir ok!");
            } else {
                log.info("create " + path + " dir failure");
            }
        } catch (Exception e) {

        } finally {
            closeFs(fs);
        }
    }

    /**
     * 读取hdfs上的文件
     *
     * @param conf
     * @param srcPath
     * @param distPath
     * @throws Exception
     */
    public static void downloadFile(Configuration conf, String srcPath,String distPath) throws Exception {
        FileSystem fs = null;
        try {
            //下载之前先清空整个目录
            FileUtil.deleteFileAndCreate(distPath);
            fs = FileSystem.get(conf);
            Path srcFilePath = new Path(srcPath);
            FileStatus[] fileList = fs.listStatus(new Path(srcPath));
            //如果存在多个文件则拉取多个，如果不存在则只下载当前的
            for(FileStatus fileStatus:fileList){
                //这里简单粗暴一点，如果是目录则直接过滤掉不处理
                if(fileStatus.isDirectory()){
                    downloadDic(fs,fileStatus,srcPath);
                }else{
                    download(fs,fileStatus.getPath(),distPath+"/"+srcFilePath.getName(),true);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            throw e;
        } finally {
            closeFs(fs);
        }
    }

    /**
     * 遍历下载目录下所有的文件
     * @param fs
     * @param file
     * @param path
     * @throws Exception
     */
    private static void downloadDic(FileSystem fs,FileStatus file,String path) throws Exception {
        if(file.getPath().toString().contains(HIVE_TEMP_FILE)){
            return;
        }
        path=path+"/"+file.getPath().getName();
        FileStatus[] fileList = fs.listStatus(file.getPath());
        for (FileStatus fileStatus:fileList){
            if(fileStatus.isDirectory()){
                downloadDic(fs,fileStatus,path);
            }else{
                download(fs,fileStatus.getPath(),path,false);
            }
        }
    }

    /**
     * 下载文件
     * @param fs
     * @param srcFilePath
     * @param distPath
     * @param isClose
     */
    public static void download(FileSystem fs, Path srcFilePath,String distPath,Boolean isClose) {
        InputStream in = null;
        FileOutputStream out = null;
        try {
            out=new FileOutputStream(distPath);
            //复制到标准输出流
            in = fs.open(srcFilePath);
            byte[] buffer=new byte[4096];
            int len=0;
            while((len=in.read(buffer))>0){
                out.write(buffer, 0, len);
                out.flush();
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            IOUtils.closeStream(out);
            IOUtils.closeStream(in);
            if(isClose){
                closeFs(fs);
            }
        }
    }

    public static FileSystem getFs(Configuration conf){
        try{
            return  FileSystem.get(conf);
        }catch (Exception e){
            log.error(e.getMessage());
        }
        return null;
    }

    /**
     * 遍历指定目录(direPath)下的所有文件
     */
    public static void getDirectoryFromHdfs(Configuration conf, String direPath) {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(direPath), conf);
            FileStatus[] filelist = fs.listStatus(new Path(direPath));
            for (int i = 0; i < filelist.length; i++) {
                log.info("_________" + direPath + "目录下所有文件______________");
                FileStatus fileStatus = filelist[i];
                log.info("Name:{}", fileStatus.getPath().getName());
                log.info("Size:{}", fileStatus.getLen());
                log.info("Path:{}", fileStatus.getPath());
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            closeFs(fs);
        }
    }

    public static void closeFs(FileSystem fs) {
        if (fs != null) {
            try {
                fs.close();
            } catch (IOException e) {
                log.error(e.getMessage());
            }
        }
    }


    public static void main(String[] args) throws IOException {
        String today = new SimpleDateFormat("yyyy-MM-dd").format(new Date());

        String localFilePath = "F:\\datafortag\\maimaimai\\quan-" + today;
        String hdfsFilePath = "/user/rec/maimaimai/upload_month=" + today.substring(0, 7) + "/upload_date=" + today + "/";
        System.out.println(localFilePath);
        System.out.println(hdfsFilePath);

        //"/user/rec/maimaimai/upload_month=2016-11/upload_date=2016-11-09/"
        //1、遍历指定目录(direPath)下的所有文件
        //getDirectoryFromHdfs("/user/rec/maimaimai");

        //2、新建目录
        //mkdir(hdfsFilePath);

        //3、上传文件
        //uploadFile(localFilePath, hdfsFilePath);
        //getDirectoryFromHdfs(hdfsFilePath);

        //4、读取文件
        //readFile("/user/rec/maimaimai/quan-2016-11-09");

        //5、重命名
//        rename("/user/rec/maimaimai/2016-11/2016-11-09/quan-2016-11-09", "/user/rec/maimaimai/2016-11/2016-11-09/quan-2016-11-08.txt");
//        getDirectoryFromHdfs("/user/rec/maimaimai/2016-11/2016-11-09");

        //6、创建文件，并向文件写入内容
        //byte[] contents =  "hello world 世界你好\n".getBytes();
        //createFile("/user/rec/maimaimai/2016-11/2016-11-09/test.txt",contents);
        //readFile("/user/rec/maimaimai/2016-11/2016-11-09/test.txt");

        //7、删除文件
        //delete("/user/rec/maimaimai/quan-2016-11-08.txt"); //使用相对路径
        //delete("test1");    //删除目录
    }
}
