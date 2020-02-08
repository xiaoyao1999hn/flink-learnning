package cn.chengjie.flink.common.utils;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.Date;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * @author ChengJie
 */
public class StringUtils extends org.apache.commons.lang3.StringUtils {

    /**
     * 对象为null 则返回""字符串，否则调用toString 方法
     * @author: ColwnfishYang
     * @date: 2019/1/31
     */
    public static String valueOf(Object obj) {
        return Objects.isNull(obj) ? EMPTY : obj.toString();
    }

    /**
     * StringBuilder 替换字符串方法
     * YCD 2018年9月21日 17:03:54
     *
     * @param text
     * @param searchString
     * @param replacement
     */
    public static void replace(StringBuilder text, String searchString, String replacement) {
        int i = text.indexOf(searchString);
        int length = searchString.length();
        text.replace(i, i + length, replacement);
    }

    /**
     * StringBuilder 替换最后字符串方法
     * YCD 2018年9月21日 17:03:54
     *
     * @param text
     * @param searchString
     * @param replacement
     */
    public static void replaceLast(StringBuilder text, String searchString, String replacement) {
        int i = text.lastIndexOf(searchString);
        int length = searchString.length();
        text.replace(i, i + length, replacement);
    }

    /***
     * 下划线命名转为驼峰命名
     * YCD 2018年9月21日 17:03:54
     * @param para 下划线命名的字符串
     */

    public static String underlineToHump(String para) {
        StringBuilder res = new StringBuilder(para);
        // 插入时下划线时length 也会随着变化
        for (int i = 0; i < res.length();) {
            // 获取索引位的字符

            if (res.charAt(i) == '_') {
                res.deleteCharAt(i);
                char iChar = res.charAt(i);
                if (!Character.isUpperCase(iChar)) {
                    res.setCharAt(i, Character.toUpperCase(iChar));
                }
            }
            i ++;
        }
        return res.toString();
    }


    /***
     * 驼峰命名转为下划线命名
     * YCD 2018年9月21日 17:03:54
     * @param para 驼峰命名的字符串
     */

    public static String humpToUnderline(String para) {
        StringBuilder res = new StringBuilder(para);
        // 插入时下划线时length 也会随着变化
        for (int i = 0; i < res.length();) {
            // 获取索引位的字符
            char iChar = res.charAt(i);
            if (Character.isUpperCase(iChar)) {
                res.setCharAt(i, Character.toLowerCase(iChar));
                res.insert(i, '_');
                i += 2;
            } else {
                i ++;
            }
        }
        return res.toString();
    }

    /**
     * 将字符串转换成clazz 类型
     * @param str
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T> T tranToClass(String str, Class<T> clazz) {
        if (Integer.class.isAssignableFrom(clazz)){
            return (T)Integer.valueOf(str);
        } else if (Long.class.isAssignableFrom(clazz)){
            return (T)Long.valueOf(str);
        } else if (Short.class.isAssignableFrom(clazz)){
            return  (T)Short.valueOf(str);
        } else if (Boolean.class.isAssignableFrom(clazz)){
            return  (T)Boolean.valueOf(str);
        } else if (Float.class.isAssignableFrom(clazz)){
            return  (T)Float.valueOf(str);
        } else if (Double.class.isAssignableFrom(clazz)){
            return  (T)Double.valueOf(str);
        } else if (Character.class.isAssignableFrom(clazz)){
            return  (T)Character.valueOf(str.toCharArray()[0]);
        } else if (Byte.class.isAssignableFrom(clazz)) {
            return (T) Byte.valueOf(str);
        } else if (BigDecimal.class.isAssignableFrom(clazz)) {
            return (T)new BigDecimal(str);
        } else if (Date.class.isAssignableFrom(clazz)) {
            try {
                return (T)DateUtils.parseDate(str);
            } catch (ParseException e) {
                throw new RuntimeException(e.getMessage());
            }
        }
        return (T)str;
    }

    /** 
     * @Description: 字符串对象不为空除去前后空格
     * @Param str 字符串
     * @return String 处理结果
     * @Author: ColwnfishYang
     * @Date: 2018/10/12 
     */ 
    public static String notNullTrim(String str) {
        return str != null ? str.trim() : str;
    }

    /**
     * 字符串长度小于指定长度时，在字符串左边补位（repairStr）
     * @param str 字符串
     * @param len 长度
     * @param repairStr 补位字符串
     * @return 补位结果
     * @author: ColwnfishYang
     * @date: 2018/10/30
     */
    public static String repairStr(String str, int len, String repairStr) {
        int strLen = str.length();
        if (strLen < len) {
            while (strLen < len) {
                StringBuffer sb = new StringBuffer();
                // 左补 repairStr
                sb.append(repairStr).append(str);
                // 右补 repairStr
                // sb.append(str).append("0");
                str = sb.toString();
                strLen = str.length();
            }
        }
        return str;
    }

    public static Set<String> analysisVertexRidList(Set<String> pathList){

        //(#59:0).out[0](#61:0).out[0](#62:0).out[0](#63:0)
        //(#59:0).out[0](#61:0).out[0](#62:0).out[1](#64:0)
        Set<String> ridList=new HashSet<String>();

        for(String path:pathList){
            String[] temp=path.split("\\.");
            for(String s:temp) {
                System.out.println(s.substring(s.indexOf("(")+1, s.indexOf(")")));
                ridList.add(s.substring(s.indexOf("(")+1, s.indexOf(")")));
            }
        }
        return ridList;
    }

    /**
     * 将字符串转换成byte数组
     *
     * @param hexString 字符串。
     * @return byte数组
     */
    public static byte[] toByteArray(String hexString) {
        if (hexString.isEmpty())
            throw new IllegalArgumentException("this hexString must not be empty");

        hexString = hexString.toLowerCase();
        final byte[] byteArray = new byte[hexString.length() / 2];
        int k = 0;
        for (int i = 0; i < byteArray.length; i++) {//因为是16进制，最多只会占用4位，转换成字节需要两个16进制的字符，高位在先
            byte high = (byte) (Character.digit(hexString.charAt(k), 16) & 0xff);
            byte low = (byte) (Character.digit(hexString.charAt(k + 1), 16) & 0xff);
            byteArray[i] = (byte) (high << 4 | low);
            k += 2;
        }
        return byteArray;
    }

    public static String toHexString(byte[] byteArray) {
        if (byteArray == null || byteArray.length < 1)
            throw new IllegalArgumentException("this byteArray must not be null or empty");

        final StringBuilder hexString = new StringBuilder();
        for (int i = 0; i < byteArray.length; i++) {
            if ((byteArray[i] & 0xff) < 0x10)//0~F前面不零
                hexString.append("0");
            hexString.append(Integer.toHexString(0xFF & byteArray[i]));
        }
        return hexString.toString().toLowerCase();
    }

    public final static boolean vadilateNumberType(String str){
        if (str != null && !"".equals(str.trim()))
            return str.matches("^(-?\\d+)(\\.\\d+)?$");
        else
            return false;
    }
}
