package com.cn.kuxun.storm;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class DateUtil {
        
        private static Log LOG = LogFactory.getLog(DateUtil.class);
        
        private static final long DAY = 24*3600*1000l;
        //private static String DEFAULT_FORMAT = "yyyy-MM-dd";
        
        /**
         * default_format:yyyy-MM-dd
         * */
        public static Date getDate(String day) throws ParseException {
                DateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
                Date today = fmt.parse(day);
                return today;
        }
        
        /**
         * default_format:yyyy-MM-dd
         * */
        public static Date getDate(String day, int diff) throws ParseException {
                DateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
                Date today = fmt.parse(day);
                Date result = null;
                if (diff > 0) {
                        result = new Date(today.getTime()+ Math.abs(diff)*DAY);
                } else {
                        result = new Date(today.getTime()- Math.abs(diff)*DAY);
                }
                return result;
        }
        
        public static String getDateStr(String day, int diff) throws ParseException {
                DateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
                Date today = fmt.parse(day);
                Date result = null;
                if (diff > 0) {
                        result = new Date(today.getTime()+ Math.abs(diff)*DAY);
                } else {
                        result = new Date(today.getTime()- Math.abs(diff)*DAY);
                }
                return fmt.format(result);
        }
        
        /**
         * default_format:yyyy-MM-dd
         * */
        public static String getDateStr(Date d) {
                DateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
                String result = fmt.format(d);
                return result;
        }
        
        /**
         * format:such as yyyy-MM-dd
         * */
        public static Date getDate(String day, int diff , String format) throws ParseException {
                DateFormat fmt = new SimpleDateFormat(format);
                Date today = fmt.parse(day);
                Date result = null;
                if (diff > 0) {
                        result = new Date(today.getTime()+ Math.abs(diff)*DAY);
                } else {
                        result = new Date(today.getTime()- Math.abs(diff)*DAY);
                }
                return result;
        }
        
        /**
         * format:such as yyyy-MM-dd
         * */
        public static String getDateStr(Date d, String format) {
                DateFormat fmt = new SimpleDateFormat(format);
                String result = fmt.format(d);
                return result;
        }
        
        
        public static Date getTime(String time, String format) throws ParseException {
                DateFormat fmt = new SimpleDateFormat(format);
                Date d = fmt.parse(time);
                return d;
        }
        
        public static Date getTime(String time) throws ParseException {
                
                DateFormat fmt = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.ENGLISH);
                Date d = fmt.parse(time);
                return d;
        }
        
        public static String convertFormat(String dt) throws ParseException {
                
                SimpleDateFormat srcSDF = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.ENGLISH);                        
                SimpleDateFormat destSDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
                
                String datetime = destSDF.format(srcSDF.parse(dt));
                
                return datetime;
        }
        
        public static String getToday() {
                Date d = new Date();
                DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                return df.format(d);
        }
        
        public static String getNow() {
                Date d = new Date();
                DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                return df.format(d);
        }
        
        public static String longToStringDate(String strLongDate){
                
                String strDate = null;
                
                try{
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        long longDate = Long.parseLong(strLongDate);
                        if(longDate != 0){
                                Date date = new Date(longDate);
                                strDate = sdf.format(date);
                        }
                }catch(Exception e){
                        LOG.error(e);
                }
                
                return strDate;
        }
        
        
        public static boolean isMonth(String monthStart, String monthEnd) {
                int mStart = Integer.parseInt(monthStart.substring(4, 6));
                int mEnd = Integer.parseInt(monthEnd.substring(4, 6));
                
                if (Math.abs(mStart-mEnd)<=1) {
                        return true;
                } else {
                        return false;
                }
        }
        
        
        public static String getTimeAdd(String dt, String type, int diff) throws ParseException {
                DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Calendar c = Calendar.getInstance();  
                Date now = df.parse(dt);
                c.setTime(now);
                
                if (type.equals("minute")) {
                        c.add(Calendar.MINUTE, diff);
                } else if (type.equals("hour")) {
                        c.add(Calendar.HOUR, diff);
                } else if (type.equals("day")) {
                        c.add(Calendar.DAY_OF_WEEK, diff);
                } else if (type.equals("month")) {
                        c.add(Calendar.MONTH, diff);
                }
                
                return df.format(c.getTime());
        }
        
        public static long getTimeDiff(String dt1, String dt2) throws ParseException {
                DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date d1 = df.parse(dt1);
                Date d2 = df.parse(dt2);
                
                return d1.getTime()-d2.getTime();
                
        }
        
        public static String getPreHour(){
                
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
                Date d = new Date();
                
                d.setTime(d.getTime() - 60*60*1000);
                
                return sdf.format(d);
                
        }
        
        public static String getMonth(){
                String today = getToday().replace("-", "");
                String month = today.substring(0, 6);
                return month;
        }
        
        public static void main(String[] args) throws ParseException {
//                System.out.println(getTimeAdd("2013-03-19 20:16:18","month",-1));
//                System.out.println(getTimeDiff("2013-03-19 20:16:18", "2013-03-19 20:00:00")/(1000*60));
                
                String a = "2013-07-17 20:14:00";
                System.out.println(a.substring(11));
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String now = sdf.format(new Date().getTime() - 10*60*1000);
                
                
                System.out.println(now);
        }
        
        public static boolean checkDate(String sourceDate){
                
        if(sourceDate == null || sourceDate.length()!= 10) return false;

        try {        
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            dateFormat.setLenient(false);
            return sourceDate.equals(dateFormat.format(dateFormat.parse(sourceDate)));
        } catch (ParseException e) {
                e.printStackTrace();
        }
        
        return false;
    }
        
        
}