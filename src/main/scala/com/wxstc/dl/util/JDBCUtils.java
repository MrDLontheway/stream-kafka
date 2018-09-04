package com.wxstc.dl.util;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

/**
 * JDBC封装类
 * @author DL
 *
 */
public class JDBCUtils{
    private static DataSource dataSource  = null;
    //声明线程共享变量
    public static ThreadLocal<Connection> container = new ThreadLocal<Connection>();
    //配置说明，参考官方网址
    //http://blog.163.com/hongwei_benbear/blog/static/1183952912013518405588/
    public static String confile = "druid.properties";
    private static Connection conn;
    private static PreparedStatement ps;
    static{
        InputStream in = null;
        try {
            confile = JDBCUtils.class.getClassLoader().getResource("").getPath()+confile;
            System.out.println(confile);
            in = new BufferedInputStream(new FileInputStream(confile.substring(1)));
            Properties p = new Properties();
            p.load(in) ;
            dataSource = DruidDataSourceFactory.createDataSource(p);
            //dataSource.setConnectProperties(p);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("配置文件未找到！");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("配置文件读取异常！");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取数据连接
     * @return
     */
    public static Connection getConnection(){
        Connection conn =null;
        try{
            conn = dataSource.getConnection();
            System.out.println(Thread.currentThread().getName()+"连接已经开启......");
            container.set(conn);
        }catch(Exception e){
            System.out.println("连接获取失败");
            e.printStackTrace();
        }
        return conn;
    }
    /***获取当前线程上的连接开启事务*/
    public static void startTransaction(){
        Connection conn=container.get();//首先获取当前线程的连接
        if(conn==null){//如果连接为空
            conn=getConnection();//从连接池中获取连接
            container.set(conn);//将此连接放在当前线程上
            System.out.println(Thread.currentThread().getName()+"空连接从dataSource获取连接");
        }else{
            System.out.println(Thread.currentThread().getName()+"从缓存中获取连接");
        }
        try{
            conn.setAutoCommit(false);//开启事务
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    //提交事务
    public static void commit(){
        try{
            Connection conn=container.get();//从当前线程上获取连接if(conn!=null){//如果连接为空，则不做处理
            if(null!=conn){
                conn.commit();//提交事务
                System.out.println(Thread.currentThread().getName()+"事务已经提交......");
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }


    /***回滚事务*/
    public static void rollback(){
        try{
            Connection conn=container.get();//检查当前线程是否存在连接
            if(conn!=null){
                conn.rollback();//回滚事务
                container.remove();//如果回滚了，就移除这个连接
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    /***关闭连接*/
    public static void close(){
        try{
            Connection conn=container.get();
            if(conn!=null){
                conn.close();
                System.out.println(Thread.currentThread().getName()+"连接关闭");
            }
        }catch(SQLException e){
            throw new RuntimeException(e.getMessage(),e);
        }finally{
            try {
                container.remove();//从当前线程移除连接切记
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }
    //简单使用方式
    public static void main(String[] args) throws SQLException {
        //select查询
        /*Connection conn = JDBCUtils.getConnection();
        PreparedStatement ps = conn.prepareStatement("SELECT 1");
        ResultSet rs = ps.executeQuery();
        JDBCUtils.close();*/

        //update,insert,delete操作
        Connection conn2 = JDBCUtils.getConnection();
        //开启事务1
        JDBCUtils.startTransaction();
        System.out.println("执行事务操作111111111111111....");
        JDBCUtils.commit();
        //开启事务2
        JDBCUtils.startTransaction();
        System.out.println("执行事务操作222222222222....");
        JDBCUtils.commit();
        JDBCUtils.close();
        for (int i = 0; i < 2; i++) {
            new Thread(new Runnable() {

                public void run() {
                    Connection conn2 = JDBCUtils.getConnection();
                    for (int i = 0; i < 2; i++) {
                        JDBCUtils.startTransaction();
                        System.out.println(conn2);
                        System.out.println(Thread.currentThread().getName()+"执行事务操作。。。。。。。。。。。。。");
                        JDBCUtils.commit();
                    }
                    JDBCUtils.close();
                }
            }).start();
        }

    }

    /**
     * 清空表数据
     * @param tabName 表名称
     */
    public static void delete(String tabName){
        conn = JDBCUtils.getConnection();    // 首先要获取连接，即连接到数据库

        try {
            String sql = "delete from  "+tabName+";";
            System.out.println("删除数据的sql:"+sql);
            //预处理SQL 防止注入
            ps = conn.prepareStatement(sql);
            //执行
            ps.executeUpdate();
            //关闭流
            ps.close();
            conn.close();    //关闭数据库连接
        } catch (SQLException e) {
            System.out.println("删除数据失败" + e.getMessage());
        }
    }

    /**
     * 创建表
     * @param tabName 表名称
     * @param tab_fields  表字段
     */
    public static void createTable(String tabName, String[] tab_fields) {
        conn = getConnection();    // 首先要获取连接，即连接到数据库
        try {
            String sql = "create table "+tabName+"(id int auto_increment primary key not null";

            if(tab_fields!=null&&tab_fields.length>0){
                sql+=",";
                int length = tab_fields.length;
                for(int i =0 ;i<length;i++){
                    //添加字段
                    sql+=tab_fields[i].trim()+" varchar(50)";
                    //防止最后一个,
                    if(i<length-1){
                        sql+=",";
                    }
                }
            }
            //拼凑完 建表语句 设置默认字符集
            sql+=")DEFAULT CHARSET=utf8;";
            System.out.println("建表语句是："+sql);
            ps = conn.prepareStatement(sql);
            ps.executeUpdate(sql);
            ps.close();
            conn.close();    //关闭数据库连接
        } catch (SQLException e) {
            System.out.println("建表失败" + e.getMessage());
        }
    }
}