import org.relique.jdbc.csv.CsvDriver;

import java.io.*;
import java.sql.*;
import java.util.Properties;


public class JdbcDemo {
    //一些驱动和需要连接到的
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String URL = "jdbc:mysql://localhost:3306?useUnicode=true&characterEncoding=UTF-8";
    private static final String URL2 = "jdbc:mysql://localhost:3306/jdbcdemo?useUnicode=true&characterEncoding=UTF-8";

    private static final String SQLITE_DRIVER = "org.sqlite.JDBC";
    private static final String USERNAME = "root";
    private static final String CSV_JDBC_DRIVER = "org.relique.jdbc.csv.CsvDriver";
    private static final String CSV_PATH = "data\\";
    private static final String SQL_PATH = "data\\creatTable.sql";
    private static final String SQLITE_PATH= "data\\room.db";
    public static void main(String[] args){
        //首先载入读取csv、sqlite等所需的驱动（初始化类）
        try{
            Class.forName(JDBC_DRIVER);
            Class.forName(CSV_JDBC_DRIVER);
            System.out.println("driver load successfully") ;
        }catch(ClassNotFoundException e){
            System.out.println("failed to load driver") ;
        }
        ResultSet csvRes = null;
        ResultSet sqliteRes = null;
        Connection sqliteCon = null;
        Statement sqliteStmt;
        Properties props = new Properties();
        props.put ("encoding", "UTF8");
        try {
            Class.forName(CSV_JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println(e);
        }


        Connection csvCon = null;
        Statement csvStmt = null;
        //得到连接和statement
        try {
            csvCon = DriverManager.getConnection("jdbc:relique:csv:" + CSV_PATH);
            csvStmt = csvCon.createStatement();
        //使用select语句读取数据到result（sqlite数据同理）
            csvRes = csvStmt.executeQuery("SELECT * FROM student");
            if (csvRes!=null){
                System.out.println("read csv successfully");
            }

            try {
                Class.forName(SQLITE_DRIVER);

                sqliteCon = DriverManager.getConnection("jdbc:sqlite:"+SQLITE_PATH,props);
                sqliteStmt = sqliteCon.createStatement();
                sqliteRes = sqliteStmt.executeQuery( "SELECT * FROM room;" );

                if (sqliteRes!=null){
                    System.out.println("read sqlite successfully");
                }
                Connection con;

                Statement stat = null;
                String sql;
                PreparedStatement ps = null;

                //得到mysql的连接，并执行创建数据表的sql
                con = DriverManager.getConnection(URL,USERNAME,"");


                con.setAutoCommit(false);
                try {
                    creatTable(con);
                }catch (SQLException e) {
                    System.out.println("create table failed");
                }

                //重新连接，精确到数据库jdbcdemo
                con = DriverManager.getConnection(URL2,USERNAME,"");
                con.setAutoCommit(false);
                int suc = 0, fail = 0;

                assert sqliteRes != null;
                //插入数据（使用预编译以加快速度）
                ResultSetMetaData sqliteMD = sqliteRes.getMetaData();
                StringBuilder preSql = new StringBuilder("INSERT INTO room (");
                for (int i = 1; i <= sqliteMD.getColumnCount(); i++) {
                    preSql.append(sqliteMD.getColumnName(i)).append(",");
                }
                preSql.delete(preSql.length()-1,preSql.length());
                preSql.append(") VALUES (");
                for (int i = 1; i < sqliteMD.getColumnCount(); i++) {
                    preSql.append("?,");
                }
                preSql.append("?)");
                // ps = con.prepareStatement("INSERT INTO student (" +") VALUES (?,?,?,?,?,?)");
                ps = con.prepareStatement(preSql.toString());


                while(sqliteRes.next()){
                    for (int i = 1; i <= sqliteMD.getColumnCount(); i++) {
                        if (sqliteRes.getBytes(i).length==0)
                            ps.setBytes(i,null);
                        else
                            ps.setBytes(i,sqliteRes.getBytes(i));
                    }
                    try {
                        suc++;
                        ps.executeUpdate();
                    }catch (SQLException e) {
                        System.out.println(e);
                        fail++;
                    }
                }
                con.commit();
                System.out.println("table room updated , success "+suc+" line, fail "+fail+" line");

                suc = 0;
                fail = 0;
                assert csvRes != null;
                ResultSetMetaData csvMD = csvRes.getMetaData();

                StringBuilder insertSql = new StringBuilder("INSERT INTO student (");
                for (int i = 1; i <= csvMD.getColumnCount(); i++) {
                    insertSql.append(csvMD.getColumnName(i)).append(",");
                }
                insertSql.delete(insertSql.length()-1,insertSql.length());
                insertSql.append(") VALUES (");
                for (int i = 1; i < csvMD.getColumnCount(); i++) {
                    insertSql.append("?,");
                }
                insertSql.append("?)");
               // ps = con.prepareStatement("INSERT INTO student (" +") VALUES (?,?,?,?,?,?)");
                ps = con.prepareStatement(insertSql.toString());
                while(csvRes.next()){
                    for (int i = 1; i <= csvMD.getColumnCount(); i++) {
                        if (csvRes.getBytes(i).length==0)
                            ps.setBytes(i,null);
                        else
                            ps.setBytes(i,csvRes.getBytes(i));
                    }
                    try {
                        ps.executeUpdate();
                        suc++;
                    }catch (SQLException e) {
                        System.out.println(e);
                        fail++;
                    }
                }
                con.commit();
                System.out.println("table student updated , success "+suc+" line, fail "+fail+" line");

                ps.close();
                con.close();


                sqliteStmt.close();
                sqliteCon.close();
            } catch ( Exception e ) {
                e.printStackTrace();
                System.exit(0);
            }
            csvStmt.close();
            csvCon.close();
        } catch (SQLException e) {
            System.out.println(e);
        }
    }
    private static void creatTable(Connection con)throws SQLException{
        Statement stat = con.createStatement();
        StringBuilder sql = new StringBuilder();
        BufferedReader reader;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(SQL_PATH), "UTF-8"));
            String line;
            while ((line=reader.readLine())!=null) {
                sql.append(line);
                if (line.endsWith(";")){
                    stat.addBatch(sql.toString());
                    sql.delete(0,sql.length());
                }
            }
            reader.close();
        } catch (Exception e) {
            System.out.println(e);
        }
        stat.executeBatch();
        con.commit();
    }


}
