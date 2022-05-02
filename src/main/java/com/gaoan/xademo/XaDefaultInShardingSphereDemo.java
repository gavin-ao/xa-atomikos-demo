package com.gaoan.xademo;

import org.apache.shardingsphere.driver.api.yaml.YamlShardingSphereDataSourceFactory;
import org.apache.shardingsphere.transaction.core.TransactionType;
import org.apache.shardingsphere.transaction.core.TransactionTypeHolder;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

/**
 * Created by aoyonggang on 2022/5/2.
 */
public class XaDefaultInShardingSphereDemo {



    public static void main(String[] args) throws IOException, SQLException {
        DataSource dataSource = getShardingDatasource();
        initData(dataSource);

        //设置XA事务类型，默认就是atomikos
        TransactionTypeHolder.set(TransactionType.XA);

        Connection conn = dataSource.getConnection();
        String sql = "insert into t_order (user_id, order_id) VALUES (?, ?);";

        System.out.println("XA Start insert data to order");

        //在一个会话中，XA事务开启，同一个事务里，即使是正常数据，也一样会回滚掉
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);

            //数据库初始状态为空，以下模仿正常业务逻辑
            normalProcess(statement);

            //以下模仿出错业务逻辑
            iregularProcess(statement);

            conn.commit();
        } catch (Exception e) {
           e.printStackTrace();
            conn.rollback();
        } finally {
            conn.close();
        }

    }

    //模拟正常业务流程
    private static void normalProcess(PreparedStatement stmt) throws SQLException {
        stmt.setLong(1, 1);
        stmt.setLong(2, 1);
        stmt.executeUpdate();
    }

    //模拟异常业务流程
    private static void iregularProcess(PreparedStatement stmt) throws SQLException {
        stmt.setLong(1, 1);//重复主键，数据库报错
        stmt.setLong(2, 1);
        stmt.executeUpdate();
    }

    //初始化
    private static void initData(DataSource dataSource) {
        System.out.println("init all Data");
        try (Connection conn = dataSource.getConnection(); Statement statement = conn.createStatement()) {
            statement.execute("delete from t_order;");
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("int all Data successful");
    }

    /**
     * @return DataSource
     * @throws IOException
     * @throws SQLException
     */
    static private DataSource getShardingDatasource() throws IOException, SQLException {
        String fileName = "/Users/aoyonggang/Downloads/xa-atomikos-demo/src/main/resources/sharding-config.yaml";
        File yamlFile = new File(fileName);
        return YamlShardingSphereDataSourceFactory.createDataSource(yamlFile);
    }



}
