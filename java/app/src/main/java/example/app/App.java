package example.app;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.Optional;

import org.postgresql.ds.PGSimpleDataSource;

/**
 * CockroachDB Java连接示例应用
 * 演示如何使用PostgreSQL JDBC驱动连接CockroachDB并执行基本的SQL操作
 */
public class App {

    /**
     * 执行SQL查询语句并打印结果
     * 
     * @param conn 数据库连接对象
     * @param stmt 要执行的SQL语句
     */
    public static void executeStmt(Connection conn, String stmt) {
        try {
            // 创建SQL语句执行器
            Statement st = conn.createStatement();
            // 执行查询并获取结果集
            ResultSet rs = st.executeQuery(stmt);
            // 遍历结果集并打印第一列数据
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
            // 关闭结果集和语句对象以释放资源
            rs.close();
            st.close();
        } catch (Exception e) {
            // 捕获异常并静默返回（用于处理非查询语句）
            return;
        }
    }

    /**
     * SQL语句数组：按顺序执行的数据库操作
     * 1. 删除已存在的messages表
     * 2. 创建新的messages表，包含UUID主键和消息字段
     * 3. 向表中插入一条示例消息
     * 4. 查询并返回表中的所有消息
     */
    private static String[] statements = {
            // 清除任何现有数据
            "DROP TABLE IF EXISTS messages",
            // 创建messages表
            "CREATE TABLE IF NOT EXISTS messages (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), message STRING)",
            // 向messages表插入一行数据
            "INSERT INTO messages (message) VALUES ('Hello cockroachDB!')",
            // 从messages表查询数据
            "SELECT message FROM messages"
    };

    /**
     * 程序主入口方法
     * 
     * @param args 命令行参数
     */
    public static void main(String[] args) {
        try {
            // 创建PostgreSQL数据源对象
            PGSimpleDataSource ds = new PGSimpleDataSource();
            // 设置应用程序名称，用于数据库连接标识
            ds.setApplicationName("docs_quickstart_java");
            // 从环境变量获取数据库连接URL，如果未设置则抛出异常
            ds.setUrl(Optional.ofNullable(System.getenv("JDBC_DATABASE_URL")).orElseThrow(
                    () -> new IllegalArgumentException("JDBC_DATABASE_URL is not set.")));
            // 建立数据库连接
            Connection connection = ds.getConnection();
            // 依次执行所有SQL语句
            for (int n = 0; n < statements.length; n++) {
                executeStmt(connection, statements[n]);
            }
        } catch (Exception e) {
            // 捕获并打印任何异常信息
            e.printStackTrace();
        }
    }
}
