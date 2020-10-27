import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;

public class Main {
    static Connection connection;
    static Statement statement;
    static String content1, content2;
    static ArrayList<String> set1, set2;

    public static void main(String[] args) throws SQLException {
        init();
        int size[] = new int[]{2, 3, 5, 7, 9, 11, 19};
        for (int i = 0; i < size.length; i++) {
            int s = size[i];
            long r = 4;
            for (int j = 1; j < s; j++) {
                r *= 4;
            }
            System.out.println(s + " : " + r);
            set1 = new ArrayList<>();
            set2 = new ArrayList<>();
            if (s == 0) {
                for (int j = 0; j < content1.length() - s + 1; j++) {
                    String str = content1.substring(j, j + s);
                    if (!set1.contains(str))
                        set1.add(str);
                }
                for (int j = 0; j < content2.length() - s + 1; j++) {
                    String str = content2.substring(j, j + s);
                    if (!set2.contains(str))
                        set2.add(str);
                }

                statement.execute("create table genome_1_" + s + "(genome varchar unique);");
                statement.execute("create table genome_2_" + s + "(genome varchar unique);");
                for (int j = 0; j < set1.size() && j < r; j++) {
                    statement.execute("insert into genome_1_" + s + " values ('" + set1.get(j) + "');");
                }
                for (int j = 0; j < set2.size() && j < r; j++) {
                    statement.execute("insert into genome_2_" + s + " values ('" + set2.get(j) + "');");
                }
            }
            ResultSet rs = statement.executeQuery(
                    "WITH _t1 AS(" +
                            "    SELECT genome from genome_1_" + s + " INTERSECT ALL SELECT genome from genome_2_" + s + ")," +
                            "    _t2 AS(" +
                            "    SELECT genome from genome_1_" + s + " UNION ALL SELECT genome from genome_2_" + s + ")" +
                            "SELECT (SELECT count(*)::float from _t1) * 2 / (SELECT count(*)::float from _t2);"
            );
            rs.next();
            statement.execute("INSERT INTO genome_results (size, result) VALUES (" + s + ", " + rs.getString(1) + ");");
        }
    }

    public static void init() {
        try {
            connection = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5432/dblessons", "postgres", "12345678");
            statement = connection.createStatement();
            Class.forName("org.postgresql.Driver");
            content1 = new String(Files.readAllBytes(Paths.get("src/main/resources/Genome_1-1.txt")), StandardCharsets.US_ASCII);
            content2 = new String(Files.readAllBytes(Paths.get("src/main/resources/Genome_2-1.txt")), StandardCharsets.US_ASCII);
            statement.execute("create table genome_results(size int, result float);");
        } catch (IOException | SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
