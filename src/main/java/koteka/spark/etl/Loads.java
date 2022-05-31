package koteka.spark.etl;

import com.clickhouse.jdbc.ClickHouseDriver;
import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

public class Loads {

    private static SparkSession session;

    public Loads(SparkSession session){
        this.session = session;
    }
    public  void delta_lake(Dataset<Row> dataset, String path){
        dataset.show();
        dataset.write().mode("overwrite").option("overwriteSchema", "true").format("delta").save(path);
        DeltaTable deltaTable = DeltaTable.forPath(path);
        deltaTable.generate("symlink_format_manifest");

    }

    public  void delta_lake_gcs(Dataset<Row> dataset, String path){
        ProcessBuilder processBuilder = new ProcessBuilder();
        Map<String, String> env = processBuilder.environment();
        env.put("GOOGLE_APPLICATION_CREDENTIALS", "/Users/kotekaman/IdeaProjects/spark-java-etl/key.json");

        dataset.show();
        dataset.write().mode("overwrite").option("overwriteSchema", "true").format("delta").save("gs://"+path+"/asoi");

    }



}
