package koteka.spark.etl;

import koteka.spark.datasources.Datasources;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.col;

public class Extraction {

    private static SparkSession session;
    private static Datasources datasources;

    public Extraction(SparkSession session) {
        this.session = session;
        this.datasources = new Datasources(session);
    }


    public Dataset < Row > fulload_account_from_json(String path) {
        this.datasources.json(path).sort(col("ts").asc())
                .withColumn("card_id", functions.lit(""))
                .withColumn("savings_account_id", functions.lit(""))
                .write().mode("overwrite").format("delta").save("delta-table/accounts");
        Dataset < Row > jsonDatas = this.datasources.delta_lake("delta-table/accounts");

        return jsonDatas;
    }

    public Dataset < Row > fulload_account_from_delta_lake(String path) {
        Dataset < Row > jsonDatas = this.datasources.delta_lake(path);
        return jsonDatas;
    }

}