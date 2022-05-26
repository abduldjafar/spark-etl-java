package koteka.spark.etl;

import io.delta.tables.DeltaTable;
import koteka.spark.datasources.Datasources;
import org.apache.spark.sql.*;
import org.json.JSONObject;


import java.util.HashMap;
import java.util.List;

public class Transformation {

    private static SparkSession session;
    private static Datasources datasources;

    public Transformation(SparkSession session) {
        this.session = session;
        this.datasources = new Datasources(session);
    }

    public Transformation() {}

    public Dataset < Row > createAccountsTableFromJsonLog(Dataset < Row > accountRawDataframe, String path) {
        Loads loads = new Loads();
        Extraction extraction = new Extraction(this.session);

        Dataset < Row > accountRawDataframeTemp = accountRawDataframe.select("data.*", "ts", "id", "savings_account_id", "card_id").where(functions.col("op").equalTo("c"));
        loads.delta_lake(accountRawDataframeTemp, path + "-temp");
        loads.delta_lake(accountRawDataframe, path);

        Dataset < Row > data = extraction.fulload_account_from_delta_lake(path + "-temp");


        List < Row > datas = accountRawDataframe.select("set", "ts", "id").withColumn("set", functions.to_json(functions.col("set")))
                .where(functions.col("set").isNotNull()).toJavaRDD().collect();


        DeltaTable deltaTable = DeltaTable.forPath(path + "-temp");


        for (Row row: datas) {

            JSONObject json = new JSONObject(row.get(0).toString());
            for (Object key: json.keySet().toArray()) {
                deltaTable.update(
                        functions.col("id").equalTo(row.get(2).toString()),
                        new HashMap < String, Column > () {
                            {
                                put((String) key, functions.lit((String) json.get(String.valueOf(key))));
                            }
                        }
                );
            }
        }


        return deltaTable.toDF();

    }
}