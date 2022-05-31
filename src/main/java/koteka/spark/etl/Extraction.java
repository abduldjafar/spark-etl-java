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

    public Dataset < Row > fulload_account_from_json(String pathsource,String pathdestination) {
        this.datasources.json(pathsource).sort(col("ts").asc())
                .withColumn("card_id", functions.lit(""))
                .withColumn("savings_account_id", functions.lit(""))
                .write().mode("overwrite")
                .format("delta")
                .save("delta-table"+pathdestination);

        Dataset < Row > jsonDatas = this.datasources.delta_lake("delta-table"+pathdestination);

        return jsonDatas;
    }

    public Dataset < Row > fulload_account_from_delta_lake(String path) {
        Dataset < Row > jsonDatas = this.datasources.delta_lake(path);
        return jsonDatas;
    }

    public  Dataset<Row> fulload_from_mongodb(String database, String collection){
        Dataset<Row> datas = this.datasources.mongodb(database,collection);
        datas.write().mode("overwrite")
                .format("delta")
                .save("delta-lake/mongodb/"+database+"/"+collection);

        Dataset < Row > jsonDatas = this.datasources.delta_lake("delta-lake/mongodb/"+database+"/"+collection);

        return  jsonDatas;
    }

    public  Dataset<Row> fulload_from_mongodb_notconn(String database, String collection){

        Dataset < Row > jsonDatas = this.datasources.delta_lake("delta-lake/mongodb/"+database+"/"+collection);

        return  jsonDatas;
    }

}