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

    public  Dataset<Row> fulload_from_mongodb(String database, String collection){
        Dataset<Row> datas = this.datasources.mongodb(database,collection);

        return  datas;
    }


}