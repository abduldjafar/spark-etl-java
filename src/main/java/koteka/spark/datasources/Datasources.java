package koteka.spark.datasources;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

import static org.apache.spark.sql.functions.col;


public class Datasources {
    private static SparkSession session;

    public Datasources(SparkSession session) {
        this.session = session;
    }



    public  Dataset<Row> mongodb(String database, String collection){
        Dataset<Row> df = this.session.read().format("mongodb").option("database", database).option("collection", collection).load();
        return df;

    }
}