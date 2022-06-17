package koteka.spark.etl;

import io.delta.tables.DeltaTable;
import koteka.spark.datasources.Datasources;
import org.apache.spark.sql. * ;
import org.json.JSONObject;
import scala.Tuple2;

import java.util.Arrays;
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

    public Dataset<Row> createTableFromJsonColumns(Dataset<Row> datas, String columnname){

        datas.select("_id",columnname+".*");

        return datas.select("_id",columnname+".*");

    }
}