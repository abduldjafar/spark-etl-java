package koteka.spark.etl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Loads {
    public  void delta_lake(Dataset<Row> dataset, String path){
        dataset.write().mode("overwrite").option("overwriteSchema", "true").format("delta").save(path);

    }

}
