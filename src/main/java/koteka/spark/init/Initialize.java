package koteka.spark.init;

import org.apache.spark.sql.SparkSession;

public class Initialize {
    private static String app_name;
    private static String master;
    private String spark_config_file;

    public Initialize(String app_name, String master, String spark_config_file) {
        this.app_name = app_name;
        this.master = master;
        this.spark_config_file = spark_config_file;

    }

    public SparkSession startSpark() {
        if (this.master == "local[*]") {
            SparkSession spark = SparkSession
                    .builder()
                    .appName("Java Spark SQL basic example")
                    .config("spark.master", "local")
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    .config("spark.mongodb.read.connection.uri", "mongodb+srv://kotekaman:Robonson10@cluster0.36shw.mongodb.net/?retryWrites=true&w=majority")
                    .config("spark.mongodb.write.connection.uri", "mongodb+srv://kotekaman:Robonson10@cluster0.36shw.mongodb.net/?retryWrites=true&w=majority")
                    .getOrCreate();
            return spark;
        } else {
            return null;
        }
    }
}