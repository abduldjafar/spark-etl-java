package koteka.spark;

import koteka.spark.etl.Extraction;
import koteka.spark.etl.Loads;
import koteka.spark.etl.Transformation;
import koteka.spark.init.Initialize;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.SQLException;

public class Main {
    public static void main(String[] args) throws SQLException {
        Initialize init = new Initialize("asoi", "local[*]", "");
        SparkSession session = init.startSpark();

        // extraction part
        Extraction extraction = new Extraction(session);

        Dataset <Row> airbnbdatas = extraction.fulload_from_mongodb("sample_airbnb","listingsAndReviews");


        // transformation part
        Transformation transformation = new Transformation(session);
        //Dataset < Row > accountTransformed = transformation.createAccountsTableFromJsonLog(accountsRaw, "delta-table/accounts-firts-step-tranformation");


        transformation.createTableFromJsonColumns(airbnbdatas,"host");
        transformation.createTableFromJsonColumns(airbnbdatas,"availability");
        transformation.createTableFromJsonColumns(airbnbdatas,"review_scores");
        transformation.createTableFromJsonColumns(airbnbdatas,"images");



        // loads to destination
        Loads loads = new Loads(session);
        //loads.delta_lake_gcs(accountHistoricalTable, "raijin-data-lake"); // load to delta lake

    }
}
