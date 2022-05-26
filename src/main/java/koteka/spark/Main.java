package koteka.spark;

import koteka.spark.etl.Extraction;
import koteka.spark.etl.Loads;
import koteka.spark.etl.Transformation;
import koteka.spark.init.Initialize;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {
        Initialize init = new Initialize("asoi", "local[*]", "");
        SparkSession session = init.startSpark();

        // extraction part
        Extraction extraction = new Extraction(session);
        Dataset < Row > accountsRaw = extraction.fulload_account_from_json("data/accounts/*.json");


        // transformation part
        Transformation transformation = new Transformation(session);
        Dataset < Row > accountTransformed = transformation.createAccountsTableFromJsonLog(accountsRaw, "delta-table/accounts-firts-step-tranformation");


        // loads to destination
        Loads loads = new Loads();
        loads.delta_lake(accountTransformed, "delta-table/accountsTransformed");


    }
}