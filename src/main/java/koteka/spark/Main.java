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
        Dataset < Row > accountsRaw = extraction.fulload_account_from_json("data/accounts/*.json","/account");
        Dataset <Row> cardsRaw = extraction.fulload_account_from_json("data/cards/*.json","/cards");
        Dataset <Row> savingAccounts = extraction.fulload_account_from_json("data/savings_accounts/*.json","/saving_accounts");
        Dataset <Row> airbnbdatas = extraction.fulload_from_mongodb_notconn("sample_airbnb","listingsAndReviews");


        // transformation part
        Transformation transformation = new Transformation(session);
        //Dataset < Row > accountTransformed = transformation.createAccountsTableFromJsonLog(accountsRaw, "delta-table/accounts-firts-step-tranformation");
        Dataset<Row> accountHistoricalTable = transformation.createHistoricalTable(accountsRaw);
        Dataset<Row> cardsHistoricalTable = transformation.createHistoricalTable(cardsRaw);
        Dataset<Row> savingAccountsHistoricalTable = transformation.createHistoricalTable(savingAccounts);


        transformation.createTableFromJsonColumns(airbnbdatas,"host","delta-lake/airbnb/host");
        transformation.createTableFromJsonColumns(airbnbdatas,"availability","delta-lake/airbnb/availability");
        transformation.createTableFromJsonColumns(airbnbdatas,"review_scores","delta-lake/airbnb/review_scores");
        transformation.createTableFromJsonColumns(airbnbdatas,"images","delta-lake/airbnb/images");



        // loads to destination
        Loads loads = new Loads(session);
        //loads.delta_lake_gcs(accountHistoricalTable, "raijin-data-lake"); // load to delta lake

    }
}

/**
 * root
 *  |-- _id: string (nullable = true)
 *  |-- access: string (nullable = true)
 *  |-- accommodates: integer (nullable = true)
 *  |-- address: struct (nullable = true)
 *  |    |-- street: string (nullable = true)
 *  |    |-- suburb: string (nullable = true)
 *  |    |-- government_area: string (nullable = true)
 *  |    |-- market: string (nullable = true)
 *  |    |-- country: string (nullable = true)
 *  |    |-- country_code: string (nullable = true)
 *  |    |-- location: struct (nullable = true)
 *  |    |    |-- type: string (nullable = true)
 *  |    |    |-- coordinates: array (nullable = true)
 *  |    |    |    |-- element: double (containsNull = true)
 *  |    |    |-- is_location_exact: boolean (nullable = true)
 *  |-- amenities: array (nullable = true)
 *  |    |-- element: string (containsNull = true)
 *  |-- availability: struct (nullable = true)
 *  |    |-- availability_30: integer (nullable = true)
 *  |    |-- availability_60: integer (nullable = true)
 *  |    |-- availability_90: integer (nullable = true)
 *  |    |-- availability_365: integer (nullable = true)
 *  |-- bathrooms: decimal(2,1) (nullable = true)
 *  |-- bed_type: string (nullable = true)
 *  |-- bedrooms: integer (nullable = true)
 *  |-- beds: integer (nullable = true)
 *  |-- calendar_last_scraped: timestamp (nullable = true)
 *  |-- cancellation_policy: string (nullable = true)
 *  |-- cleaning_fee: decimal(5,2) (nullable = true)
 *  |-- description: string (nullable = true)
 *  |-- extra_people: decimal(6,2) (nullable = true)
 *  |-- first_review: timestamp (nullable = true)
 *  |-- guests_included: decimal(2,0) (nullable = true)
 *  |-- host: struct (nullable = true)
 *  |    |-- host_about: string (nullable = true)
 *  |    |-- host_has_profile_pic: boolean (nullable = true)
 *  |    |-- host_id: string (nullable = true)
 *  |    |-- host_identity_verified: boolean (nullable = true)
 *  |    |-- host_is_superhost: boolean (nullable = true)
 *  |    |-- host_listings_count: integer (nullable = true)
 *  |    |-- host_location: string (nullable = true)
 *  |    |-- host_name: string (nullable = true)
 *  |    |-- host_neighbourhood: string (nullable = true)
 *  |    |-- host_picture_url: string (nullable = true)
 *  |    |-- host_response_rate: integer (nullable = true)
 *  |    |-- host_response_time: string (nullable = true)
 *  |    |-- host_thumbnail_url: string (nullable = true)
 *  |    |-- host_total_listings_count: integer (nullable = true)
 *  |    |-- host_url: string (nullable = true)
 *  |    |-- host_verifications: array (nullable = true)
 *  |    |    |-- element: string (containsNull = true)
 *  |-- house_rules: string (nullable = true)
 *  |-- images: struct (nullable = true)
 *  |    |-- thumbnail_url: string (nullable = true)
 *  |    |-- medium_url: string (nullable = true)
 *  |    |-- picture_url: string (nullable = true)
 *  |    |-- xl_picture_url: string (nullable = true)
 *  |-- interaction: string (nullable = true)
 *  |-- last_review: timestamp (nullable = true)
 *  |-- last_scraped: timestamp (nullable = true)
 *  |-- listing_url: string (nullable = true)
 *  |-- maximum_nights: string (nullable = true)
 *  |-- minimum_nights: string (nullable = true)
 *  |-- monthly_price: decimal(7,2) (nullable = true)
 *  |-- name: string (nullable = true)
 *  |-- neighborhood_overview: string (nullable = true)
 *  |-- notes: string (nullable = true)
 *  |-- number_of_reviews: integer (nullable = true)
 *  |-- price: decimal(6,2) (nullable = true)
 *  |-- property_type: string (nullable = true)
 *  |-- review_scores: struct (nullable = true)
 *  |    |-- review_scores_accuracy: integer (nullable = true)
 *  |    |-- review_scores_checkin: integer (nullable = true)
 *  |    |-- review_scores_cleanliness: integer (nullable = true)
 *  |    |-- review_scores_communication: integer (nullable = true)
 *  |    |-- review_scores_location: integer (nullable = true)
 *  |    |-- review_scores_rating: integer (nullable = true)
 *  |    |-- review_scores_value: integer (nullable = true)
 *  |-- reviews: array (nullable = true)
 *  |    |-- element: string (containsNull = true)
 *  |-- reviews_per_month: integer (nullable = true)
 *  |-- room_type: string (nullable = true)
 *  |-- security_deposit: decimal(7,2) (nullable = true)
 *  |-- space: string (nullable = true)
 *  |-- summary: string (nullable = true)
 *  |-- transit: string (nullable = true)
 *  |-- weekly_price: decimal(6,2) (nullable = true)
 */