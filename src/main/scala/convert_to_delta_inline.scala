import io.delta.tables._;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.DataFrame ;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;


object convert_to_delta_inline {
	def main(args : Array[String]) : Unit = {
        var spark = SparkSession.builder().appName("delta").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").enableHiveSupport().getOrCreate()
	//spark.sql("select * from dsmsca_staging.ip_bm_inventory_daily_base_t").show()
        val treatment_partition_schema = "account_type string,supergeo_cd string,retailer_id string,snapshot_dt string"
        DeltaTable.convertToDelta(spark,"parquet.`s3://zz-testing/jcher2/po_base/`",treatment_partition_schema)
        }
	

}

