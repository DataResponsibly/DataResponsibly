import java.io.File
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by jugal112 on 5/18/17.
  */
object DataAnalysisUtils {
  def loadJSON(path: String): DataFrame = {
    val file = new File(path)
    val parent_path = file.getParent()
    val original_file_name = file.getName
    val parquet_file_name = FilenameUtils.removeExtension(original_file_name).concat(".parquet")
    val parquet_file = new File(parent_path, parquet_file_name)
    if (parquet_file.exists()) {
      val df = getSpark().sqlContext.read.parquet(parquet_file.getAbsolutePath())
      printf("Loading Cached Parquet File: %s", parquet_file.getAbsolutePath())
      return df
    }
    else {
      printf("Loading Json File: %s", path)
      val df = getSpark().sqlContext.read.json(path)
      printf("Creating Cached Parquet File: %s", parquet_file.getAbsolutePath())
      save(df, parquet_file.getAbsolutePath())
      return df
    }
  }

  def loadCSV(path: String) = {
    //Having trouble getting the csv library imported and working, but just model this around JSON
  }

  def save(df: DataFrame, path:String) = {
    df.write.parquet(path)
  }

  def getSpark(): SparkSession = {
    return SparkSession.builder()
      .master("local")
      .appName("Spark DataFrames Analysis")
      .getOrCreate()
  }
}
