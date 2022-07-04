import logging

def read_csv_file(spark,file_path):
    """
        method to read the CSV files
        :param spark: SparkSession -> ~'pyspark.sql.SparkSession'
        :param file_path:  String -> CSV file path
        :return: DataFrame -> 'pyspark.sql.DataFrame'
    """
    try:
        data_df = spark.read\
            .format('csv') \
            .option('mode','DROPMALFORMED')\
            .option('inferSchema','true')\
            .option('header','true')\
            .option('path',file_path)\
            .load()
        return data_df
    except Exception as err:
        logging.error("%s, Error: %s", str(__name__), str(err))

def write_csv(df, file_path):
    """
    Write dataframe as csv
    :param df: dataframe
    :param file_path: output file path
    :return: None
    """
    print("Output_is_saved_to_file_location : {}".format(file_path))
    df.write.format('csv').mode('overwrite').option("header", "true").save(file_path)