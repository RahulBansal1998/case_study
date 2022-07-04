 
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import utility
import pyspark.sql.functions as F
import logging
from pyspark.sql.window import Window



class Vehicle_Accident_Analysis:
    def __init__(self, spark,config):
        self.spark = spark
        self.config = config
        self.df_charges = utility.read_csv_file(spark, self.config["charge_use"])
        self.df_damages = utility.read_csv_file(spark, self.config["damages_use"])
        self.df_endorse = utility.read_csv_file(spark, self.config["endorse_use"])
        self.df_primary_person = utility.read_csv_file(spark, self.config["primary_person_use"])
        self.df_units = utility.read_csv_file(spark, self.config["units_use"])
        self.df_restrict = utility.read_csv_file(spark, self.config["restrict_use"])

    def count_male_crash(self, output_path):
        """
        Description : Finds the crashes in which number of persons killed are male
        :param output_path: output file path
        :param output_format: Write file format
        :return: dataframe count
        """
        
        df_primary_person = self.df_primary_person.filter((self.df_primary_person.PRSN_GNDR_ID == "MALE") & (self.df_primary_person.DEATH_CNT == 1))
        utility.write_csv(df_primary_person, output_path)
        return df_primary_person.count()

    def count_2_wheeler_crash(self, output_path):
        """
        Description : Finds the crashes where the vehicle type was 2 wheeler.
        :param output_format: Write file format
        :param output_path: output file path
        :return: dataframe count
        """
        df_units = self.df_units.filter(self.df_units.VEH_BODY_STYL_ID.contains("MOTORCYCLE"))
        utility.write_csv(df_units, output_path)

        return df_units.count()

    def state_with_highest_female_accident(self, output_path):
        """
        Description : Finds state name with highest female crash
        :param output_format: Write file format
        :param output_path: output file path
        :return: state name with highest female crash
        """
        df = self.df_primary_person.filter(self.df_primary_person.PRSN_GNDR_ID == "FEMALE").\
                groupBy('DRVR_LIC_STATE_ID').agg(F.count('DRVR_LIC_STATE_ID')\
                .alias("state_count"))
        df = df.orderBy(df.state_count.desc()).limit(1)

        utility.write_csv(df, output_path)

        return df.select('DRVR_LIC_STATE_ID').collect()[0][0]

    def top_vehicle_contributing_to_injuries(self, output_path):
        """
        Description : Finds Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        :param output_format: Write file format
        :param output_path: output file path
        :return: Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        """
        df = self.df_units.filter(self.df_units.VEH_MAKE_ID != "NA")
        df = df.withColumn('CASUALTIES_CNT', df.TOT_INJRY_CNT + df.DEATH_CNT)
        df = df.filter(df.CASUALTIES_CNT>0).groupby("VEH_MAKE_ID").agg(F.count("VEH_MAKE_ID").alias("VEH_MAKE_COUNT")).\
            orderBy(F.col("VEH_MAKE_COUNT").desc())

        df_5_to_15 = df.limit(15).subtract(df.limit(5))
        utility.write_csv(df_5_to_15, output_path)

        return [veh[0] for veh in df_5_to_15.select("VEH_MAKE_ID").collect()]

    def top_ethnic_user_group_crash_for_each_body_style(self, output_path):
        """
        Description : Finds and show top ethnic user group of each unique body style that was involved in crashes
        :param output_format: Write file format
        :param output_path: output file path
        :return: None
        """
        partition_clause = Window.partitionBy("VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID").orderBy(F.col("count").desc())
        df = self.df_units.join(self.df_primary_person, on=['CRASH_ID'], how='inner')
        df = df.filter(F.col('VEH_BODY_STYL_ID').isin('NA', 'NOT REPORTED', 'UNKNOWN')==False). \
            filter(F.col('PRSN_ETHNICITY_ID').isin("NA", "UNKNOWN")==False). \
            groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").count(). \
            withColumn("DENSE_RANK_NO", F.dense_rank().over(partition_clause)).filter(F.col("DENSE_RANK_NO") == 1).drop("DENSE_RANK_NO", "count")

        utility.write_csv(df, output_path)

        return df.collect()

    def top_5_zip_codes_with_alcohols_as_reason_for_crash(self, output_path):
        """
        Description : Finds top 5 Zip Codes with the highest number crashes with alcohols as the contributing factor to a crash
        :param output_format: Write file format
        :param output_path: output file path
        :return: List of Zip Codes
        """
        df = self.df_units.join(self.df_primary_person, on=['CRASH_ID'], how='inner')
        df = df.dropna(subset=["DRVR_ZIP"]). \
            filter((df.CONTRIB_FACTR_1_ID.contains("ALCOHOL")) | (df.CONTRIB_FACTR_1_ID.contains("DRINKING")) | (df.CONTRIB_FACTR_2_ID.contains("ALCOHOL")) | (df.CONTRIB_FACTR_2_ID.contains("DRINKING")) | (df.CONTRIB_FACTR_P1_ID.contains("ALCOHOL")) | (df.CONTRIB_FACTR_P1_ID.contains("DRINKING"))). \
            groupBy('DRVR_ZIP').agg(F.count('DRVR_ZIP').alias('count_column')).orderBy(F.col('count_column').desc()).limit(5)
            
        utility.write_csv(df, output_path)

        return df.select('DRVR_ZIP').collect()

    def crash_ids_with_no_damage(self, output_path):
        """
        Description : Counts Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4
        and car avails Insurance.
        :param output_format: Write file format
        :param output_path: output file path
        :return: List of crash ids
        """
        df = self.df_damages.join(self.df_units, on=["CRASH_ID"], how='inner')
        df = df.filter((df.VEH_DMAG_SCL_1_ID == "DAMAGED 4") | (df.VEH_DMAG_SCL_1_ID == "DAMAGED 5") \
                     | (df.VEH_DMAG_SCL_1_ID == "DAMAGED 6") | (df.VEH_DMAG_SCL_1_ID == "DAMAGED 7 HIGHEST")\
                     | (df.VEH_DMAG_SCL_2_ID == "DAMAGED 4") | (df.VEH_DMAG_SCL_2_ID == "DAMAGED 5") \
                     | (df.VEH_DMAG_SCL_2_ID == "DAMAGED 6") | (df.VEH_DMAG_SCL_2_ID == "DAMAGED 7 HIGHEST"))
        df = df.filter((df.DAMAGED_PROPERTY == "NONE") & (df.FIN_RESP_TYPE_ID == "PROOF OF LIABILITY INSURANCE"))
        df = df.dropDuplicates(["CRASH_ID"])
        utility.write_csv(df, output_path)

        return df.count()

    def top_5_vehicle_brand(self, output_path):
        """
        Description : Determines the Top 5 Vehicle Makes/Brands where drivers are charged with speeding related offences, has licensed
        Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of
        offences
        :param output_format: Write file format
        :param output_path: output file path
        :return List of Vehicle brands
        """
        top_color_df = self.df_units.groupBy('VEH_COLOR_ID').count().orderBy(F.col('count').desc()).limit(10)
        color_list = [row[0] for row in top_color_df.select('VEH_COLOR_ID').collect()]

        top_state_df = self.df_units.groupBy('VEH_LIC_STATE_ID').count().orderBy(F.col('count').desc()).limit(25)
        state_list = [row[0] for row in top_state_df.select('VEH_LIC_STATE_ID').collect()]

        # drivers are charged with speeding related offences
        df_speed = self.df_charges.join(self.df_endorse, 'CRASH_ID').drop(self.df_endorse.CRASH_ID)\
            .filter((F.col('CHARGE').contains('SPEED'))
                    & (~F.col('DRVR_LIC_ENDORS_ID').isin(['UNKNOWN', 'UNLICENSED', 'NONE'])))

        required_columns = df_speed.join(self.df_units, 'CRASH_ID')\
            .select(df_speed.CRASH_ID, self.df_units.VEH_COLOR_ID, self.df_units.VEH_LIC_STATE_ID, self.df_units.VEH_MAKE_ID)\
            .filter(self.df_units.VEH_MAKE_ID != 'NA')
        df = required_columns\
            .filter((F.col('VEH_LIC_STATE_ID').isin(state_list))
                    & (F.col('VEH_COLOR_ID').isin(color_list)))\
            .groupBy('VEH_MAKE_ID').count().orderBy(F.col('count').desc()).limit(5)


        utility.write_csv(df, output_path)

        return df.select('VEH_MAKE_ID').collect()




    


    
