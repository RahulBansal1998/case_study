import argparse
from distutils.command.config import config
from pyspark.sql import SparkSession
import logging
import json
from pyspark.sql.functions import *
import Veh_Accident_analysis


def get_argument_parse():
    """
    Description : Get Arguments 
    :return : Return all run time arguments
    """
    parser = argparse.ArgumentParser(description='Vehicle Accident Analysis BCG Gamma Case Study')
    parser.add_argument('-i','--inp_config',dest = 'config',default='config/config.json',help='Input config')
    parser.add_argument('-k','--key',required =True,help='Output keys in json')
    args = parser.parse_args()
    return args


def get_results(spark,config,args):
    """
    Description : Get results from Module Veh_Accident_analysis
    :param spark: spark session
    :param config: input config file 
    :param args: Runtime Argument
    :return: Analysis Result
    """
    vehicle_accident_analysis_obj = Veh_Accident_analysis.Vehicle_Accident_Analysis(spark,config)

    if(args.key == 'count_male'):
        # Q1. Find the number of crashes in which number of persons killed are male?
        print("The number of crashes in which number of persons killed are male:", vehicle_accident_analysis_obj.count_male_crash(config["output_file_path"][args.key]))

    elif (args.key == 'count_2_wheeler_crash'):
        # Q2. How many two-wheelers are booked for crashes?
        print("The number of two-wheelers are booked for crashes :", vehicle_accident_analysis_obj.count_2_wheeler_crash(config["output_file_path"][args.key]))

    elif (args.key == 'state_with_highest_female_accident'):
        # Q3. Which state has the highest number of crash in which females are involved?
        print("The state has the highest number of crash in which females are involved :", vehicle_accident_analysis_obj.state_with_highest_female_accident(config["output_file_path"][args.key]))

    elif (args.key == 'top_vehicle_contributing_to_injuries'):
        # Q4. Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        print("The Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death:", vehicle_accident_analysis_obj.top_vehicle_contributing_to_injuries(config["output_file_path"][args.key]))

    elif (args.key == 'top_ethnic_user_group_crash_for_each_body_style'):
        # Q5. For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
        print("The body styles involved in crashes, mention the top ethnic user group of each unique body style:",vehicle_accident_analysis_obj.top_ethnic_user_group_crash_for_each_body_style(config["output_file_path"][args.key]))

    elif (args.key == 'top_5_zip_codes_with_alcohols_as_reason_for_crash'):
        # Q6. Among the crashed cars, what are the Top 5 Zip Codes with the highest number crashes with alcohols as the
        # contributing factor to a crash (Use Driver Zip Code)
        print("The Top 5 Zip Codes with the highest number crashes with alcohols as CF :", vehicle_accident_analysis_obj.top_5_zip_codes_with_alcohols_as_reason_for_crash(config["output_file_path"][args.key]))

    elif (args.key == 'crash_ids_with_no_damage'):
        # Q7. Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4
        # and car avails Insurance
        print("Number of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance:", vehicle_accident_analysis_obj.crash_ids_with_no_damage(config["output_file_path"][args.key]))

    elif (args.key == 'top_5_vehicle_brand'):
        # Q8. Determine the Top 5 Vehicle Makes/Brands where drivers are charged with speeding related offences, has licensed
        # Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of
        # offences (to be deduced from the data)
        print("The Top 5 Vehicle Makes/Brands where drivers are charged with speeding related offences, has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data):", vehicle_accident_analysis_obj.top_5_vehicle_brand(config["output_file_path"][args.key]))

    
def vehicle_accident_analysis_results(args):
    """
    Description : Get results 
    :param args: Runtime Argument
    """

    try:
        spark = SparkSession.builder \
                    .appName('vehicle_accident_analysis_results') \
                    .getOrCreate()
        config = open(args.config)
        config_file = json.load(config)
        print("Config File : {}".format(config_file))
        get_results(spark,config_file,args)
    except Exception as err:
        logging.error("Error :", str(err))
    finally:
        spark.stop()                                                 # stop the spark session at the end to the execution
        print("Successfully stopped spark session ")

def main():
    arguments = get_argument_parse()
    print("Arguments : {}".format(arguments))
    vehicle_accident_analysis_results(arguments)


if __name__ == '__main__':
    main()