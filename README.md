## Case Study:
### Dataset:
Data Set folder has 6 csv files. Please use the data dictionary (attached in the mail) to understand the dataset and then develop your approach to perform below Question.

1.Charges_use.csv  
2.Damages_use.csv  
3.Endorse_use.csv  
4.Primary_Person_use.csv  
5.Restrict_use.csv  
6.Units_use.csv


### Question: 
Application should perform below analysis and store the results for each analysis.
* Question 1: Find the number of crashes (accidents) in which number of persons killed are male?
* Question 2: How many two-wheelers are booked for crashes? 
* Question 3: Which state has the highest number of accidents in which females are involved? 
* Question 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
* Question 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
* Question 6: Among the crashed cars, what are the Top 5 Zip Codes with the highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
* Question 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
* Question 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)

### Expected Output:
1. Develop an application which is modular & follows software engineering best practices (e.g. Classes, docstrings, functions, config driven, command line executable through spark-submit)
2. Code should be properly organized in folders as a project.
3. Input data sources and output should be config driven
4. Code should be strictly developed using Dataframe APIs (Do not use Spark SQL)
5. Share the entire project as zip or link to project in GitHub repo.

#### Project Structure

```
.
├── Data
│   └── Charges_use.csv  
|   └── Damages_use.csv  
|   └── Endorse_use.csv  
|   └── Primary_Person_use.csv  
|   └── Restrict_use.csv  
|   └── Units_use.csv
├── output
│   └── count_male_crash.csv  
|   └── count_2_wheeler_crash.csv  
|   └── state_with_highest_female_accident.csv  
|   └── top_vehicle_contributing_to_injuries.csv  
|   └── count_2_wheeler_crash.csv  
|   └── top_5_zip_codes_with_alcohols_as_reason_for_crash.csv
│   └── count_2_wheeler_crash.csv
|   └── top_5_vehicle_brand.csv
└── config
|       └── config.json
└── Veh_Accident_analysis.py
└── utility.py
├── main.py
├── README.md

```

#### Steps to run the analysis:

#### Input and Output file path

In this use case, Json file is used to configure the input and output files directory.

Update the input and output file path in the config file

### Execution Steps [Local]:

Question 1: Find the number of crashes (accidents) in which number of persons killed are male?

        spark-submit --master local[*] main.py -k count_male_crash
        
Question 2: How many two-wheelers are booked for crashes?
    
        spark-submit --master local[*] main.py -k count_2_wheeler_crash

Question 3: Which state has the highest number of accidents in which females are involved?
        
        spark-submit --master local[*] main.py -k state_with_highest_female_accident   

Question 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death

        spark-submit --master local[*] main.py -k top_vehicle_contributing_to_injuries

Question 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style

        spark-submit --master local[*] main.py -k count_2_wheeler_crash

Question 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)

        spark-submit --master local[*] main.py -k top_5_zip_codes_with_alcohols_as_reason_for_crash   

Question 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
    
        spark-submit --master local[*] main.py -k count_2_wheeler_crash

Question 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
    
        spark-submit --master local[*] main.py -k top_5_vehicle_brand

