import streamlit as st
import pandas as pd
import pymysql

#Database connection
def create_connection():
    try:
        myconnection = pymysql.connect(
            host='localhost', 
            user='root',
            password='Gaja!Kani2022',
            database='ds_secure_check',
            cursorclass=pymysql.cursors.DictCursor    
        )
        return myconnection
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return None
    
#Fetch data from database
def fetch_data(query):
    myconnection = create_connection()
    if myconnection:
        try:
            with myconnection.cursor() as cur:
                cur.execute(query)
                result = cur.fetchall()
                df = pd.DataFrame(result)
                return df
        finally:
            myconnection.close()
    else:
        return pd.DataFrame        

print("Streamlit")

st.set_page_config(page_title="Streamlit Police Dashboard",layout="wide")
st.title("Secure check: A DIGITAL LEDGER FOR POLICE POST LOGS")

st.header("Vechicle Log Overview")
query = "SELECT * FROM check_post_log"
data = fetch_data(query)
st.dataframe(data, use_container_width= True)

st.header("Violation & Officers Report")
column1,column2,column3,column4 = st.columns(4)

with column1:
    total_stops = data.shape[0]
    st.metric("Total_police_stops", total_stops)
with column2:
    Arrests = data[data['stop_outcome'].str.contains("Arrest",case=False, na=False)].shape[0]
    st.metric("Total_Arrests",Arrests)
with column3:
    Warnings=data[data['stop_outcome'].str.contains("Warning",case=False, na=False)].shape[0]
    st.metric("Total_Warnings",Warnings)
with column4:
    drug_related = data[data["drugs_related_stop"]==1].shape[0]
    st.metric("Drugs_related_stops",drug_related)

st.header("Queries")

select_query = st.selectbox("Vehicle based a Query to run",[
    "Top 10 vehicles involved in drug_related stops",
    "Most frequently searched vehicles",
    "Highest arrest rate of driver age group",
    "Gender distribution of drivers stopped in each country",
    "Which race and gender combination has the highest search rate",
    "Time of day sees the most traffic stops",
    "Average stop duration for differend violations",
    "Are stops during the night more likely to lead to arrests",
    "Which violation are most associated with searches or arrests",
    "Violation are most common among younger drivers(<25)",
    "Violation that rarely results in search or arrest",
    "Country reports the highest rate of drug-related stops",
    "Arrest rate by country and violation",
    "Country has the most stops with search conducted",
    "Yearly breakdown of stops and arrest by country",
    "Driver violation trends based on driver race and age",
    "Time period analysis of stops, number of stops with year,month and hours of the day",
    "Violation with high search and arrest rate",
    "Driver demographics by country(Age,Gender,Race)",
    "Top 5 Violations with highest arrest rate"
])

Map_Query = {
    "Top 10 vehicles involved in drug_related stops":"SELECT vehicle_number,COUNT(*)AS drug_related_count FROM check_post_log WHERE drugs_related_stop='True' GROUP BY vehicle_number limit 10",
    "Most frequently searched vehicles":"SELECT vehicle_number,COUNT(*)AS search_count FROM check_post_log WHERE search_conducted='Yes' GROUP BY vehicle_number LIMIT 5",
    "Highest arrest rate of driver age group": "SELECT CASE WHEN driver_age<18 THEN 'under 18' WHEN driver_age BETWEEN 18 AND 35 THEN '18-35' WHEN driver_age BETWEEN 36 AND 50 THEN '36-50' ELSE 'above 50' END AS age_group, COUNT(*) AS total_stops, SUM(CASE WHEN stop_outcome='Arrested' THEN 1 ELSE 0 END) AS arrests, ROUND(SUM(CASE WHEN stop_outcome='Arrested' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS arrest_rate FROM check_post_log GROUP BY age_group ORDER BY arrest_rate DESC",
    "Which race and gender combination has the highest search rate": "SELECT driver_race, driver_gender, AVG(CASE WHEN search_conducted = 'True' THEN 1 ELSE 0 END) AS search_rate FROM check_post_log GROUP BY driver_race, driver_gender ORDER BY search_rate DESC LIMIT 4",
    "Gender distribution of drivers stopped in each country":"SELECT country_name,driver_gender, COUNT(*) AS total_stops from check_post_log GROUP BY country_name,driver_gender ORDER BY country_name,driver_gender DESC",
    "Which race and gender combination has the highest search rate":"SELECT driver_race,driver_gender,AVG(case when search_conducted='Yes' then 1 else 0 end) AS search_rate FROM check_post_log GROUP BY driver_race,driver_gender ORDER BY search_rate DESC LIMIT 4",
    "Time of day sees the most traffic stops":"SELECT HOUR(stop_time),COUNT(*) AS total_stops FROM check_post_log GROUP BY HOUR(stop_time) ORDER BY total_stops DESC ",
    "Average stop duration for differend violations":"SELECT violation, AVG(stop_duration) AS avg_stop_duration FROM check_post_log GROUP BY violation ORDER BY avg_stop_duration",
    "Which violation are most associated with searches or arrests":"SELECT violation,AVG(CASE WHEN search_conducted='Yes' THEN 1 ELSE 0 END) AS search_rate,AVG( CASE WHEN is_arrested='Yes' THEN 1 ELSE 0 END) AS arrest_rate FROM check_post_log GROUP BY violation ORDER BY (AVG(CASE WHEN search_conducted='Yes' THEN 1 ELSE 0 END)+AVG(CASE WHEN is_arrested='Yes' THEN 1 ELSE 0 END))DESC",
    "Violation are most common among younger drivers(<25)":"SELECT violation,COUNT(*)AS total_violations FROM check_post_log WHERE driver_age<25 GROUP BY violation ORDER BY total_violations DESC",
    "Violation that rarely results in search or arrest":"SELECT violation, AVG(CASE WHEN search_conducted='True'THEN 1 ELSE 0 END)AS search_rate,AVG(CASE WHEN is_arrested='True'THEN 1 ELSE 0 END)AS arrest_rate FROM check_post_log GROUP BY violation ORDER BY (AVG(CASE WHEN search_conducted = 'True' THEN 1 ELSE 0 END) + AVG(CASE WHEN is_arrested ='True' THEN 1 ELSE 0 END)) ASC",
    "Country reports the highest rate of drug-related stops":"SELECT country_name,COUNT(*)AS total_drug_stops FROM check_post_log WHERE violation='DUI'GROUP BY country_name ORDER BY total_drug_stops DESC",
    "Arrest rate by country and violation":"SELECT country_name,violation,AVG(CASE WHEN stop_outcome = 'Arrest' THEN 1 ELSE 0 END) AS arrest_rate FROM check_post_log GROUP BY country_name, violation ORDER BY arrest_rate DESC",
    "Country has the most stops with search conducted":"SELECT country_name,COUNT(*) AS total_searches FROM check_post_log WHERE search_conducted ='True' GROUP BY country_name ORDER BY total_searches DESC",
   "Yearly breakdown of stops and arrest by country":"SELECT country_name,stop_year,total_stops,total_arrests,ROUND(total_arrests * 100.0 / total_stops, 2) AS arrest_rate_percent,SUM(total_arrests) OVER (PARTITION BY country_name ORDER BY stop_year) AS arrests,SUM(total_stops) OVER (PARTITION BY country_name ORDER BY stop_year) AS stops FROM (SELECT country_name,YEAR(stop_date) AS stop_year,COUNT(*) AS total_stops,SUM(CASE WHEN is_arrested = 'True' THEN 1 ELSE 0 END) AS total_arrests FROM check_post_log GROUP BY country_name, YEAR(stop_date)) AS yearly_data ORDER BY country_name, stop_year",
   "Driver violation trends based on driver race and age":"SELECT v.driver_race,v.age_group,v.violation,COUNT(*) AS total_violationFROM(SELECTdriver_race,violation,CASE WHEN driver_age <25 THEN 'Under 25' WHEN driver_age BETWEEN 25 AND 40 THEN '25-40' WHEN driver_age BETWEEN 41 AND 60 THEN '41-60' ELSE 'Above 60' END AS age_groupFROM check_post_log)AS v GROUP BY v.driver_race,v.age_group,v.violation ORDER BY v.driver_race,v.age_group,total_violation DESC",
    "Time period analysis of stops, number of stops with year,month and hours of the day":"SELECT YEAR(stop_date) AS Year,MONTH(stop_date) AS Month,HOUR(stop_time) as Time,COUNT(*) AS total_stopsFROM check_post_logGROUP BY YEAR(stop_date),MONTH(stop_date),HOUR(stop_time)ORDER BY Year,Month,Time",
    "Violation with high search and arrest rate":"SELECT violation,total_violations,total_searches,total_arrests,ROUND(total_searches * 100/total_violations,2)as search_rate_percent,ROUND(total_arrests * 100/total_violations,2)as arrest_rate_percent,RANK()OVER(ORDER BY total_searches * 1/total_violations DESC) AS Search_rank,RANK()OVER(ORDER BY total_arrests * 1/total_violations DESC) AS Arrest_rank FROM(SELECTviolation,count(*) AS total_violations,SUM(CASE WHEN search_conducted ='True' THEN 1 ELSE 0 END)AS total_searches,SUM(CASE WHEN is_arrested ='True' THEN 1 ELSE 0 END)AS total_arrestsFROM check_post_logGROUP BY violation ) AS Violation ORDER BY arrest_rate_percent DESC",
    "Driver demographics by country(Age,Gender,Race)":"SELECT country_name,ROUND(AVG(driver_age),1) AS avg_age,COUNT(CASE WHEN driver_gender='M' THEN 1 ELSE 0 END) AS Male_driver,COUNT(CASE WHEN driver_gender='F' THEN 1 ELSE 0 END) AS Female_driver,COUNT(CASE WHEN driver_race='Asian' THEN 1 ELSE 0 END) AS Asian_driver,COUNT(CASE WHEN driver_race='Black' THEN 1 ELSE 0 END) AS Black_driver,COUNT(CASE WHEN driver_race='Hispanic' THEN 1 ELSE 0 END) AS Hispanic_driver,COUNT(CASE WHEN driver_race='Other' THEN 1 ELSE 0 END) AS Other_driver,COUNT(CASE WHEN driver_race='White' THEN 1 ELSE 0 END) AS White_driver,COUNT(*) AS Total_drivers FROM check_post_log GROUP BY country_name",
    "Top 5 Violations with highest arrest rate":"SELECT violation,Count(*) AS Total_violation,SUM(CASE WHEN is_arrested ='True' THEN 1 ELSE 0 END)AS total_arrests,ROUND(SUM(CASE WHEN is_arrested = 'True' THEN 1 ELSE 0 END)*100/COUNT(*),2)AS arrest_percent FROM check_post_log GROUP BY violation ORDER BY arrest_percent DESC LIMIT 5",
}

if st.button("Run Query"):
    result = fetch_data(Map_Query[select_query])
    if not result.empty:
        st.write(result)
    else:
        st.warning("No result found")

st.markdown("Fill the details to predict the stop outcome")
st.header("Predict outcome &Violation")

with st.form("Police_log_Form"):
    stop_date = st.date_input("stop_date")
    stop_time = st.time_input("stop_time")
    country_name = st.selectbox("country_name",["Canada","India","USA"])
    driver_gender = st.selectbox("driver_gender",["Male","Female"])
    driver_age = st.number_input("driver_age",min_value=16,max_value=100,value=27)
    driver_race = st.text_input("driver_race")
    search_conducted=st.selectbox("Was a search conducted?",["0","1"])
    search_type=st.text_input("search_type")
    drug_related_stop = st.selectbox("Was it Drug Related?",["0","1"])
    stop_duration = st.selectbox("stop_duration",data['stop_duration'].dropna().unique())
    vehicle_number = st.text_input("vehicle_number")

    submitted = st.form_submit_button("Predict outcome & Violation")

    if submitted:
        filtered_data =data[
        (data['driver_gender']==driver_gender)&
        (data['driver_age']==driver_age)&
        (data['search_conducted']==int(search_conducted))&
        (data['stop_duration']==stop_duration)&
        (data['drugs_related_stop']==int(drugs_related_stop))
        ]

    search_text ="A search was conducted" if int(search_conducted) else "No search was conducted"
    drug_text="Was drug related" if int(drug_related_stop) else "was not drug related"

    st.markdown(f"""
    **Prediction summary**
   
    A {driver_age} year old {driver_gender} driver in {country_name} was stopped at {stop_time.strftime('%I:%M %p')} on {stop_date}.{search_text},and the stop {drug_text}.
    """)        


        
                                
    

                                               






