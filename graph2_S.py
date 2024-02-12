import pandas as pd
from pyspark.sql.functions import *
from builtins import round as builtinround
from builtins import sum as builtin_sum
from datetime import datetime
import json

# Define mappings of region and wilayat IDs to their Arabic names.

region_ID = {"1":"محافظة مسقط","2":"محافظة شمال الباطنة","3":"محافظة مسندم","4":"محافظة البريمي","5":"محافظة الظاهرة","6":"محافظة الد​اخلية"
            ,"7":"محافظة شمال الشرقية","8":"محافظة الوسطى","9":"محافظة ظفار","10":"محافظة جنوب الباطنة","11":"محافظة جنوب الشرقية"}

Wilayat_ID = {"1":"مسـقط","2":"السـيب","3":"مـطرح","4":"بوشـر","5":"العامـرات","6":"قريات","7":"صحار","8":"الرستاق","9":"شناص","10":"لوى",
    "11":"صحم","12":"الخابورة","13":"السويق","14":"نخل","15":"وادي المعاول","16":"العوابي","17":"المصنعة",
    "18":"بركاء","19":"خصب","20":"بخاء","21":"دبا","22":"مدحاء","23":"البريمي","24":"عبري","25":"محضة","26":"ينقل",
    "27":"ضنك","28":"نزوى","29":"سمائل","30":"بهلاء","31":"أدم","32":"الحمراء","33":"منح","34":"إزكي","35":"بدبد","36":"صور",
    "37":"إبراء","38":"بدية","39":"القابل","40":"المضيبي","41":"دماء والطائيين","42":"الكامل والوافي",
    "43":"جعلان بني بو علي","44":"جعلان بني بو حسن","45":"وادي بني خالد","46":"مصيرة","47":"هيماء","48":"محوت",
    "49":"الدقم","50":"الجازر","51":"صلالة","52":"ثـمـريت","53":"طاقة","54":"مرباط","55":"سدح","56":"رخيوت",
    "57":"ضلكوت","58":"مقشن","59":"شليم وجزر الحلانيات","60":"المزيونة","61":"السنينة","62":"الجبل الاخضر","63":"سناو",
}

def get_arabic_name(level, id_value):
    """
    Fetch the Arabic name based on the given ID and level.

    Args:
        level (str): Level type ('RegionID' or other for Wilayat).
        id_value (int or str): The ID whose name is to be fetched.

    Returns:
        str: Corresponding Arabic name. If not found, returns the ID value as a string.
    """
     
    if level == 'RegionID':
        return region_ID.get(str(id_value), str(id_value))
    else:  
        return Wilayat_ID.get(str(id_value), str(id_value))
    
# Load required JSON files.
with open('allWilayat.json', 'r') as file:
    WILAYAT = json.load(file)
with open('allRegion.json', 'r') as file:
    REGION = json.load(file)
with open('oman.json', 'r') as file:
    OMAN = json.load(file)


def convert_24hr_to_12hr(time_str):
    """
    Convert a 24-hour format string to 12-hour format with AM/PM.

    Args:
        time_str (str or int): 24-hour format time string.

    Returns:
        str: 12-hour format time string.
    """

    if isinstance(time_str, int):
        time_str = str(time_str)
    time_obj = datetime.strptime(time_str, '%H')
    return time_obj.strftime('%I %p')

def overall_polling_prediction(conn):
    """
    Fetch and compute overall polling prediction data.

    Args:
        conn (Connection object): Database connection object.

    Returns:
        dict: Data structure containing information and data for the overall polling prediction.
    """

    total_voters = OMAN['Oman'][0]['CountOfVoters']
    sql_query = """SELECT TimeSlot, SUM(Male + Female) as TotalPolling FROM prediction GROUP BY TimeSlot ORDER BY TimeSlot;"""
    df = pd.read_sql(sql_query, conn)
    df['TimeSlot'] = pd.to_datetime(df['TimeSlot'])
    df['Hour'] = df['TimeSlot'].dt.hour
    df_sorted = df.sort_values('Hour')
    df_sorted['PollingPercentage'] = builtinround((100 * df_sorted['TotalPolling']) / total_voters)
    df_sorted['PollingPercentage'] = df_sorted['PollingPercentage'].fillna(0).astype(int)
    plots = {row['Hour']: row['PollingPercentage'] for _, row in df_sorted.iterrows()}
    data_structure = {"type": "forcasted","graph": "LineGraph","info": "Overall polling prediction","xandy": ["Hour", "Polling Percentage"],"plots": plots}
    return data_structure

def pollingstats(conn):
    """
    Fetch polling statistics from the database.

    Args:
        conn (Connection object): Database connection object.

    Returns:
        dict: Data structure containing information and data for the polling stats.
    """

    sql_query = """SELECT V_Time,Percentage FROM Time_Summary ORDER BY CASE WHEN CHARINDEX(' AM', V_Time) > 0 THEN CAST(LEFT(V_Time, CHARINDEX(' ', V_Time) - 1) AS INT) 
        WHEN CHARINDEX(' PM', V_Time) > 0 THEN CAST(LEFT(V_Time, CHARINDEX(' ', V_Time) - 1) AS INT) + 12 ELSE 0 END"""
    df = pd.read_sql(sql_query, conn)
    if not isinstance(df, pd.DataFrame):
        df = df.toPandas()
    df['Percentage'] = df['Percentage'].apply(builtinround)
    if df is not None:
        plots = df.set_index("V_Time")["Percentage"].to_dict()
        data_structure = {"type": "stats","graph": "LineGraph","info": "Oman polling stats","xandy": ["Hours", "Polling Percentage"],"plots": plots}
    return data_structure

def combined_polling_data(conn):
    """
    Combine the polling statistics and the overall polling prediction data.

    Args:
        conn (Connection object): Database connection object.

    Returns:
        dict: Combined data structure for both polling stats and prediction.
    """

    stats_output = pollingstats(conn)
    prediction_output = overall_polling_prediction(conn)
    stats_plots = stats_output.get("plots", {})
    prediction_plots = prediction_output.get("plots", {})
    converted_prediction_plots = {convert_24hr_to_12hr(k): v for k, v in prediction_plots.items()}
    if isinstance(stats_plots, dict) and isinstance(converted_prediction_plots, dict) and converted_prediction_plots:
        last_key = list(converted_prediction_plots.keys())[-1]
        last_value_prediction = {last_key: converted_prediction_plots[last_key]}
        merged_plots = {**stats_plots, **last_value_prediction}
        sorted_plots = dict(sorted(merged_plots.items(), key=lambda x: datetime.strptime(x[0], '%I %p')))
        prediction_output["plots"] = sorted_plots
    return prediction_output

def genderstats(conn):
    """
    Computes the percentage of male and female counts against 
    the total male and female counts, respectively.

    :param conn: Database connection object used for querying.
    :return: A dictionary with details about the gender distribution in Oman.
    """
    
    total_male = OMAN['Oman'][0]['MaleCount']
    total_female = OMAN['Oman'][0]['FemaleCount']
    sql_query = '''SELECT 'M' AS "Sex", SUM(Male) AS "LiveCount" FROM Oman_Summary UNION ALL SELECT 'F' AS "Sex", SUM(Female) AS "LiveCount" FROM Oman_Summary;'''
    df = pd.read_sql(sql_query, conn)
    live_male_count = df[df['Sex'] == 'M']['LiveCount'].values[0]
    live_female_count = df[df['Sex'] == 'F']['LiveCount'].values[0]
    male_percentage = builtinround((live_male_count * 100) / total_male)
    female_percentage = builtinround((live_female_count * 100) / total_female)
    plots = {"M": male_percentage,"F": female_percentage}
    data_structure = {"type": "stats","graph": "radial","info": "Oman gender distribution (realtime/overall)","xandy": ["", ""],"plots": plots}
    return data_structure

def agewisePolling_stats(conn):
    """
    Fetches the top 10 age groups from the database ordered by 
    the polling percentage.

    :param conn: Database connection object used for querying.
    :return: A dictionary with details about age-wise polling statistics in Oman.
    """
        
    sql_query = """SELECT TOP 10 Age, Percentage FROM Top_10_Age ORDER BY Percentage DESC;"""
    df = pd.read_sql(sql_query, conn)
    
    if not isinstance(df, pd.DataFrame):
        df = df.toPandas()
    
    if df is not None:
        plots = {f"Age_{age}": percentage for age, percentage in zip(df["Age"], df["Percentage"])}
        data_structure = {"type": "stats","graph": "BarGraph","info": "Oman Age wise polling stats","xandy": ["Age", "Polling Percentage"],"plots": plots}
        return data_structure
 
def genderwise_Prediction(conn):
    """
    Predicts the gender distribution of polling based on AI's previous data.

    :param conn: Database connection object used for querying.
    :return: A dictionary with details about the predicted gender distribution of polling in Oman.
    """
    
    total_male = OMAN['Oman'][0]['MaleCount']
    total_female = OMAN['Oman'][0]['FemaleCount']
    sql_query = """SELECT SUM(Male) AS Male, SUM(Female) AS Female FROM prediction;"""
    df = pd.read_sql(sql_query, conn)
    if not isinstance(df, pd.DataFrame):
        df = df.toPandas()
    if df is not None:
        df['Male'] = (df['Male'] * 100)/ total_male
        df['Female'] = (df['Female'] * 100)/ total_female
        df = df.melt(var_name="Sex", value_name="Percentage")
        df["Percentage"] = df["Percentage"].apply(builtinround).astype(int)
        plots = df.set_index("Sex")["Percentage"].to_dict()
        data_structure = {"type": "forcasted","graph": "BarGraph","info": "Oman Gender wise prediction by AI","xandy": ["Gender", "Polling Percentage"],"plots": plots}
        return data_structure

def agewise_Prediction(conn):
    """
    Predict age distribution for the population of Oman.

    Parameters:
    - conn: Database connection object to execute SQL queries.

    Returns:
    - data_structure: A dictionary containing data for plotting.
    """
    
    # Define age groups and their respective counts using the global OMAN dictionary
    total_age_counts = {
        '20-30': builtin_sum([OMAN['Oman'][0][f'Age{i}'] for i in range(21, 31)]),'30-40': builtin_sum([OMAN['Oman'][0][f'Age{i}'] for i in range(31, 41)]),'40-50': builtin_sum([OMAN['Oman'][0][f'Age{i}'] for i in range(41, 51)]),
        '50-60': builtin_sum([OMAN['Oman'][0][f'Age{i}'] for i in range(51, 61)]),'60-70': builtin_sum([OMAN['Oman'][0][f'Age{i}'] for i in range(61, 71)]),'70+': OMAN['Oman'][0]['AgeCountAbove70']
    }

    # Pre-define columns and age groups for SQL queries
    age_columns = ['col_1', 'col_2', 'col_3', 'col_4', 'col_5', 'col_6']
    age_groups = list(total_age_counts.keys())
    sql_queries = []

    # Generate SQL queries for each age group
    for age_column, age_group in zip(age_columns, age_groups):
        query = f"""SELECT '{age_group}' AS AgeGroup, SUM({age_column}) AS Count FROM prediction """
        sql_queries.append(query)
    
    # Combine SQL queries using UNION ALL
    sql_query = '\nUNION ALL\n'.join(sql_queries)

    # Execute SQL query and store result in a DataFrame
    df = pd.read_sql(sql_query, conn)

    # Calculate the percentage distribution for each age group
    df['Percentage'] = df.apply(lambda row: builtinround((row['Count'] * 100 )/ total_age_counts[row['AgeGroup']]) if total_age_counts[row['AgeGroup']] else 0, axis=1)
    
    # Convert to Pandas DataFrame if necessary (when using Spark for instance)
    if hasattr(df, 'toPandas'):
        df = df.toPandas()

    # Process data for plotting
    if not df.empty:
        plots = df.set_index("AgeGroup")["Percentage"].to_dict()
        data_structure = {"type": "forcasted", "graph": "BarGraph", "info": "Oman Age wise prediction by AI", "xandy": ["Age", "Polling Percentage"], "plots": plots}
    return data_structure

def regionwise_gender_stats(conn):
    """
    Calculate gender-wise polling percentage for each region in Oman.
    
    Parameters:
    - conn: Database connection object to execute SQL queries.
    
    Returns:
    - data_structure: A dictionary containing data for plotting.
    """
    # Create a lookup dictionary for regions
    all_region_dict = {region['RegionId']: region for region in REGION['allRegion']}
    
    # SQL query to fetch region-wise gender counts from the database
    sql_query = """SELECT RegionId, RegionName, SUM(Male) AS MaleSum, SUM(Female) AS FemaleSum FROM Region_Summary GROUP BY RegionId, RegionName;"""
    
    # Execute SQL query and store result in a DataFrame
    df = pd.read_sql(sql_query, conn)

    # Convert to Pandas DataFrame if necessary (when using Spark for instance)
    if not isinstance(df, pd.DataFrame):
        df = df.toPandas()
    if not df.empty:

        # Calculate percentage distribution for male and female in each region
        df['MalePercentage'] = df.apply(lambda row: builtinround((row['MaleSum'] * 100) / all_region_dict[row['RegionId']]['MaleCount']) if all_region_dict.get(row['RegionId']) else 0, axis=1).astype(int)
        df['FemalePercentage'] = df.apply(lambda row: builtinround((row['FemaleSum'] * 100) / all_region_dict[row['RegionId']]['FemaleCount']) if all_region_dict.get(row['RegionId']) else 0, axis=1).astype(int)
        
        # Convert data into required format for plotting
        plot_dict = df.set_index('RegionName')[['MalePercentage', 'FemalePercentage']].T.to_dict()
        plot_dict_str = {str(k): {i_k: float(i_v) for i_k, i_v in v.items()} for k, v in plot_dict.items()}
       
        # Define the data structure for plotting
        data_structure = {"type": "stats","graph": "ColumnStackedChart","info": "Oman Region wise Gender stats","suinfo":"Gender","xandy": ["Region", "percentage of polling"],"plots": plot_dict_str}
        return data_structure

def wilayatwise_gender_stats(conn):
    """
    Fetches and processes Wilayat wise gender stats for visualization.
    
    Args:
    - conn (object): Database connection object.

    Returns:
    - dict: Data structure containing information for plotting a Column Stacked Chart.
    """
    
    # Lookup data for all Wilayats
    wilayat_lookup = {str(item["wilayat"]): item for item in WILAYAT["allWilayat"]}
    sql_query = """SELECT TOP 10 WilayatName AS 'WilayatName', WILAYATID AS 'WilayatId', SUM(Male) AS 'MaleCountDB', SUM(Female) AS 'FemaleCountDB'
                   FROM Wilayat_Summary GROUP BY WilayatName, WILAYATID ORDER BY (SUM(Male) + SUM(Female)) DESC;"""
  
    # SQL Query to fetch gender statistics from Wilayat_Summary table
    df = pd.read_sql(sql_query, conn)
    if not isinstance(df, pd.DataFrame):
        df = df.toPandas()    
    if not df.empty:
        df['M'] = df.apply(lambda row: builtinround((row['MaleCountDB'] * 100) / wilayat_lookup[str(row['WilayatId'])]['MaleCount']) if wilayat_lookup.get(str(row['WilayatId'])) else 0, axis=1)
        df['F'] = df.apply(lambda row: builtinround((row['FemaleCountDB'] * 100) / wilayat_lookup[str(row['WilayatId'])]['FemaleCount']) if wilayat_lookup.get(str(row['WilayatId'])) else 0, axis=1)
        plot_dict = df.set_index('WilayatName')[['M', 'F']].T.to_dict()
        plot_dict_str = {str(k): {i_k: float(i_v) for i_k, i_v in v.items()} for k, v in plot_dict.items()}
        data_structure = {"type": "stats", "graph": "ColumnStackedChart", "info": "Oman Wilayat wise Gender stats","suinfo":"Gender","xandy": ["Wilayat", "percentage of polling"], "plots": plot_dict_str}
        return data_structure

def Ageinterval_stats(conn):
    """
    Fetches and processes age interval stats for visualization.
    
    Args:
    - conn (object): Database connection object.

    Returns:
    - dict: Data structure containing information for plotting a Bar Graph.
    """
    
    # Data mapping for age intervals from OMAN constant
    oman_data = OMAN["Oman"][0]
    sql_query = """SELECT 'Age_21_30' AS 'Age interval', SUM(Age_21_30) as 'Count' FROM Oman_Summary UNION ALL SELECT 'Age_31_40', SUM(Age_31_40) FROM Oman_Summary UNION ALL
    SELECT 'Age_41_50', SUM(Age_41_50) FROM Oman_Summary UNION ALL SELECT 'Age_51_60', SUM(Age_51_60) FROM Oman_Summary UNION ALL
    SELECT 'Age_61_70', SUM(Age_61_70) FROM Oman_Summary UNION ALL SELECT 'Age_71_P', SUM(Age_71_P) FROM Oman_Summary"""
    df = pd.read_sql(sql_query, conn)
    if not isinstance(df, pd.DataFrame):
        df = df.toPandas()
    age_intervals = {"Age_21_30": oman_data["AgeCount21to30"],"Age_31_40": oman_data["AgeCount31to40"],"Age_41_50": oman_data["AgeCount41to50"],
                     "Age_51_60": oman_data["AgeCount51to60"],"Age_61_70": oman_data["AgeCount61to70"],"Age_71_P": oman_data["AgeCountAbove70"]
    }
    df["Percentage"] = df.apply(lambda row: builtinround((row["Count"] * 100) / age_intervals[row["Age interval"]]), axis=1)
    plots = df.set_index("Age interval")["Percentage"].to_dict()
    data_structure = {"type": "stats","graph": "BarGraph","info": "Oman Age interval stats","xandy": ["Age interval", "Percentage"],"plots": plots}
    return data_structure

def Registered_year_wise_stats(conn):
    """
    Fetches and processes registered year-wise stats for visualization.
    
    Args:
    - conn (object): Database connection object.

    Returns:
    - dict: Data structure containing information for plotting a Bar Graph.
    """
    
    # Mapping between database columns and JSON keys for registered years
    years_db_columns = [("RY_2003", "RegisteredYear2003"),("RY_2007", "RegisteredYear2007"),
                        ("RY_2011", "RegisteredYear2011"),("RY_2012", "RegisteredYear2012"),
                        ("RY_2015", "RegisteredYear2015"),("RY_2016", "RegisteredYear2016"),
                        ("RY_2019", "RegisteredYear2019"),("RY_2020", "RegisteredYear2020"),
                        ("RY_2022", "RegisteredYear2022"),("RY_2023", "RegisteredYear2023")
                        ]
    union_queries = []
    for db_col, json_key in years_db_columns:
        denominator_value = OMAN["Oman"][0][json_key]
        select_query = f"""SELECT '{db_col}' as RY_YEAR, ROUND((SUM({db_col}) * 100.0) / {denominator_value}, 0) as Percentage FROM Oman_Summary"""
        union_queries.append(select_query)
    sql_query = " UNION ALL ".join(union_queries)
    df = pd.read_sql(sql_query, conn)
    if not isinstance(df, pd.DataFrame):
        df = df.toPandas()
    plots = df.set_index("RY_YEAR")["Percentage"].to_dict()
    data_structure = {"type": "stats","graph": "BarGraph","info": "Oman Registered year-wise stats","xandy": ["RY_YEAR", "Percentage"],"plots": plots}
    return data_structure

def Regionwise_stats(conn):
    """
    Fetches and processes region-wise stats for visualization.
    
    Args:
    - conn (object): Database connection object.

    Returns:
    - dict: Data structure containing information for plotting a Bar Graph.
    """
    
    # Mapping of RegionId to CountOfVoters
    region_denominator_dict = {region["RegionId"]: region["CountOfVoters"] for region in REGION["allRegion"]}
    sql_query = """SELECT RegionName AS 'RegionName', RegionId, Total AS 'polling count' FROM Region_Summary;"""
    df = pd.read_sql(sql_query, conn)
    if not isinstance(df, pd.DataFrame):
        df = df.toPandas()
    if df is not None and not df.empty:
        df["Denominator"] = df["RegionId"].apply(lambda x: region_denominator_dict.get(x, 0))
        df["percentage"] = df.apply(lambda row: builtinround((row["polling count"] * 100) / row["Denominator"]), axis=1)
        plots = df.set_index("RegionName")["percentage"].to_dict()
        data_structure = {"type": "stats","graph": "BarGraph","info": "Oman Region Wise stats","xandy": ["Region", "Percentage of polling"],"plots": plots}
        return data_structure

def regionwiseage_stats(conn):
    """
    Fetches and processes region-wise age interval stats for visualization.
    
    Args:
    - conn (object): Database connection object.

    Returns:
    - dict: Data structure containing information for plotting a Column Stacked Chart.
    """
    
    # SQL Query to fetch age interval stats by region from Region_Summary table
    sql_query = """SELECT REGIONID, RegionName, Age_21_30, Age_31_40, Age_41_50, Age_51_60, Age_61_70, Age_71_P FROM Region_Summary;"""
    df = pd.read_sql(sql_query, conn)
    if not isinstance(df, pd.DataFrame):
        df = df.toPandas()
    age_mapping = {'Age_21_30': 'AgeCount21to30','Age_31_40': 'AgeCount31to40','Age_41_50': 'AgeCount41to50','Age_51_60': 'AgeCount51to60','Age_61_70': 'AgeCount61to70','Age_71_P': 'AgeCountAbove70'}
    region_dict = {region['RegionId']: region for region in REGION['allRegion']}
    for age_col in age_mapping.keys():
        denom_col = age_mapping[age_col]
        df[denom_col] = df['REGIONID'].apply(lambda x: region_dict.get(x, {}).get(denom_col, 0))
        df[age_col] = df.apply(lambda row: builtinround((row[age_col] * 100) / (row[denom_col]) if row[denom_col] != 0 else 1), axis=1)

    df_melted = df.melt(id_vars=['REGIONID', 'RegionName'], value_vars=list(age_mapping.keys()), var_name='Age_Interval', value_name='Percentage')
    top_10_regions = df_melted.groupby('RegionName')['Percentage'].mean().nlargest(10).index.tolist()
    top_df = df_melted[df_melted['RegionName'].isin(top_10_regions)]
    top_ages = top_df.groupby('RegionName').apply(lambda group: group.nlargest(2, 'Percentage')).reset_index(drop=True)
    plot_dict = top_ages.groupby('RegionName')[['Age_Interval', 'Percentage']].apply(lambda x: dict(zip(x['Age_Interval'], x['Percentage']))).to_dict()
    data_structure = {"type": "stats","graph": "ColumnStackedChart","info": "Oman Region wise Age stats (Top 2 Age Intervals)","suinfo":"Age","xandy": ["Region", "Percentage of polling"],"plots": plot_dict}
    return data_structure

def wilayatwiseage_stats(conn):
    """
    Fetch statistics based on age intervals for each Wilayat and return data structured for a ColumnStackedChart.
    
    Args:
    - conn: Database connection object

    Returns:
    - dict: Data structure for a ColumnStackedChart.
    """
    # Define mapping from age interval names to their respective columns
    age_interval_map = {'Age_21_30': 'AgeCount21to30','Age_31_40': 'AgeCount31to40','Age_41_50': 'AgeCount41to50','Age_51_60': 'AgeCount51to60','Age_61_70': 'AgeCount61to70','Age_71_P': 'AgeCountAbove70'}
    # SQL query to fetch age interval data for all Wilayats
    sql_query = """SELECT WILAYATID, WilayatName, Age_21_30, Age_31_40, Age_41_50, Age_51_60, Age_61_70, Age_71_P FROM Wilayat_Summary;"""
    df = pd.read_sql(sql_query, conn)

    # Convert Spark DataFrame to pandas DataFrame if necessary
    if not isinstance(df, pd.DataFrame):
        df = df.toPandas()

    # Create a lookup map for all Wilayats
    wilayat_map = {str(wilayat["wilayat"]): wilayat for wilayat in WILAYAT["allWilayat"]}
    
    # Calculate the percentage of each age interval for each Wilayat
    for age_col in age_interval_map.keys():
        denom_col = age_interval_map[age_col]
        df[denom_col] = df['WILAYATID'].apply(lambda x: wilayat_map.get(str(x), {}).get(denom_col, 0))
        df[age_col] = df.apply(lambda row: builtinround((row[age_col] * 100) / row[denom_col] if row[denom_col] != 0 else 1), axis=1)

    # Melt DataFrame to have one row per age interval per Wilayat
    df_melted = df.melt(id_vars=['WILAYATID', 'WilayatName'], value_vars=list(age_interval_map.keys()), var_name='Age_Interval', value_name='Percentage')
    # Extract the top 10 Wilayats based on average percentage across all age intervals
    top_10_wilayats = df_melted.groupby('WilayatName')['Percentage'].mean().nlargest(10).index.tolist()

    # For each of the top 10 Wilayats, find the two age intervals with the highest percentages
    top_df = df_melted[df_melted['WilayatName'].isin(top_10_wilayats)]
    top_age_intervals = top_df.groupby('WilayatName').apply(lambda group: group.nlargest(2, 'Percentage')).reset_index(drop=True)
    
    # Convert top age intervals data to the desired structure for plotting
    column_stacked_chart = top_age_intervals.groupby('WilayatName')[['Age_Interval', 'Percentage']].apply(lambda x: dict(zip(x['Age_Interval'], x['Percentage']))).to_dict()
    
    # Construct and return the data structure for the chart
    data_structure = {"type": "stats","graph": "ColumnStackedChart","info": "Oman Top 10 Wilayats wise Age stats (Top 2 Age Intervals)","suinfo":"Age","xandy": ["Wilayat", "Percentage of polling"],"plots": column_stacked_chart}
    return data_structure

def regionwilayat_pollingstats(id_value, level, conn):
    """
    Fetch and return polling statistics for a specific Region or Wilayat.

    Args:
    - id_value (int/str): ID of the Region or Wilayat
    - level (str): Level of granularity ('RegionID' or 'WILAYATID')
    - conn: Database connection object

    Returns:
    - dict: Data structure for a LineGraph.
    """
    # Logic and database queries to fetch and structure data
    level_id = 'REGIONID' if level == 'RegionID' else 'WILAYATID'
    sql_query = f"SELECT Time, SUM(Total) as Total FROM Wilayat_hr_Summary WHERE {level_id} = {id_value} GROUP BY Time ORDER BY Time ASC"
    df = pd.read_sql(sql_query, conn)
    if level == 'RegionID':
        total = next(item['CountOfVoters'] for item in REGION['allRegion'] if item['RegionId'] == id_value)
    else:
        total = next(item['CountOfVoters'] for item in WILAYAT['allWilayat'] if item['wilayat'] == id_value)
    df['Time'] = df['Time'].astype(int)
    df_sorted = df.sort_values('Time')
    df_sorted['CumulativeTotal'] = df_sorted['Total'].cumsum()
    df_sorted['Percentage'] = (100 * df_sorted['CumulativeTotal'] / total).round().astype(int)
    arabic_name = get_arabic_name(level, id_value)
    data_structure = {"type": "stats","graph": "LineGraph","info": f"{arabic_name} polling stats", "xandy": ["Time", "Percentage of Total"],"plots": df_sorted.set_index("Time")["Percentage"].to_dict()}
    return data_structure

def regionwilayat_genderstats(id_value, level, conn):
    """
    Fetches and computes gender statistics based on the given id_value and level.
    
    Parameters:
    - id_value (int): ID value for the specified region or wilayat.
    - level (str): Indicates the level - either 'RegionID' or not (assumed as 'WilayatID').
    - conn: Database connection object.
    
    Returns:
    - dict: Data structure containing gender statistics.
    """
    level_id = 'REGIONID' if level == 'RegionID' else 'WILAYATID'
    table_name = 'Region_Summary' if level == 'RegionID' else 'Wilayat_Summary'
    sql_query = f"SELECT Male, Female FROM {table_name} WHERE {level_id} = {id_value}"
    df = pd.read_sql(sql_query, conn)
    db_male = df['Male'].iloc[0]
    db_female = df['Female'].iloc[0]
    if level == 'RegionID':
        total_male = next(item['MaleCount'] for item in REGION['allRegion'] if item['RegionId'] == id_value)
        total_female = next(item['FemaleCount'] for item in REGION['allRegion'] if item['RegionId'] == id_value)
    else:
        total_male = next(item['MaleCount'] for item in WILAYAT['allWilayat'] if item['wilayat'] == id_value)
        total_female = next(item['FemaleCount'] for item in WILAYAT['allWilayat'] if item['wilayat'] == id_value)
    male_percentage = (db_male * 100) / (total_male) if (total_male) != 0 else 0
    female_percentage = (db_female * 100) / (total_female) if (total_female) != 0 else 0
    male_percentage = builtinround(male_percentage)
    female_percentage = builtinround(female_percentage)
    plots = {"M": male_percentage, "F": female_percentage}
    arabic_name = get_arabic_name(level, id_value)
    data_structure = {"type": "stats","graph": "radial","info": f"{arabic_name} gender stats","xandy": ["Gender", "percentage of polling"],"plots": plots}
    return data_structure

def regionwilayatpolling_prediction(id_value, level, conn):
    """
    Predicts polling percentages based on the given id_value and level.
    
    Parameters:
    - id_value (int): ID value for the specified region or wilayat.
    - level (str): Indicates the level - either 'RegionID' or not (assumed as 'WilayatID').
    - conn: Database connection object.
    
    Returns:
    - dict: Data structure containing polling predictions.
    """
    if level == 'RegionID':
        count_of_voters = next(item['CountOfVoters'] for item in REGION['allRegion'] if item['RegionId'] == id_value)
    else:
        count_of_voters = next(item['CountOfVoters'] for item in WILAYAT['allWilayat'] if item['wilayat'] == id_value)
    sql_query = f"""SELECT {level}, TimeSlot, SUM(Male + Female) as TotalPolling FROM prediction WHERE {level} = {id_value} GROUP BY {level}, TimeSlot ORDER BY TimeSlot;"""
    df = pd.read_sql(sql_query, conn)
    df['TimeSlot'] = pd.to_datetime(df['TimeSlot'])
    df['Hour'] = df['TimeSlot'].dt.hour
    df_sorted = df.sort_values('Hour')
    df_sorted['CumulativePolling'] = df_sorted['TotalPolling'].cumsum()
    if count_of_voters == 0:
        df_sorted['PollingPercentage'] = 0
    else:
        df_sorted['PollingPercentage'] = (100 * df_sorted['CumulativePolling'] / count_of_voters).round()
    df_sorted['PollingPercentage'] = df_sorted['PollingPercentage'].fillna(0).astype(int)
    plots = {row['Hour']: row['PollingPercentage'] for _, row in df_sorted.iterrows()}
    arabic_name = get_arabic_name(level, id_value)
    data_structure = {"type": "forcasted","graph": "LineGraph","info": f"{arabic_name} polling prediction","xandy": ["Hour", "Polling Percentage"],"plots": plots}
    return data_structure

def combined_regionwilayat_polling(id_value, level, conn):
    """
    Combines polling statistics and predictions for a specified region or wilayat, 
    and adjusts time formatting for the plots.
    
    Parameters:
    - id_value (int): ID value for the specified region or wilayat.
    - level (str): Indicates the level - either 'RegionID' or not (assumed as 'WilayatID').
    - conn: Database connection object.
    
    Returns:
    - dict: Combined data structure containing polling predictions, 
            with time format adjusted for the plots.
    """
    
    # Get the polling statistics for the specified region or wilayat.
    stats_output = regionwilayat_pollingstats(id_value, level, conn)
    
    # Get the polling prediction for the specified region or wilayat.
    prediction_output = regionwilayatpolling_prediction(id_value, level, conn)
    
    # Convert the 24-hour time format to 12-hour for the polling statistics plots.
    converted_stats_plots = {convert_24hr_to_12hr(k): v for k, v in stats_output.get("plots", {}).items()}
    
    # Convert the 24-hour time format to 12-hour for the polling prediction plots.
    converted_prediction_plots = {convert_24hr_to_12hr(k): v for k, v in prediction_output.get("plots", {}).items()}
    
    # Check if both the stats and prediction plots are valid dictionaries and if the prediction plots are non-empty.
    if isinstance(converted_stats_plots, dict) and isinstance(converted_prediction_plots, dict) and converted_prediction_plots:
        
        # Extract the last time slot and its value from the prediction plots.
        last_key = list(converted_prediction_plots.keys())[-1]
        last_value_prediction = {last_key: converted_prediction_plots[last_key]}
        
        # Merge the statistics plots with the last value of the prediction plots.
        merged_plots = {**converted_stats_plots, **last_value_prediction}
        
        # Sort the plots based on the time.
        sorted_plots = dict(sorted(merged_plots.items(), key=lambda x: datetime.strptime(x[0], '%I %p')))
        
        # Update the prediction output with the sorted plots.
        prediction_output["plots"] = sorted_plots
    
    # Return the combined polling predictions.
    return prediction_output


def regionwilayatgender_prediction(id_value, level, conn):
    """
    Predicts gender-based polling percentages based on the given id_value and level.
    
    Parameters:
    - id_value (int): ID value for the specified region or wilayat.
    - level (str): Indicates the level - either 'RegionID' or not (assumed as 'WilayatID').
    - conn: Database connection object.
    
    Returns:
    - dict: Data structure containing gender polling predictions.
    """
    level_id = 'REGIONID' if level == 'RegionID' else 'WILAYATID'
    if level == 'RegionID':
        male_count = next(item['MaleCount'] for item in REGION['allRegion'] if item['RegionId'] == id_value)
        female_count = next(item['FemaleCount'] for item in REGION['allRegion'] if item['RegionId'] == id_value)
    else:
        male_count = next(item['MaleCount'] for item in WILAYAT['allWilayat'] if item['wilayat'] == id_value)
        female_count = next(item['FemaleCount'] for item in WILAYAT['allWilayat'] if item['wilayat'] == id_value)
    sql_query = f"""SELECT {level_id}, 'Male' as Sex, ROUND((SUM(Male) * 100.0 / {male_count}), 0) as Count FROM prediction WHERE {level_id} = {id_value} GROUP BY {level_id} UNION ALL 
                    SELECT {level_id}, 'Female' as Sex, ROUND((SUM(Female) * 100.0 / {female_count}), 0) as Count FROM prediction WHERE {level_id} = {id_value} GROUP BY {level_id}"""
    df = pd.read_sql(sql_query, conn)
    if isinstance(df, pd.DataFrame) == False:
        df = df.toPandas()  
    if df is not None:
        plots = df.set_index("Sex")["Count"].to_dict()
        arabic_name = get_arabic_name(level, id_value)
        data_structure = {"type": "forcasted","graph": "radial","info": f"{arabic_name} Gender prediction","xandy": ["Gender", "Percentage of polling"],"plots": plots}  
        return data_structure

def regionwilayatageInterval_stats(id_value, level, conn):
    """
    Fetches and computes age interval polling statistics based on the given id_value and level.
    
    Parameters:
    - id_value (int): ID value for the specified region or wilayat.
    - level (str): Indicates the level - either 'RegionID' or not (assumed as 'WilayatID').
    - conn: Database connection object.
    
    Returns:
    - dict: Data structure containing age interval polling statistics.
    """
    level_id = 'REGIONID' if level == 'RegionID' else 'WILAYATID'
    table_name = 'Region_Summary' if level == 'RegionID' else 'Wilayat_Summary'
    if level == 'RegionID':
        age_counts = next((item for item in REGION['allRegion'] if item['RegionId'] == id_value), {})
    else:  
        age_counts = next((item for item in WILAYAT['allWilayat'] if item['wilayat'] == id_value), {})
    age_intervals = [('Age_21_30', 'AgeCount21to30'),('Age_31_40', 'AgeCount31to40'),('Age_41_50', 'AgeCount41to50'),
                     ('Age_51_60', 'AgeCount51to60'),('Age_61_70', 'AgeCount61to70'),('Age_71_P', 'AgeCountAbove70')]
    sql_query = f"""SELECT {', '.join([interval[0] for interval in age_intervals])} FROM {table_name} WHERE {level_id} = {id_value}"""
    df = pd.read_sql(sql_query, conn)
    if df.empty:
        return None
    for db_interval, dict_interval in age_intervals:
        total = age_counts.get(dict_interval, 1) 
        percentage_column = f"{db_interval}_Percentage"
        df[percentage_column] = (df[db_interval] * 100.0 / total).round()
    df_melted = df.filter(like='_Percentage').melt(var_name="AgeInterval", value_name="Percentage")
    df_melted['AgeInterval'] = df_melted['AgeInterval'].str.replace('_Percentage', '')
    plots = df_melted.set_index("AgeInterval")["Percentage"].to_dict()
    arabic_name = get_arabic_name(level, id_value)
    data_structure = {"type": "stats","graph": "BarGraph","info": f"{arabic_name} Age interval polling stats","xandy": ["Age interval", "Percentage of polling"],"plots": plots}
    return data_structure