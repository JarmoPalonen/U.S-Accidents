
# kokoaa koko aineiston columnit dataframeiksi, lukuunottamatta Severity ja Description columneja
# yhdistävä tekijä DataFrameille on ID

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    to_timestamp, date_format, col, when, round
)

print("all_dataframes.py loaded successfully")


# DataFrame koko datasetistä

def load_base_df(path):
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv(path, header=True, inferSchema=True)
    print("Base DataFrame loaded.")
    #print("Rows:", df.count())
    #print("Columns:", len(df.columns))
    return df


# säätilaa kuvaavat columit - weather_df
# ---------------------------------------------------------
def create_weather_df(df):
    weather_cols = [
        "ID", "Weather_Timestamp", "Temperature(F)", "Wind_Chill(F)", "Humidity(%)",
        "Pressure(in)", "Visibility(mi)", "Wind_Direction", "Wind_Speed(mph)",
        "Precipitation(in)", "Weather_Condition", "Airport_Code"
    ]

    weather_df = df.select(weather_cols)

    # Tarkistus
    print("\nweather_df created.")
    #print("Rows:", weather_df.count())
    #print("Columns:", len(weather_df.columns))
    #weather_df.printSchema()

    return weather_df


# Ympäristöön liittyvät columnit - environment_df

def create_environment_df(df):
    environment_cols = [
        "ID", "Amenity", "Give_Way", 
        "Bump", "Crossing", "Junction", "No_Exit", "Railway", "Roundabout",
        "Station", "Stop", "Traffic_Calming", "Traffic_Signal", "Turning_Loop"
    ]

    environment_df = df.select(environment_cols)

    # Tarkistus
    print("\nenvironment_df created.")
    #print("Rows:", environment_df.count())
    #print("Columns:", len(environment_df.columns))
    #environment_df.printSchema()

    return environment_df


# Aikaa kuvaavat columnit - time_df

def create_time_df(df):
    # perus timestampit
    time_df = df.select(
        "ID",
        to_timestamp("Start_Time").alias("Start_Time"),
        to_timestamp("End_Time").alias("End_Time"),
        to_timestamp("Weather_Timestamp").alias("Weather_Timestamp")
    )

    # muokatut sarakkeet
    time_df = (
        time_df
        .withColumn("time_hhmm", date_format("Start_Time", "HH:mm"))
        .withColumn("day_of_week", date_format("Start_Time", "E"))
        .withColumn("month", date_format("Start_Time", "MMM"))
        .withColumn(
            "is_weekend",
            when(col("day_of_week").isin("Sat", "Sun"), "Y").otherwise("N")
        )
        .withColumn(
            "weather_delay_minutes",
            round((col("Start_Time").cast("long") - col("Weather_Timestamp").cast("long")) / 60, 1)
        )
    )
 
    print("\ntime_df created.")
    #print("Rows:", time_df.count())
    #print("Columns:", len(time_df.columns))
    #time_df.printSchema()

    return time_df

# maantieteelliset columnit - geo_df
def create_geo_df(df):
    geo_cols = [
        "ID",
        "Country",
        "State",
        "County",
        "City",
        "Street",
        "Zipcode",
        "Start_Lat",
        "Start_Lng",
        "Distance(mi)",
        "Timezone"
    ]
    geo_df = df.select(geo_cols)

    # Tarkistus
    print("\ngeo_df created.")
    #print("Rows:", geo_df.count())
    #print("Columns:", len(geo_df.columns))
    #geo_df.printSchema()

    return geo_df

# valo-olosuhteita kuvaavat columnit - light_df

def create_light_df(df):
    light_cols = [
        "ID",
        "Sunrise_Sunset",
        "Civil_Twilight"
    ]

    light_df = df.select(light_cols)

    print("\nlight_df created.")
    #print("Rows:", light_df.count())
    #print("Columns:", len(light_df.columns))
    #light_df.printSchema()

    return light_df

