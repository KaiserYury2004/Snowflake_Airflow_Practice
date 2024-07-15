-- Имя базы данных - AIRDATA
CREATE SCHEMA my_dwh.raw;
USE SCHEMA AIRDATA.RAW;
CREATE OR REPLACE TABLE RAW.RAW_DATA(
    Position INTEGER,
    Passenger_Id STRING,
    First_Name STRING,
    Last_Name STRING,
    Gender STRING,
    Age INTEGER,
    Nationality STRING,
    Airport_Name STRING,
    Airport_Country_Code STRING,
    Country_Name STRING,
    Airport_Continent STRING,
    Continents STRING,
    Departure_time STRING,
    Arrival_Airport STRING,
    Pilot_Name STRING,
    Flight_Status STRING,
    Ticket_Type STRING,
    Passenger_Status STRING
);

CREATE OR REPLACE TABLE RAW.CLEAN_DATA(
    Position INTEGER NOT NULL,
    Passenger_Id STRING NOT NULL,
    First_Name STRING NOT NULL,
    Last_Name STRING NOT NULL,
    Gender STRING NOT NULL,
    Age INTEGER NOT NULL,
    Nationality STRING,
    Airport_Name STRING NOT NULL,
    Airport_Country_Code STRING NOT NULL,
    Country_Name STRING,
    Airport_Continent STRING,
    Continents STRING,
    Departure_time STRING NOT NULL,
    Arrival_Airport STRING NOT NULL,
    Pilot_Name STRING NOT NULL,
    Flight_Status STRING NOT NULL,
    Ticket_Type STRING NOT NULL,
    Passenger_Status STRING NOT NULL,
    UNIQUE (Passenger_Id, Departure_time)
);

CREATE OR REPLACE TABLE datamart_data (
    Passenger_Id STRING NOT NULL,
    First_Name STRING NOT NULL,
    Last_Name STRING NOT NULL,
    Gender STRING NOT NULL,
    Age INTEGER NOT NULL,
    Nationality STRING,
    Airport_Name STRING NOT NULL,
    Airport_Country_Code STRING NOT NULL,
    Country_Name STRING,
    Airport_Continent STRING,
    Continents STRING,
    Departure_time DATE NOT NULL,
    Arrival_Airport STRING NOT NULL,
    Pilot_Name STRING NOT NULL,
    Flight_Status STRING NOT NULL,
    Ticket_Type STRING NOT NULL,
    Passenger_Status STRING NOT NULL,
    UNIQUE (Passenger_Id, Departure_time)
);
