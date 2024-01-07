import mysql.connector
import pandas as pd

host = "localhost"
user = "root"
passwords = "PrasHantHCHinnapappal19802003"
connection = mysql.connector.connect(
           host=host,
           user=user,
           password=passwords
            )

cursor = connection.cursor()
database = "create database if not exists Airbnb_Analysis"
cursor.execute(database)
cursor.execute("use Airbnb_Analysis")



A = pd.read_csv("C:/Users/Senthil/Desktop/DS/Code/Airbnb/Data/F_Airbnb_Analysis.csv")




create_table = """
CREATE TABLE if not exists Analysis (
    _id INT,
    name VARCHAR(255),
    property_type VARCHAR(255),
    room_type VARCHAR(255),
    bed_type VARCHAR(255),
    minimum_nights INT,
    maximum_nights INT,
    cancellation_policy VARCHAR(255),
    accommodates INT,
    bedrooms INT,
    beds INT,
    number_of_reviews INT,
    bathrooms INT,
    amenities VARCHAR(2225),
    price DECIMAL(10, 2),
    security_deposit DECIMAL(10, 2),
    extra_people DECIMAL(10, 2),
    guests_included INT,
    address_street VARCHAR(255),
    country VARCHAR(255),
    host_neighbourhood VARCHAR(255),
    host_name VARCHAR(255),
    host_response_rate VARCHAR(255),
    review_scores_accuracy INT,
    review_scores_cleanliness INT,
    review_scores_checkin INT,
    review_scores_communication INT,
    review_scores_location INT,
    review_scores_value INT,
    review_scores_rating INT,
    availability_30 INT,
    availability_60 INT,
    availability_90 INT,
    availability_365 INT,
    weekly_price DECIMAL(10, 2),
    monthly_price DECIMAL(10, 2),
    amenities_count INT,  
    first_review_date DATE,
    last_review_date DATE
);
"""

# Executing the create table querybbd
cursor.execute(create_table)

# SQL command to insert data into the table
insert_data = """
INSERT INTO Analysis (
    _id, name, property_type, room_type,bed_type,minimum_nights, maximum_nights,
    cancellation_policy, accommodates, bedrooms, beds,
    number_of_reviews, bathrooms, amenities,price, security_deposit,
    extra_people, guests_included, address_street, country,
    host_neighbourhood, host_name, host_response_rate, review_scores_accuracy,
    review_scores_cleanliness, review_scores_checkin, review_scores_communication,
    review_scores_location, review_scores_value, review_scores_rating,
    availability_30, availability_60, availability_90, availability_365, weekly_price,monthly_price,amenities_count,first_review_date ,last_review_date 
) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s);
"""

# Assuming 'A' is your DataFrame
data_to_insert = A.to_records(index = False).tolist()


# Executing the insert data query
cursor.executemany(insert_data, data_to_insert)

# Commit the changes
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()

