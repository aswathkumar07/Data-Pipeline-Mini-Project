import mysql.connector
import pandas as pd
import db_connection_details
import os

from mysql.connector import connection

def get_db_connection():
    """
        Gets the MySQL server connection
    """
    connection = None
    try:
        connection = mysql.connector.connect(user = db_connection_details.USER,
                                            password = db_connection_details.PASSWORD,
                                            host = db_connection_details.HOST,
                                            port = db_connection_details.PORT)

    except Exception as error:
        print("Error while connecting to database for job tracker", error)

    return connection

def create_new_database_and_table(db_name, table_name, connection):
    """
        Creates a new database and table if it does not exist already.
    """
    try:
        db_cursor = connection.cursor()
        query1 = f"CREATE DATABASE IF NOT EXISTS {db_name};"
        query2 = f"""CREATE TABLE IF NOT EXISTS {db_name}.{table_name}(
                          ticket_id INT,
                          trans_date DATE,
                          event_id INT,
                          event_name VARCHAR(50),
                          event_date DATE,
                          event_type VARCHAR(10),
                          event_city VARCHAR(20),
                          customer_id INT,
                          price DECIMAL,
                          num_tickets INT);
            """
        db_cursor.execute(query1)
        db_cursor.execute(query2)
        connection.commit()
        db_cursor.close()
    
    except Exception as e:
        db_cursor.close()

def load_third_party(db_name, table_name, connection, csv_file_path):
    try:
        db_cursor = connection.cursor()
        csv_data = pd.read_csv(csv_file_path, header=None)
        #print(csv_data)
        
        for row_no in range(0, len(csv_data)):
            row_to_upload = (
                csv_data.iloc[row_no][0],
                csv_data.iloc[row_no][1],
                csv_data.iloc[row_no][2],
                csv_data.iloc[row_no][3],
                csv_data.iloc[row_no][4],
                csv_data.iloc[row_no][5],
                csv_data.iloc[row_no][6],
                csv_data.iloc[row_no][7],
                csv_data.iloc[row_no][8],
                csv_data.iloc[row_no][9]
            )
            query = f"INSERT INTO {db_name}.{table_name} VALUES {str(row_to_upload)};"
            db_cursor.execute(query)
            connection.commit()
            
        db_cursor.close()

    except Exception as e:
        db_cursor.close()

def query_popular_tickets(db_name, table_name, connection):
    # Getting the most popular ticket in the last month
    query = f"""
        SELECT event_name, SUM(num_tickets) AS no_of_tickets_sold
        FROM {db_name}.{table_name}
        GROUP BY event_name
        ORDER BY no_of_tickets_sold DESC
        LIMIT 3
    """
    db_cursor = connection.cursor()
    db_cursor.execute(query)
    records = db_cursor.fetchall()
    db_cursor.close()
    return records

if __name__ == "__main__":
    connection = get_db_connection()

    create_new_database_and_table("Tickets_DB", "sales", connection)

    current_working_directory = os.getcwd()

    csv_file_path = current_working_directory + "\\third_party_sales_1.csv"

    load_third_party("tickets_db", "sales", connection, csv_file_path)

    output_records = query_popular_tickets("tickets_db", "sales", connection)

    lst_print_result = [x[0] for x in output_records]

    print(f"""Here are the most popular shows in the last month:
                -  {lst_print_result[0]}
                -  {lst_print_result[1]}
                -  {lst_print_result[2]}  
    """)



