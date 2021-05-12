# coding=utf-8
# author: Bruno Lançoni Neto
# version: 1.0.0

# Importing traditional libraries
import datetime
import sqlite3
import sys

# Importing internal libraries
from keys import KEYS  # not an issue

# Importing libraries
import pandas as pandas
from sqlalchemy import create_engine
import requests


# Extracts daily_series from TIME_SERIES_DAILY function
def request_daily_series_for(symbol):
    try:
        alpha_vantage_url = \
            f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={KEYS['api_key']}"
        alpha_vantage_request = requests.get(url=alpha_vantage_url)
        alpha_vantage_content = alpha_vantage_request.json()
        alpha_vantage_daily_series = alpha_vantage_content["Time Series (Daily)"]
        return alpha_vantage_daily_series
    except KeyError:
        return None


# Converts daily_series json to dataframe
def daily_series_to_dataframe(symbol, series):
    dataframe_input = {"id": [],
                       "symbol": [],
                       "dailyDate": [],
                       "open": [],
                       "high": [],
                       "low": [],
                       "close": [],
                       "volume": []}
    for daily_serial in series:
        daily_serial_values = series[daily_serial]
        daily_serial_daily_date = datetime.datetime.strptime(daily_serial, "%Y-%m-%d")
        daily_serial_open = daily_serial_values["1. open"]
        daily_serial_high = daily_serial_values["2. high"]
        daily_serial_low = daily_serial_values["3. low"]
        daily_serial_close = daily_serial_values["4. close"]
        daily_serial_volume = daily_serial_values["5. volume"]
        daily_date = str(daily_serial_daily_date)[:10]
        daily_id = symbol+daily_date.replace("-", "")
        dataframe_input["id"].append(daily_id)
        dataframe_input["symbol"].append(symbol)
        dataframe_input["dailyDate"].append(str(daily_serial_daily_date)[:10])
        dataframe_input["open"].append(float(daily_serial_open))
        dataframe_input["high"].append(float(daily_serial_high))
        dataframe_input["low"].append(float(daily_serial_low))
        dataframe_input["close"].append(float(daily_serial_close))
        dataframe_input["volume"].append(int(daily_serial_volume))
    output_dataframe = pandas.DataFrame(data=dataframe_input)
    return output_dataframe


# Creates temp_table using dataframe
def dataframe_to_temp_table(input_dataframe):
    engine = create_engine('sqlite:///SQLite_Convest.db')
    input_dataframe.to_sql('temp_table', engine, if_exists='replace', index=False)


# [Creates SQLite_Convest] [Creates daily_series table] Insert temp_table content into daily_series
def insert_or_ignore_database():
    connection = sqlite3.connect("SQLite_Convest.db")
    sqlite_cursor = connection.cursor()
    sqlite_insert_query = f"""
                           INSERT OR IGNORE INTO daily_series
                           SELECT * FROM temp_table;                       
                           """
    try:
        sqlite_cursor.execute(sqlite_insert_query)
    except sqlite3.OperationalError as e:
        error_message = str(e)
        if "no such table: daily_series" in error_message:
            print("Creating daily_series table!")
            sqlite_create_query = f"""
                                   CREATE TABLE daily_series (
                                       id TEXT PRIMARY KEY,
                                       symbol TEXT NOT NULL,
                                       dailyDate TEXT NOT NULL,
                                       open FLOAT NOT NULL,
                                       high FLOAT NOT NULL,
                                       low FLOAT NOT NULL,
                                       close FLOAT NOT NULL,
                                       volume BIGINT NOT NULL
                                   );
                                   """
            sqlite_cursor.execute(sqlite_create_query)
    sqlite_cursor.execute(sqlite_insert_query)
    connection.commit()
    sqlite_cursor.close()
    connection.close()


# [Creates SQLite_Convest] Update daily_series content using temp_table
def update_database():
    connection = sqlite3.connect("SQLite_Convest.db")
    sqlite_cursor = connection.cursor()
    sqlite_update_query = \
        f"""
        UPDATE daily_series 
        SET 
            open = (SELECT temp_table.open FROM temp_table WHERE temp_table.id = daily_series.id),
            high = (SELECT temp_table.high FROM temp_table WHERE temp_table.id = daily_series.id), 
            low = (SELECT temp_table.low FROM temp_table WHERE temp_table.id = daily_series.id), 
            close = (SELECT temp_table.close FROM temp_table WHERE temp_table.id = daily_series.id), 
            volume = (SELECT temp_table.volume FROM temp_table WHERE temp_table.id = daily_series.id)
        WHERE id IN (SELECT id FROM temp_table WHERE temp_table.id= daily_series.id);                      
        """
    sqlite_cursor.execute(sqlite_update_query)
    connection.commit()
    sqlite_cursor.close()
    connection.close()


# [Creates SQLite_Convest] Converts SELECT * FROM daily_series WHERE symbol = [symbol] into .csv
def database_to_csv(symbol):
    connection = sqlite3.connect("SQLite_Convest.db")
    now = str(datetime.datetime.now()).split(" ")
    now_date = now[0].replace("-", "")
    now_time = now[1][:8].replace(":", "")
    csv_name = f"{symbol}_{now_date}_{now_time}.csv"
    try:
        csv_dataframe = pandas.read_sql_query(f"""
                                               SELECT * FROM daily_series WHERE symbol = "{symbol}"
                                               """, connection)
        content_exists = len(csv_dataframe) > 0
        if content_exists:
            csv_dataframe.to_csv(csv_name, index=False)
        else:
            print(f"SymbolExistenceError: {symbol} does NOT exist on database.")
            csv_name = None
    except pandas.io.sql.DatabaseError:  # not an issue
        print("ConversionError: This is NOT a valid conversion request.")
        print("You need to perform extraction before trying to generate a csv!")
        print("Try 'main.py symbol -e' and then 'main.py symbol -c'.")
        csv_name = None
    connection.close()
    return csv_name


# Main method
def main():
    try:
        symbol = sys.argv[1]
        action = sys.argv[2]
        if action == "-e" or action == "--extract":
            daily_series = request_daily_series_for(symbol)
            request_is_successfull = daily_series is not None
            if request_is_successfull:
                dataframe = daily_series_to_dataframe(symbol, daily_series)
                dataframe_to_temp_table(dataframe)
                insert_or_ignore_database()
                update_database()
                print(f"{symbol} extraction performed successfully!")
            else:
                print(f"SymbolError: {symbol} is NOT a valid symbol. Or maybe your API key on keys.py is wrong?")
        elif action == "-c" or action == "--csv":
            csv_name = database_to_csv(symbol)
            conversion_is_successful = csv_name is not None
            if conversion_is_successful:
                print(f"{symbol} conversion performed successfully!")
    except IndexError:
        print("SyntaxError: This is NOT a valid syntax.")
        print("Please use the following:")
        print("-e | --extract Extracts symbol and updates database")
        print("-c | --csv     Extracts symbol and convert to .csv")
        print("main.py symbol [-e | --extract | -c | --csv]")


if __name__ == '__main__':
    main()
