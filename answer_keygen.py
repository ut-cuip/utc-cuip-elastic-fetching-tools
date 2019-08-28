"""
Author:
    Jose Stovall

Turns a given CSV (via cmd args) into an answer key for the CSCDC

Commandline Args:
    path to csv file (str): The path (relative or absolute) to the CSV file to turn into an answer key for the CSCDC
"""

import datetime
import os
import sys

import pandas as pd
from dateutil import parser
from pip._vendor.colorama import Fore


def to_date_string(timestamp):
    """
    Args:
        timestamp (pandas.Timestamp): A Pandas Timestamp object
    Returns:
        date (str): a string date of form YYYY-MM-DD in its proper timezone
    """
    to_datetime = timestamp.to_pydatetime()
    to_datetime = to_datetime.replace(tzinfo=datetime.timezone.utc).astimezone(tz=None)
    return "{}-{}-{}".format(to_datetime.year, to_datetime.month, to_datetime.day)


def to_time_string(timestamp):
    """
    Args:
        timestamp (pandas.Timestamp): A Pandas Timestamp object
    Returns:
        time (str): a string time of form HH:MM:SS in its proper timezone
    """
    to_datetime = timestamp.to_pydatetime()
    to_datetime = to_datetime.replace(tzinfo=datetime.timezone.utc).astimezone(tz=None)
    return "{}:{}:{}".format(to_datetime.hour, to_datetime.minute, to_datetime.second)


def clean(dataframe):
    """
    Args:
        dataframe (pandas.DataFrame): A Pandas DataFrame Object
    Returns:
        cleaned dataframe (pandas.DataFrame): A Pandas DataFrame object without columns not needed for the answer key
    """
    cols_to_keep = ["date", "time", "nicename", "pm2_5_cf_1"]
    df = dataframe.copy()
    # Get rid of empty cols
    for col_name in df.columns:
        if not col_name in cols_to_keep:
            df.drop([col_name], inplace=True, axis=1)
    return df


def main(file_path):
    """
    Main program loop which will parse and clean the CSV file for you, saving to a new one if accepted.
    Args:
        file_path (str): Path (absolute or relative) to the CSV to convert

    """
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print(
            Fore.RED
            + "File could not be found. Please ensure that your path is correct and any double \\'s are removed."
            + Fore.RESET
        )
        exit()
    df["timestamp-iso"] = pd.to_datetime(df["timestamp-iso"])
    df["date"] = df["timestamp-iso"].apply(lambda x: to_date_string(x))
    df["time"] = df["timestamp-iso"].apply(lambda x: to_time_string(x))
    new_df = pd.DataFrame()

    # completion_map is a map of every hour of every day of every AQ sensor to see if we've gotten all the data we're supposed to
    completion_map = {}
    for nicename in df.nicename.unique().tolist():
        completion_map[nicename] = {}
        for day in range(25, 30):
            completion_map[nicename][day] = {}
            for hour in range(7, 20):
                completion_map[nicename][day][hour] = False

    # Start iterating and parsing out these dates to determine if the day is in range and the time is in range
    start_day = 25
    end_day = 29
    for _, row in df.iterrows():
        day = int(row.date[row.date.rindex("-") + 1 :])
        if day >= start_day and day <= end_day:
            hour = int(row.time[: row.time.index(":")])
            if hour not in completion_map[row.nicename][day]:
                continue
            minute = int(row.time[row.time.index(":") + 1 : row.time.rindex(":")])
            if minute == 0:
                new_df = new_df.append(row)
                completion_map[row.nicename][day][hour] = True
            elif minute == 1 and not completion_map[row.nicename][day][hour]:
                new_df = new_df.append(row)
                completion_map[row.nicename][day][hour] = True

    # Notify the user of any gaps in the data if there are any
    printed = False
    for nicename in df.nicename.unique().tolist():
        for day in range(25, 30):
            for hour in range(7, 20):
                if not completion_map[nicename][day][hour]:
                    if not printed:
                        # Print this just once - if there are no missing values this won't print at all!
                        print("Missing info for:")
                        printed = True
                    print("  • 2019-6-{}  |  {}:00".format(day, hour))

    # Prompt the user to save the file (append the word 'anyways' in the event that there was missing data) - notify user of save location
    prompt = input("Save{}? Y/N:  ".format(" anyways" if printed else ""))
    if prompt.lower() == "y":
        new_df = clean(new_df)
        name = (
            file_path
            if "--inplace" in sys.argv
            else "answer_key_{}.csv".format(datetime.datetime.now())
        )
        new_df.to_csv(name, index=False)
        print("Saved to {}".format(name))


if __name__ == "__main__":
    """
    Commandline Args:
        path to csv file (str): The path (relative or absolute) to the CSV file to turn into an answer key for the CSCDC
    """
    args = sys.argv
    if "--help" in args or "help" in args or len(args) < 2 or len(args) > 3:
        print(
            "Command-line Arguments ({}*{} indicates required):".format(
                Fore.CYAN, Fore.RESET
            )
        )
        print(
            Fore.CYAN
            + "  * path-to-csv: This can be absolute or relative. This should be the FIRST argument: 'python answer_keygen.py path-to-csv`"
            + Fore.RESET
        )
        print(
            Fore.LIGHTBLACK_EX
            + "  • --inplace: Whether or not to directly modify the CSV file you pass in. This is not recommended unless you make a backup"
            + Fore.RESET
        )
        print(Fore.LIGHTBLACK_EX + "  • --help, ?: Shows this." + Fore.RESET)
        exit()
    else:
        if "--inplace" in args:
            print(
                Fore.RED
                + "Running with --inplace flag - THE FILE WILL BE DIRECTLY MODIFIED"
                + Fore.RESET
            )
            print(
                Fore.RED
                + "Hit CTRL+C (^C) now to abort before it's too late"
                + Fore.RESET
            )
        main(args[1])
