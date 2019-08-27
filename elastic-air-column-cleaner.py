import glob

import pandas as pd


def clean(files):
    cols_to_drop = [
        "SensorId",
        "Adc",
        "response_date",
        "key2_response_b",
        "wlstate",
        "pm2.5_aqi",
        "ts_s_latency_b",
        "DateTime",
        "httpsuccess",
        "key2_count_b",
        "key2_response_date_b",
        "period",
        "hardwareversion",
        "httpsends",
        "memcs",
        "pressure",
        "key2_response",
        "version",
        "Geo",
        "status_6",
        "status_5",
        "status_8",
        "key1_response",
        "status_7",
        "Mem",
        "pm2.5_aqi_color",
        "status_9",
        "key1_count",
        "ts_latency_b",
        "key1_response_date",
        "status_0",
        "Id",
        "status_2",
        "status_1",
        "status_4",
        "status_3",
        "pm2.5_aqi_b",
        "loggingrate",
        "latency",
        "key1_count_b",
        "memfrag",
        "key1_response_b",
        "key2_count",
        "hardwarediscovered",
        "latency_b",
        "status_10",
        "response_b",
        "response_date_b",
        "memfb",
        "location",
        "response",
        "ts_s_latency",
        "ts_latency",
        "uptime",
        "ts_latency",
        "pa_latency",
        "pm2.5_aqi_color_b",
        "key2_response_date",
        "rssi",
        "place",
        "key1_response_date_b",
    ]
    for file in files:
        df = pd.read_csv(file)
        for col_to_drop in cols_to_drop:
            try:
                df = df.drop([col_to_drop], axis=1)
            except KeyError:
                pass
        # Get rid of empty cols
        for col_name in df.columns:
            if "Unnamed: " in col_name:
                df = df.drop([col_name], axis=1)
        df.to_csv(file)
        print("Done clearing out extra columns from {}".format(file))


if __name__ == "__main__":
    files = glob.glob("./csv/**/*.csv")
    clean(files)
    files = glob.glob("./csv/testing_dataset/**/*.csv")
    clean(files)
