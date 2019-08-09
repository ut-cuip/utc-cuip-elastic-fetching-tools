import datetime
import json
import smtplib
import sys
import time
from email.message import EmailMessage
import multiprocessing

import pandas as pd
from elasticsearch import Elasticsearch
from concurrent.futures import ProcessPoolExecutor

import tqdm


def notify(auth, receiver, task_name=""):
    """
        Sends an email to `to_email` letting them know their task is complete
        Args:
            auth (dict): A dict containing keys "email" and "password" for authentication for sending the email
            receiver (str): The email address receiving this notification
            task_name (str) [optional]: The name of the task that has been completed
        Returns:
            True if email could be sent; False otherwise
    """
    try:
        subject = "Your task {}has completed".format(
            (task_name + " ") if task_name else ""
        )
        body = "Your task {}in file {} has completed".format(
            (task_name + " ") if task_name else "", sys.argv[0]
        )
        message = "Subject: {}\n\n{}".format(subject, body)
        server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
        server.ehlo()
        server.login(auth["email"], auth["password"])
        server.sendmail(auth["email"], receiver, message)
        return True
    except:
        return False


def parse_query(query_range):
    start = query_range[0]
    end = query_range[1]
    collective_df = pd.DataFrame(
        columns=[
            "camera_id",
            "hit_counts",
            "id",
            "intersection",
            "label",
            "locations",
            "pole_id",
            "timestamp",
        ]
    )
    elastic_index = "cuip_vision_events"
    elastichost = {"host": "scmgmt2.research.utc.edu", "port": 9200}
    total_processed, num_processed = 0, 0

    es = Elasticsearch(hosts=[elastichost])
    results = es.search(
        index=elastic_index,
        body={
            "query": {
                "range": {
                    "timestamp": {
                        "gte": int(start.timestamp() * 1000),
                        "lte": int(end.timestamp() * 1000),
                        "format": "epoch_millis",
                    }
                }
            }
        },
        filter_path=['hits.hits', '_scroll_id'],
        scroll="60m",
        size=1000,
    )
    scroll_id = results["_scroll_id"]
    docs = results["hits"]["hits"]
    start = time.time()
    while True:
        for doc in docs:
            event = doc["_source"]
            # Fix hit counts sometimes not being there
            if not "hit_counts" in event:
                event["hit_counts"] = len(event["locations"])
            event["locations"] = str(event["locations"])
            data_slice = pd.DataFrame(event, index=[0])
            data_slice.insert(len(event), "timestamp-iso", data_slice["timestamp"], True)
            data_slice["timestamp-iso"] = pd.to_datetime(data_slice["timestamp-iso"], unit="ms")
            collective_df = collective_df.append(data_slice, ignore_index=True, sort=False)
            del data_slice, event
            if "--debug" in sys.argv:
                num_processed += 1
                total_processed += 1
                if time.time() - start >= 1:
                    print(
                        "Processing {0:03d} items/s    |    {1:07d} processed in total".format(
                            num_processed, total_processed
                        )
                    )
                    start = time.time()
                    num_processed = 0
        docs = es.scroll(scroll_id=scroll_id, scroll="60m")["hits"]["hits"]
        if not docs:
            return collective_df


if __name__ == "__main__":
    with open("auth.json") as json_file:
        auth = json.loads(json_file.read())
    all_df = pd.DataFrame(
        columns=[
            "camera_id",
            "hit_counts",
            "id",
            "intersection",
            "label",
            "locations",
            "pole_id",
            "timestamp",
            "timestamp-iso"
        ]
    )
    # A dict of start and end times for each day between 4/1/19 and 4/30/19
    acceptable_ranges = [
        (
            datetime.datetime(2019, 6, x),
            datetime.datetime(2019, 6, x, 23, 59, 59, 999999),
        )
        for x in range(1, 31)
    ]
    query_ranges = []
    for day in range(1, 31):
        for hour in range(0, 24):
            query_ranges.append(
                (
                    datetime.datetime(2019, 6, day, hour, 0, 0, 0),
                    datetime.datetime(2019, 6, day, hour, 59, 59, 999999),
                )
            )

    """
        Multiprocessing Chunk:
    """
    with ProcessPoolExecutor(multiprocessing.cpu_count() // 2) as pool_executor:
        slices = list(tqdm.tqdm(pool_executor.map(parse_query, query_ranges)))
        for data_slice in slices:
            all_df = all_df.append(data_slice, ignore_index=True, sort=False)

    cam_ids = all_df.camera_id.unique().tolist()
    for cam_id in cam_ids:
        # Narrows down by camera_id
        df_by_cam_id = all_df.query("camera_id == '{}'".format(cam_id))
        # Narrows down by days
        for minimum, maximum in acceptable_ranges:
            mask = (df_by_cam_id["timestamp-iso"] > minimum) & (
                df_by_cam_id["timestamp-iso"] <= maximum
            )
            by_day = df_by_cam_id.loc[mask]
            if by_day.size != 0:
                by_day.to_csv(
                    "./csv/{}_{}.csv".format(cam_id, str(minimum), date_format="unix")
                )
    all_df.to_csv("./csv/all.csv")
    print("Done processing data")
#    notify(auth, "stovallj1995@gmail.com", "csv parsing video events from elastic")
