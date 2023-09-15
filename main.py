import datetime as dt
import urllib.request
import pandas as pd
import numpy as np
import datetime as dt
import requests
import json
from google.cloud import bigquery
from google.cloud import bigquery_storage_v1beta1

bqclient = bigquery.Client()
bqstorageclient = bigquery_storage_v1beta1.BigQueryStorageClient()

class Region:
    def __init__(self, name, lat1, lon1, lat2, lon2, slack_channels=dict(), last_stats=dict(), alerts=dict(), last_msg_sent_time=None):
        self.name = name
        self.lat1 = lat1
        self.lon1 = lon1
        self.lat2 = lat2
        self.lon2 = lon2
        self.slack_channels = slack_channels
        self.last_stats = last_stats
        self.alerts = alerts
        self.last_msg_sent_time = last_msg_sent_time

        
    def set_slack_channels(self, slack_channels):
        self.slack_channels = slack_channels
    
    def set_last_stats(self, last_stats):
        self.last_stats = last_stats
        
    def set_alerts(self, alerts):
        self.alerts = alerts
    
    def set_last_msg_sent_time(self, last_msg_sent_time):
        self.last_msg_sent_time = last_msg_sent_time
    
    def set_stats(self, stats):
        self.stats = stats
    
    def update_bq(self, bqclient):
        if self. last_msg_sent_time is None:
            lmst = '1900-1-1 00:00:00'
        else:
            lmst = self.last_msg_sent_time
            
        query_string = "UPDATE `eartquakes_turkey.regions` SET lat1={}, lon1={}, lat2={}, lon2={}, slack_channels='{}', last_stats='{}', alerts = '{}', last_msg_sent_time='{}' WHERE name = '{}'".format(self.lat1, self.lon1,
                                                                                                                                                       self.lat2, self.lon2,
                                                                                                                                                       json.dumps(self.slack_channels),
                                                                                                                                                       json.dumps(self.last_stats),
                                                                                                                                                       json.dumps(self.alerts),
                                                                                                                                                       lmst,
                                                                                                                                                       self.name)
        return bqclient.query(query_string).result()
        
    def get_earthquakes_in_region(self, cur_time, bqclient, bq_storage_client):
        
        query_string = "SELECT datetime, MD, ML, MW, place, ResolutionQuality FROM `eartquakes_turkey.earthquakes` WHERE lat between {} and {} and lon between {} and {} and datetime>'{}' order by 1 desc".format(self.lat1, self.lat2, self.lon1, self.lon2, cur_time - dt.timedelta(1))
        df = bqclient.query(query_string).result().to_dataframe(bqstorage_client=bq_storage_client)

        return df
    
    def calculate_region_stats(self, df, cur_time):
        stats = dict()
        stats["1 day"] = len(df)
        stats["1 hour"] = len(df.loc[df['datetime'] > cur_time - dt.timedelta(1.0/24)])
        stats["3 hours"] = len(df.loc[df['datetime'] > cur_time - dt.timedelta(3.0/24)])
        stats["6 hours"] = len(df.loc[df['datetime'] > cur_time - dt.timedelta(6.0/24)])
        stats["12 hours"] = len(df.loc[df['datetime'] > cur_time - dt.timedelta(12.0/24)])
        
        return stats
    
def crawl_earthquakes():
    page = urllib.request.urlopen('http://www.koeri.boun.edu.tr/scripts/lst0.asp')
    content = page.read().decode('iso-8859-9')
    data = content.split('<pre>')[1].split('</pre>')[0]

    rows = data.split('\r\n')

    equakes = list()
    count = 0

    for line in rows:
        columns = line.split()
        try:
            eq = list()
            eq.append(dt.datetime.strptime(columns[0] + columns[1],'%Y.%m.%d%H:%M:%S'))
            eq.append(float(columns[2]))
            eq.append(float(columns[3]))
            eq.append(float(columns[4]))

            md = None
            ml = None
            mw = None
            if not columns[5] == '-.-':
                md = float(columns[5])
            if not columns[6] == '-.-':
                ml = float(columns[6])    
            if not columns[7] == '-.-':
                ml = float(columns[7])

            eq += [md, ml, mw]

            if "REVIZE" in line:
                eq.append(' '.join(columns[8:-3]))
                eq.append(' '.join(columns[-3:]))
            else:
                eq.append(' '.join(columns[8:-1]))
                eq.append(columns[-1])

        except: 
            continue
        else:
            equakes.append(eq)

    column_names = ['datetime', 'lat', 'lon', 'depth', 'MD', 'ML', 'MW', 'place', 'ResolutionQuality']
    df_web = pd.DataFrame(equakes, columns=column_names)
    df_web["datetime"] = df_web["datetime"].dt.tz_localize('UTC')
    df_web["MD"] = df_web["MD"].astype(np.float64)
    df_web["ML"] = df_web["ML"].astype(np.float64)
    df_web["MW"] = df_web["MW"].astype(np.float64)

    return df_web

def insert_earthquakes_to_db(earthquakes_df):
    table_id = 'eartquakes_turkey.earthquakes'
    # Since string columns use the "object" dtype, pass in a (partial) schema
    # to ensure the correct BigQuery data type.
    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("place", "STRING"),
        bigquery.SchemaField("ResolutionQuality", "STRING"),

    ])

    job = bqclient.load_table_from_dataframe(
        earthquakes_df, table_id, job_config=job_config
    )

    # Wait for the load job to complete.
    return job.result()


def collect_earthquakes():
    eq_df = crawl_earthquakes()
   
    query_string = "delete from `eartquakes_turkey.earthquakes` where datetime >= '{}'".format(str(eq_df['datetime'].dt.tz_localize(None).min()))
    results = bqclient.query(query_string).result()
    
    insert_earthquakes_to_db(eq_df)
    
    return eq_df
    
def main(request=None):
    
    #Collect Earthquake Data
    new_eq = collect_earthquakes()
    
    regions = get_regions_from_db()
    cur_time = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None) + dt.timedelta(3/24)
    cur_time_tz = pd.to_datetime(str(cur_time)).tz_localize('UTC')
    cur_date = cur_time.date()
    last_eq_date = max(new_eq['datetime']).date()
    
    for r in regions:
        r = regions[r]
        region_eq = r.get_earthquakes_in_region(cur_time, bqclient, bqstorageclient)
        r.set_stats(r.calculate_region_stats(region_eq, cur_time_tz))
        if r.stats != r.last_stats or r.last_msg_sent_time is None or (r.last_msg_sent_time is not None and r.last_msg_sent_time.date() < cur_date and (cur_time.hour >= 1 or last_eq_date == cur_date)):
            message = prepare_message(r.stats, r.alerts, r.name, r.last_stats)
            send_message_to_slack(message, r.slack_channels.values())
            r.last_msg_sent_time = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None) + dt.timedelta(3/24)
            r.last_stats = r.stats
        
        r.update_bq(bqclient)
    
    add_execution_time_to_db()
    bqclient.close()
    
    
def prepare_message(stats, alerts, region_name, last_stats):
    
    message = "---{} BÖLGESİ DEPREM RAPORU---\n\nDeprem Sayıları:\n---------------\n".format(region_name)
    message += "Son 1 saat: {}\nSon 3 saat: {}\nSon 6 saat: {}\nSon 12 saat: {}\nSon 24 saat: {}\n\n".format(stats["1 hour"], stats["3 hours"], stats["6 hours"], stats["12 hours"],stats["1 day"])
    
    message += "Deprem Haritası: " + "http://udim.koeri.boun.edu.tr/zeqmap/osmap.asp\n"
    message += "Depremler Liste: " + "http://www.koeri.boun.edu.tr/scripts/lst0.asp"
    
    
    
    if "1 hour" in last_stats and stats["1 hour"] > last_stats["1 hour"] and stats["1 hour"] >= alerts["1 hour"]:
        alert_message = "!!!DİKKAT!!!\n{} BÖLGESİNDE SON 1 SAATTE {} YA DA DAHA FAZLA DEPREM OLDU!!!\n@channel \n\n\n".format(region_name, alerts["1 hour"], stats["1 hour"])
        message = alert_message + message

    elif "3 hours" in last_stats and stats["3 hours"] > last_stats["3 hours"] and stats["3 hours"] >= alerts["3 hours"]:
        alert_message = "!!!DİKKAT!!!\n{} BÖLGESİNDE SON 3 SAATTE {} YA DA DAHA FAZLA DEPREM OLDU!!!\n@channel \n\n\n".format(region_name, alerts["3 hours"], stats["3 hours"])
        message = alert_message + message

    elif "6 hours" in last_stats and stats["6 hours"] > last_stats["6 hours"] and stats["6 hours"] >= alerts["6 hours"]:
        alert_message = "!!!DİKKAT!!!\n{} BÖLGESİNDE SON 6 SAATTE {} YA DA DAHA FAZLA DEPREM OLDU!!!\n@channel \n\n\n".format(region_name, alerts["6 hours"], stats["6 hours"])
        message = alert_message + message

    elif "12 hours" in last_stats and stats["12 hours"] > last_stats["12 hours"] and stats["12 hours"] >= alerts["12 hours"]:
        alert_message = "!!!DİKKAT!!!\n{} BÖLGESİNDE SON 12 SAATTE {} YA DA DAHA FAZLA DEPREM OLDU!!!\n@channel \n\n\n".format(region_name, alerts["12 hours"], stats["12 hours"])
        message = alert_message + message

    return message


def send_message_to_slack(message, urls):
    slack_msg = {'text': message, 'link_names': 1}
    
    for web_hook_url in urls:    
        requests.post(web_hook_url, data=json.dumps(slack_msg))

        
def get_regions_from_db():
    regions = dict()
    
    query_string = "SELECT * FROM `eartquakes_turkey.regions`"
    results = bqclient.query(query_string).result()
    
    for r in results:
        region = Region(r.get("name"), 
                        r.get("lat1"), 
                        r.get("lon1"), 
                        r.get("lat2"), 
                        r.get("lon2"))
        
        slack_channels = r.get("slack_channels")
        last_stats = r.get("last_stats")
        last_msg_sent_time = r.get("last_msg_sent_time")
        alerts = r.get("alerts")
        
        region.set_last_msg_sent_time(last_msg_sent_time)
        
        try:
            region.set_slack_channels(json.loads(slack_channels))
        except:
            print("Slack channels of {} region could not be loaded".format(r.get('name')))
        
        try:
            region.set_alerts(json.loads(alerts))
        except:
            print("Alerts of {} region could not be loaded".format(r.get('name')))
        
        try:
            region.set_last_stats(json.loads(last_stats))
        except:
            print("Last statistics of {} region could not be loaded".format(r.get('name')))
        
        regions[r.name] = region
        
    return regions

def add_execution_time_to_db():
    query_string = "insert into `eartquakes_turkey.task_executions` (execution_time) values (CURRENT_DATETIME('Asia/Istanbul'))"
    return bqclient.query(query_string).result()
