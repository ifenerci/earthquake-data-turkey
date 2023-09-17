"""
This module contains collection of functions which fetches earthquake data 
from Bogazici Unviersity Kandilli Observatory web page.
"""
import datetime as dt
from urllib import request
from bs4 import BeautifulSoup
from numpy import float64
from pandas import DataFrame


def get_web_data(url: str) -> str:
    """
    Gets raw html website data from the specified url.

    :url: URL link for the website
    :return: Raw website html data
    """
    return request.urlopen(url).read().decode('iso-8859-9')


def parse_raw_line(line: str) -> list:
    """
    Splits an input raw line into columns by applying several rules.

    :line: line input
    :returns: splitted columns in a list string
    """
    columns = line.split()

    result = [
        dt.datetime.strptime(columns[0] + columns[1],
                             '%Y.%m.%d%H:%M:%S'
                             ), #First two columns date & time
        float(columns[2]), 
        float(columns[3]), 
        float(columns[4]),
        None if columns[5] == '-.-' else columns[5],
        None if columns[6] == '-.-' else columns[6],
        None if columns[7] == '-.-' else columns[7]
    ]

    if "REVIZE" in line:
        result.extend(
            [
                ' '.join(columns[8:-3]),
                ' '.join(columns[-3:])
            ]
        )
    else:
        result.extend(
            [
                ' '.join(columns[8:-1]),
                ' '.join(columns[-1])
            ]
        )
    return result


def parse_web_data(html: str) -> DataFrame:
    """
    Parses the earthquake html data and converts it to a Pandas dataframe.

    :html: Html string from the website.
    :returns: Earthquakes dataframe
    """
    soup = BeautifulSoup(html, 'html.parser')
    if soup.pre is not None:
        raw_lines = soup.pre.text.splitlines()[7:-1]
    else:
        raise ValueError("Related data section \"pre\" cannot be found")

    splitted = [parse_raw_line(line) for line in raw_lines]
    columns = ['datetime', 'lat', 'lon', 'depth', 
               'MD', 'ML', 'MW', 'place', 'ResolutionQuality']

    eq_df = DataFrame(data=splitted, columns=columns)
    eq_df["datetime"] = eq_df["datetime"].dt.tz_localize('UTC')
    eq_df["MD"] = eq_df["MD"].astype(float64)
    eq_df["ML"] = eq_df["ML"].astype(float64)
    eq_df["MW"] = eq_df["MW"].astype(float64)

    return eq_df
