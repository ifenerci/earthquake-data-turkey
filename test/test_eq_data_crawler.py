"""
Test Module for eq_data_crawler
"""
from pytest import raises, fixture
from eq_data_crawler import get_web_data
from eq_data_crawler import parse_web_data


@fixture()
def get_sample_response():
    """
    Reads sample html file and gets the test for  a testcase
    """
    html_file = open("test/misc/sample_eq_page.html")
    html = html_file.read()
    html_file.close()
    return html


def test_get_web_data__returns_html_output():
    """
    If the correct url returns an html output.
    """
    url = 'http://www.koeri.boun.edu.tr/scripts/lst0.asp'
    html_output = get_web_data(url)

    assert 'html' in html_output.lower()


def test_get_web_data__non_existing_url():
    """
    If a nonexisting url throws an exception.
    """
    url = 'thisIsABrokenURL.COMMMM'

    with raises(Exception):
        get_web_data(url)


def test_parse_web_data(get_sample_response):
    """
    Test case just expects to read the sample file into a dataframe 
    without and exception and return with correct size.
    """
    try:
        eq_df = parse_web_data(get_sample_response)
    except Exception:
        assert False
    else:
        assert eq_df.shape == (500, 9)