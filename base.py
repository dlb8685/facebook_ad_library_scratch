"""Basic data fetch for Facebook"""
import csv
import logging
import re
import requests
import facebook
import operator
from retrying import retry

LOG = logging.getLogger(__name__)
_MAX_RETRIES = 10


def _date_checker(data, date, comparison_func):
    return_value = True
    for item in data:
        for value_dict in item.get("values", []):
            if "end_time" in value_dict:
                end_time_date = value_dict["end_time"]
                end_time_date = iso8601.parse_date(end_time_date)
                end_time_date = datetime.datetime.timestamp(end_time_date)
                date_comparison = comparison_func(date, end_time_date)
                return_value = return_value and date_comparison
            if not return_value:
                break
    return return_value


def start_checker(data, start_date):
    """Check if all given dates are >= start_date"""
    return _date_checker(data, start_date, operator.le)


def end_checker(data, end_date):
    """Check if all given dates are <= end_date"""
    return _date_checker(data, end_date, operator.ge)



def _retry_on_error(exc, package):
    """Log and retry on Facebook error. Template function since retry can only use
    one-argument functions
    """
    if isinstance(exc, (requests.HTTPError, facebook.GraphAPIError)):
        LOG.error("%s encountered a Facebook API error. Retrying Facebook request, up to %i times",
                  package, _MAX_RETRIES)
        return True
    return False


def retry_on_error_facebook(exc):
    """Retry on facebook-sdk error"""
    return _retry_on_error(exc, "facebook-sdk")


def retry_on_error_requests(exc):
    """Retry on requests error"""
    return _retry_on_error(exc, "requests")


def gen_csv(gen, columns, file_name):
    """Create csv from Facebook generator
    Parameters
    ----------
    gen : generator
        Generator of Facebook data
    columns : list
        List of columns
    file_name : string
        File to be filled with csv
    """
    with open(file_name, mode="w", newline="\n") as file_obj:
        dict_writer = csv.DictWriter(file_obj, columns, quoting=csv.QUOTE_ALL,
                                     restval=None, extrasaction="ignore")
        dict_writer.writeheader()
        dict_writer.writerows(gen)
        file_obj.close()


class BaseClient(object):
    """Base client for pulling Facebook data"""

    def __init__(self, access_token, version=None):
        if not version:
            version = max([float(x) for x in facebook.VALID_API_VERSIONS])
        self._client = facebook.GraphAPI(access_token=access_token, version=str(version))

    def get_data(self, path, start_date=None, end_date=None, **params):
        """Get data from Facebook given a Graph API path. Can take params
        valid for the path."""
        if not params:
            params = {}
        if start_date:
            params["since"] = start_date
        if end_date:
            params["until"] = end_date

        # Fetch first page with client
        # Use client to assemble the correct URL for first request, basically. From there
        # I can use the fully assembled URL sent for next page along with data.

        page = self._first_call(path, **params)
        previous_data = None
        first_time = True
        yielded_count = 0

        # hacky way to not blow through API rate limits (don't even know what they are, based on docs) until we get this up to speed a bit more.
        while yielded_count < 50:
            # Process fetched data
            data = page.get("data", [])

            # Yield if there's new data, in range
            # Break if data is past range or the same--FB will keep sending data for the
            # future, or the same data, forever
            if data:
                yielded = True
                for data_row in data:
                    yield data_row
                yielded_count += 1
            elif first_time:
                yield {}
                break
            else:
                yielded = False

            # Fetch new data, if any, with requests
            if yielded and "paging" in page and "next" in page["paging"]:
                print("paging/next: {}".format(page["paging"]["next"]))
                page = self._other_calls(page["paging"]["next"])
            else:
                break

            first_time = False

    def first_call_no_retry(self, path, **params):
        """First call to Graph API is via client, removes retry logic.
           useful for validating FB API requests at runtime.
        """
        # Per https://developers.facebook.com/docs/graph-api/using-graph-api:
        # "As a best practice, for large requests use a POST request instead of a GET request
        # and add a method=GET parameter. If you do this, the POST will be interpreted
        # as if it were a GET."
        if not params:
            params = {}
        params["method"] = "GET"
        print("first call params: {}/{}".format(path, params))
        return self._client.request(path, args=params, method="POST")

    @retry(retry_on_exception=retry_on_error_facebook,
           stop_max_attempt_number=_MAX_RETRIES,
           wait_exponential_multiplier=1000,
           wait_exponential_max=10000)
    def _first_call(self, path, **params):
        """First call to Graph API is via client"""
        return self.first_call_no_retry(path, **params)

    @classmethod
    @retry(retry_on_exception=retry_on_error_requests,
           stop_max_attempt_number=_MAX_RETRIES,
           wait_exponential_multiplier=1000,
           wait_exponential_max=10000)
    def _other_calls(cls, url):
        """Next to calls to Graph API are via requests, as client does not have
        built-in iterator. This is fine as Facebook provides full URL's for paging.
        """
        # Per https://developers.facebook.com/docs/graph-api/using-graph-api:
        # "As a best practice, for large requests use a POST request instead of a GET request
        # and add a method=GET parameter. If you do this, the POST will be interpreted
        # as if it were a GET."
        if re.search("method=GET", url, flags=re.I):
            params = {}
        else:
            params = {"method": "GET"}
        response = requests.post(url, params=params)

        try:
            response.raise_for_status()
        except requests.HTTPError:
            raise facebook.GraphAPIError(response)

        return response.json()