import time
import json
import logging
import pathlib
import argparse
from datetime import datetime, timedelta, timezone
from functools import partial, wraps
from itertools import pairwise

import requests
import pandas as pd
from tqdm import tqdm


API_ENDPOINT = "https://api.pushshift.io/reddit/search/comment/"
QUERY_STRING = "-moscopole -moscone mosco*|kremlin*|russia*|putin*"
SUBREDDIT = "europe"
COMMENTS_FILENAME = "comments.jsonl"
METADATA_FILENAME = "metadata.json"


class TqdmLoggingHandler(logging.StreamHandler):
    """Logging stream-like handler to make sure logging works with tqdm
    https://github.com/tqdm/tqdm/issues/193
    """

    def __init__(self):
        logging.StreamHandler.__init__(self)

    def emit(self, record):
        msg = self.format(record)
        tqdm.write(msg)


def timeit(f):
    @wraps(f)
    def wrap(*args, **kwargs):
        t0 = time.time()
        result = f(*args, **kwargs)
        t1 = time.time()
        logging.info(("Function {f_name} args:[{args}, {kwargs}]"
                      + " took: {time:.2f} secs.") \
            .format(f_name=f.__name__, 
                    args=args, 
                    kwargs=kwargs, 
                    time=t1-t0))
        return result
    return wrap


def send_request(parameters, timeout, endpoint=API_ENDPOINT):
    while True:
        
        response = requests.get(endpoint, params=parameters)
        
        if response.status_code == 200:
            response = response.json()
            metadata = response["metadata"] if "metadata" in response.keys() else None
            data = response["data"]
            return (metadata, data)
        
        elif response.status_code == 429:
            logging.info(f"Rate limit reached sleeping for {timeout} secs.")
            time.sleep(timeout)
        
        elif response.status_code >= 500:
            logging.info(("Server error (HTTP {status_code}), "
                        + "sleeping for {timeout} secs.") \
                            .format(status_code=response.status_code,
                                    timeout=timeout))
            time.sleep(timeout)
        
        else:
            raise NotImplementedError(f"HTTP status code {response.status_code}")


def format_query_params(after, size, before=None, include_metadata=False,
                        query_str=QUERY_STRING, subreddit=SUBREDDIT):
    params = {
        "q": query_str, 
        "subreddit": subreddit,
        "metadata": include_metadata, 
        "size": size,
        "after": after,
        "order": "asc"
    }
    if before is not None:
        params["before"] = before
    return params


@timeit
def get_daily_comments(after_datetime, before_datetime, batch_size, timeout):
    after_timestamp = int(after_datetime.timestamp())
    before_timestamp = int(before_datetime.timestamp())
    
    get_next_query_params = partial(format_query_params, before=before_timestamp, size=batch_size)
    
    params = get_next_query_params(after=after_timestamp, include_metadata=True)
    metadata, comments = send_request(params, timeout=timeout)

    iteration = 0
    logging.info("Batch {iter_}, {batch_size} ({downloaded}/{available})" \
        .format(iter_=iteration,
                batch_size=len(comments),
                downloaded=len(comments),
                available=metadata["es"]["hits"]["total"]["value"]))

    while True:
        # `after` param relation changed from "gt" to "gte" hence + 1
        params = get_next_query_params(after=int(comments[-1]["created_utc"]) + 1)
        _, data = send_request(params, timeout=timeout)
        
        if bool(data):
            comments += list(data)
            iteration += 1
            logging.info("Batch {iter_}, {batch_size} ({downloaded}/{available})" \
                .format(iter_=iteration,
                        batch_size=len(data),
                        downloaded=len(comments),
                        available=metadata["es"]["hits"]["total"]["value"]))
        else:
            break
    
    return metadata, comments


def main(start_date, end_date, batch_size, timeout, dest_folder, force_rewrite):

    # Generating a list of days beween the start date and the end date
    if end_date is None:
        date_range = pd.date_range(start_date, periods=2, tz=timezone.utc)
    else:
        date_range = pd.date_range(start_date, end_date)


    for i, (after_datetime, before_datetime) in enumerate(tqdm(pairwise(date_range))):
        target_date_str = after_datetime.strftime("%Y-%m-%d")

        # Skip if data for this date already exists 
        # and should not be overwritten
        target_folder = dest_folder.joinpath(target_date_str)
        if target_folder.exists() and target_folder.is_dir() and not force_rewrite:
            logging.info("Data for {date} already exist, skipping" \
                .format(date=target_date_str))
            continue

        logging.info("Getting data for {date}...".format(date=target_date_str))
        metadata, comments = get_daily_comments(after_datetime, before_datetime,
                                                batch_size, timeout)

        # Write data to disk
        target_folder.mkdir(parents=True, exist_ok=True)

        comments_file = target_folder.joinpath(COMMENTS_FILENAME)
        with open(comments_file, "w", encoding="utf8") as fp:
            for entry in comments:
                json.dump(entry, fp, ensure_ascii=False)
                fp.write("\n")
            
        metadata_file = target_folder.joinpath(METADATA_FILENAME)
        with open(metadata_file, "w", encoding="utf8") as fp:
            json.dump(metadata, fp, ensure_ascii=False, indent="\t")
        

def date_str_to_ISO_8601(date_str):
    try:
        return datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
    except ValueError:
        msg = "Date must be specified in \"YYYY-MM-DD\" format."
        raise argparse.ArgumentTypeError(msg)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""Retrieve Reddit comments using pushshift.io API"""
    )
    parser.add_argument("--start", required=True, type=date_str_to_ISO_8601,
        metavar="YYYY-MM-DD", 
        help="""Start date (inclusive) after which comments should be requested. 
                ISO 8601 format (YYYY-MM-DD). Default timezone UTC.""")

    parser.add_argument("--end", required=False, type=date_str_to_ISO_8601,
        metavar="YYYY-MM-DD",  
        help="""End date (exclusive) before which comments should be requested. 
                ISO 8601 format (YYYY-MM-DD). Default timezone UTC.""")
    
    parser.add_argument("--batch-size", type=int, nargs="?", const=500, 
        default=500, 
        help="""Batch size for a single request.""")

    
    parser.add_argument("--timeout", type=int, nargs="?", const=10, default=10, 
        help="""Timeout (seconds) if a rate limit is reached.""")


    parser.add_argument("--dest_folder", required=False, type=pathlib.Path, 
        default=pathlib.Path("data/comments/raw"), 
        help="""Destination folder where comments should be saved to. 
                Defaults to `data/comments/raw` inside project directory.""")

    parser.add_argument("--force-rewrite", action="store_true", dest="force_rewrite",
        help="""Force rewrite data even if a version of it exists already.""")
    
    # Optional TODOs:
    # TODO: add query string parameter

    args = parser.parse_args()

    logging.basicConfig(format="%(asctime)s : %(levelname)s : %(message)s", 
                        level=logging.INFO, handlers=[TqdmLoggingHandler()])
        
    main(start_date=args.start, end_date=args.end, batch_size=args.batch_size,
         timeout=args.timeout, dest_folder=args.dest_folder, 
         force_rewrite=args.force_rewrite)