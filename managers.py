"""Module for Persistency Managers"""
from io import BytesIO
from typing import Iterator, List, Optional
from abc import ABCMeta, abstractmethod
from tempfile import NamedTemporaryFile
import logging
from google.oauth2 import service_account
from jinja2 import Environment, loaders
import pandas as pd
from google.cloud import bigquery, storage
from c4v.scraper.scraped_data_classes.scraped_data import ScrapedData
from c4v.scraper.persistency_manager.base_persistency_manager import BasePersistencyManager
import re
import streamlit as sl

log = logging.getLogger("c4v-py-demo")


def extract_website_from_url(url) -> str:
    "Extract the website part for a given url and return a unique website hash."
    website_regex = r"(\w+\.)+\w+"
    regex_match = re.search(website_regex, url)
    if regex_match:
        return regex_match[0]
    raise LookupError(f"No website found in the url {url}")


def generate_content_filename(url: str, suffix="txt") -> str:
    "Given an url, generate a unique .txt filename"
    url_regex = r"(\w+\.)+\w+(/\w+)+"
    regex_match = re.search(url_regex, url)
    if regex_match:
        filename = regex_match[0].replace("/", "-")
        return f"{filename}.{suffix}"
    raise LookupError(f"No url regex match for {url}")


class DatabaseService(metaclass=ABCMeta):
    """Base class for having a database service for the c4v-py library"""
    @abstractmethod
    def get_all(self, limit, scraped=None) -> Iterator[pd.core.series.Series]:
        """
        Return an iterator over the set of stored instances

        Parameters:
        -----------
        limit : `int`
            Max amount of elements to retrieve
        scraped : `bool`
            True if retrieved data should be scraped, false if it shouldn't, None if not relevant
        """
        raise NotImplementedError("Implement get_all abstract method")


    @abstractmethod
    def get_scraped_urls_by_website(self, website: str) -> List[pd.DataFrame]:
        """
        Return a list containing the scraped urls for an specific website
        
        Parameters:
        -----------
        website : `str`
            The website of interest.
        """
        raise NotImplementedError("Implement get_scraped_urls_by_website abstract method")


    @abstractmethod
    def save(self, database_rows: List[dict]):
        """
        Store a row containing information about scraped data.
        
        Parameters:
        -----------
        database_row : `dict`
            The rows to store in the database.
        """
        raise NotImplementedError("Implement save abstract method")


    @abstractmethod
    def delete(self, urls: List[str]):
        """
        Delete a set of urls scraped data from the database service.

        Parameters:
        -----------
        urls : `List[str]`
            The list of urls to delete the scraped data from.
        """
        raise NotImplementedError("Implement delete abstract method")


class FileStorageService(metaclass=ABCMeta):
    """Base class for having a file storage service for the c4v-py library"""
    @abstractmethod
    def save_file(self, destination_file: str, csv_source_file: str):
        """
        Store a csv file in the file storage service.

        Parameters:
        -----------
        destination_file : `str`
            The destination filename to store in the file storage service.
        csv_source_file : `str`
            The local csv file to store in the file storage service.
        """
        raise NotImplementedError("Implement save_file abstract method")


    @abstractmethod
    def list_files(self, prefix: str = None, delimiter: str = None) -> List[str]:
        """
        List files with a given prefix.

        Parameters:
        -----------
        prefix : `str`
            The common prefix for files to look for.
        delimiter : `str`
            A suffix that the files should match.
        """
        raise NotImplementedError("Implement list_files abstract method")


class BigQueryService(DatabaseService):
    """
    BigQuery database service for c4v-py scraped data
    
    Parameters:
    -----------
    scrap_table : `str`
        The scrap table name in format `dataset_name.table_name`.
    """

    GET_SCRAPED_URLS_BY_WEBSITE_SQL = "get_scraped_urls.sql"
    GET_SCRAPED_URLS_SQL = "select_star_limit.sql"
    DELETE_SCRAPED_URLS_SQL = "delete_scraped_urls.sql"

    def __init__(self, scrap_table: str):
        # -- < AÑADIDO POR LUIS > ----------------------------------------------------------
        credentials = service_account.Credentials.from_service_account_info(sl.secrets['gcp_service_account'])
        self.client = bigquery.Client(credentials=credentials)
        # ----------------------------------------------------------------------------------
        self.scrap_table = self.client.get_table(scrap_table)
        self.jinja_env = Environment(cache_size=0, loader=loaders.FileSystemLoader("bq_sql/"))
        self.cached_scraped_urls = None


    def get_all(self, limit):
        query_template = self.jinja_env.get_template(self.GET_SCRAPED_URLS_SQL)
        query = query_template.render(table_name=self.scrap_table, limit=limit)
        log.info("Running query: %s", query)
        query_job = self.client.query(query, job_id_prefix="get_scraped_data")
        result_df: pd.DataFrame = query_job.to_dataframe()
        for row in result_df.iloc:
            yield row


    def get_scraped_urls_by_website(self, website: str):
        if self.cached_scraped_urls is not None and self.cached_scraped_urls["website"] == website:
            return self.cached_scraped_urls["data"]
        query_template = self.jinja_env.get_template(self.GET_SCRAPED_URLS_BY_WEBSITE_SQL)
        table_name = self.scrap_table.full_table_id.replace(":", ".")
        query = query_template.render(table_name=table_name, website=website)
        log.info("Running query: %s", query)
        bigquery_job = self.client.query(query, job_id_prefix="get_scraped_urls_by_website")
        self.cached_scraped_urls = {"website": website, "data": bigquery_job.to_dataframe()}
        return self.cached_scraped_urls["data"]


    def save(self, database_rows):
        dataframe = pd.DataFrame(database_rows)
        log.info("Storing %d rows in %s", len(dataframe), self.scrap_table.full_table_id)
        self.client.insert_rows_from_dataframe(self.scrap_table, dataframe)


    def delete(self, urls):
        query_template = self.jinja_env.get_template(self.DELETE_SCRAPED_URLS_SQL)
        query = query_template.render(table_name=self.scrap_table, urls_list="\",\"".join(urls))
        log.info("Running query: %s", query)
        bigquery_job = self.client.query(query, job_id_prefix="delete_scraped_urls")
        query_job_result = bigquery_job.result()


class GCSService(FileStorageService):
    """
    Google GCP Storage service for c4v-py scraped data
    
    Parameters:
    -----------
    bucket : `str`
        The bucket name to interact with.
    bucket_prefix : `str`
        The prefix for all interactions with the bucket.
    """
    def __init__(self, bucket: str, bucket_prefix: str):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket)
        self.bucket_prefix = bucket_prefix + "/scraped_data"


    def get_byte_stream(self, filepath: str):
        blob_name = filepath if filepath.startswith(self.bucket_prefix) \
            else f"{self.bucket_prefix}/{filepath}"
        log.info("Bucket %s: Retrieving file: %s", self.bucket.name, blob_name)
        blob = self.bucket.get_blob(blob_name)
        byte_stream = BytesIO()
        blob.download_to_file(byte_stream)
        byte_stream.seek(0)
        return byte_stream


    def save_file(self, destination_file, csv_source_file):
        blob_name = f"{self.bucket_prefix}/{destination_file}"
        log.info("Bucket %s: Saving file with name: %s", self.bucket.name, blob_name)
        blob = self.bucket.blob(blob_name)
        blob.upload_from_filename(csv_source_file)


    def list_files(self, prefix: str = None, delimiter: str = None):
        full_prefix = self.bucket_prefix if prefix is None else f"{self.bucket_prefix}/{prefix}"
        log.info("Bucket %s: Listing files with prefix: %s", self.bucket.name, full_prefix)
        blobs = self.client.list_blobs(self.bucket, prefix=full_prefix, delimiter=delimiter)
        return [blob.name for blob in blobs]


class GCPPersistencyManager(BasePersistencyManager):
    """
    Class to store c4v-py data in GCP Services
    
    Parameters:
    -----------
    bigquery_service : `DatabaseService`
        A Database service manager that supports query operations.
    classifiers_storage_service : `FileStorageService`
        A File storage service manager to store classifiers files.
    urls_storage_service : `Optional[FileStorageService]`
        A File storage service manager to store temporal url lists.
    run_id : `Optional[str]`
        An identifier for the run. It helps connecting scrapers, crawlers and classifiers
        persistency managers between different runs.
    """

    def __init__(
        self,
        bigquery_service: DatabaseService,
        classifiers_storage_service: FileStorageService,
        urls_storage_service: Optional[FileStorageService] = None,
        run_id: Optional[str] = "default",
    ):
        self.bigquery_service = bigquery_service
        self.classifiers_storage_service = classifiers_storage_service
        self.urls_storage_service = urls_storage_service
        self.run_id = run_id

    def get_all(self, limit, scraped = None):
        for result in self.bigquery_service.get_all(limit=limit):
            scraped_data = ScrapedData()
            scraped_data.url = result["url"]
            scraped_data.last_scraped = result["last_scraped"]
            scraped_data.title = result["title"].decode("utf-8")
            scraped_data.content = result["content"].decode("utf-8")
            scraped_data.author = result["author"].decode("utf-8")
            scraped_data.categories = [category.decode("utf-8") for category in result["categories"]]
            scraped_data.date = result["date"].decode("utf-8")
            yield scraped_data


    def filter_website_scraped_urls(self, website: str, website_urls: List[str]) -> List[str]:
        """
        Filter out the crawled urls related to an specific website
        
        Parameters:
        -----------
        website : `str`
            The website that all website_urls share.
        website_urls : `List[str]`
            The urls to filter.
        """
        log.info("Filtering %d urls", len(website_urls))
        scraped_urls = self.bigquery_service.get_scraped_urls_by_website(website)
        log.info("Got %d urls in database", len(scraped_urls))
        filtered_urls = [url for url in website_urls if url not in scraped_urls["url"]]
        return filtered_urls


    def filter_scraped_urls(self, urls: List[str]) -> List[str]:
        log.debug("Called filter_scraped_urls with %d urls", len(urls))
        urls_website = set([extract_website_from_url(url) for url in urls])
        log.debug("Websites in urls list: %s", urls_website)
        if len(urls_website) > 1:
            log.warning("Having multiple websites in urls list. Filtering for each website")
        filtered_urls = []
        while len(urls_website) > 0:
            website = urls_website.pop()
            website_urls = [url for url in urls if extract_website_from_url(url) == website]
            new_urls = self.filter_website_scraped_urls(website, website_urls)
            filtered_urls = filtered_urls + new_urls
        return filtered_urls


    def was_scraped(self, url: str) -> bool:
        url_website = extract_website_from_url(url)
        return len(self.filter_website_scraped_urls(url_website, [url])) == 0


    def extract_database_fields(self, scraped_data: ScrapedData) -> dict:
        """
        Extract the fields from the ScrapedData that should be stored in the database
        
        Parameters:
        -----------
        scraped_data : `ScrapedData`
            The scraped_data object to extract fields from.
        content_filepath : `str`
            A filepath containing the content property from the scraped_data object.
        """
        categories = [category.encode("utf-8") for category in scraped_data.categories]
        return {
            "title": scraped_data.title.encode("utf-8"),
            "content": scraped_data.content.encode("utf-8"),
            "author": scraped_data.author.encode("utf-8"),
            "date": scraped_data.date.encode("utf-8"),
            "categories": categories,
            "url": scraped_data.url,
            "website": extract_website_from_url(scraped_data.url),
            "last_scraped": scraped_data.last_scraped,
            "label": None,
            "source": None,
        }


    def save(self, url_data: List[ScrapedData]):
        database_rows = [self.extract_database_fields(scraped_data) for scraped_data in url_data]
        self.bigquery_service.save(database_rows)


    def delete(self, urls: List[str]):
        self.bigquery_service.delete(urls)


    def store_temp_urls(self, source_filename, destination_filename):
        destination_filename = f"{self.run_id}/{destination_filename}"
        self.urls_storage_service.save_file(destination_filename, source_filename)


    def list_temp_urls(self) -> List[str]:
        return self.urls_storage_service.list_files(prefix=self.run_id)


    def get_temp_file(self, filename: str) -> BytesIO:
        return self.urls_storage_service.get_byte_stream(filename)


    def list_classifiers(self) -> List[str]:
        return self.classifiers_storage_service.list_files(prefix=f"classifiers/")


def get_persistency_manager():

    # BigQuery configuration
    project_id = "event-pipeline"
    dataset_name = "sambil_collab_dev"
    scrape_table = f"{project_id}.{dataset_name}.scraped_raw_v3"
    bigquery_service = BigQueryService(scrape_table)

    # Classifiers storage configuration
    buckets_prefix = "dev"
    classifiers_bucket = "sambil_classifiers"
    gcs_classifiers_service = GCSService(bucket=classifiers_bucket, bucket_prefix=buckets_prefix)
    return GCPPersistencyManager(bigquery_service, gcs_classifiers_service)