from dataclasses import dataclass
import re
import sys
import tempfile
import urllib.parse
from pathlib import Path
from typing import Any, Literal, Optional, TypedDict, Union, cast
from bs4 import ResultSet

import yaml

DATA_SET_FILENAME_PATTERN = re.compile("^/(.*).gz")
CURSOR_UP_ONE = "\x1b[1A"
ERASE_LINE = "\x1b[2K"


class DataSetPaths(TypedDict):
    film: Optional[str]
    person: Optional[str]
    principal: Optional[str]
    alternative: Optional[str]
    rating: Optional[str]
    crew: Optional[str]
    episode: Optional[str]

DataSetKeys = Literal["film","person","principal","alternative","rating","crew","episode"]


class Config(TypedDict):
    data_sets_url: str
    dataset_file_ext: str
    dataset_paths: DataSetPaths
    dataset_delimiter: str
    default_database_uri: str
    csv_extension: str
    film_filter: list[str]


def get_config(config_path: Path) -> Config:
    with open(config_path) as cfg:
        return cast(Config, yaml.load(cfg, Loader=yaml.FullLoader))


def get_links(dataset_index_page_content: str, config: Config) -> list[str]:
    from bs4 import BeautifulSoup

    bs_obj = BeautifulSoup(dataset_index_page_content, "html.parser")
    return [
        link.get("href") for link in bs_obj.find_all("a") if _filter_links(link, config)
    ]


def _filter_links(link: ResultSet[Any], config: Config) -> bool:
    dataset_files = [
        f"{el}.{config['dataset_file_ext']}" for el in config["dataset_paths"].values()
    ]
    return urllib.parse.urlparse(link.get("href")).path.strip("/") in dataset_files


@dataclass
class DataSet:
    url: str
    gzipped: Path
    extracted: Path


def get_data_sets(
    urls: list[str], root: Path = Path(tempfile.gettempdir())
) -> list[DataSet]:
    _ret_val: list[DataSet] = []
    for url in urls:
        path = urllib.parse.urlparse(url).path
        if file_path_re := DATA_SET_FILENAME_PATTERN.search(path):
            extracted = root / file_path_re.group(1)
        else:
            raise ValueError("Data set filename doesn't match")
        gzipped = root / path.lstrip("/")

        _ret_val.append(DataSet(gzipped=gzipped, extracted=extracted, url=url))
    return _ret_val


def overwrite_upper_line(content: str, quiet: bool = False) -> None:
    """
    Output string content in the current line by overwriting
    :param content: string content
    :param quiet: bool
    :return:
    """
    if not quiet:
        sys.stdout.write(CURSOR_UP_ONE)
        sys.stdout.write(ERASE_LINE)
        print(content)


def get_int(id_: str) -> Union[int, None]:
    """
    Convert string id like tt0000002 to integer 2
    :param id_: string id
    :return: integer id
    """
    try:
        return int(id_[2:])
    except ValueError:
        return None


def get_null(value: str) -> str:
    if value.strip() not in ["\\N", ""]:
        return value
    return "0"
