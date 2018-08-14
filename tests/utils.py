import csv
import gzip
import http.server
import io
from collections import namedtuple

from src.models import Name, Title

__all__ = ['TestServer', 'TEST_HTTP_PORT', 'TEST_FILENAME', 'TEST_TVS_DATA', 'TEST_FILENAME_INVALID',
           'parse_names', 'parse_titles']


TEST_HTTP_PORT = 8333
DELIMITER = '\t'
TEST_FILENAME = 'valid.test.dataset.tsv.gz'
TEST_FILENAME_INVALID = 'invalid.test.dataset.tsv.zip'
TEST_TVS_DATA = r"""
nconst	primaryName	birthYear	deathYear	primaryProfession	knownForTitles
nm0000001	Fred Astaire	1899	1987	soundtrack,actor,miscellaneous	tt0072308,tt0050419,tt0045537,tt0043044
nm0000002	Lauren Bacall	1924	2014	actress,soundtrack	tt0037382,tt0117057,tt0071877,tt0038355
nm0000003	Brigitte Bardot	1934	\N	actress,soundtrack,producer	tt0054452,tt0049189,tt0059956,tt0057345
nm0000004	John Belushi	1949	1982	actor,writer,soundtrack	tt0078723,tt0077975,tt0072562,tt0080455
nm0000005	Ingmar Bergman	1918	2007	writer,director,actor	tt0060827,tt0050986,tt0050976,tt0083922
nm0000006	Ingrid Bergman	1915	1982	actress,soundtrack,producer	tt0038787,tt0034583,tt0071877,tt0038109
nm0000007	Humphrey Bogart	1899	1957	actor,soundtrack,producer	tt0037382,tt0033870,tt0043265,tt0034583
nm0000008	Marlon Brando	1924	2004	actor,soundtrack,director	tt0047296,tt0068646,tt0070849,tt0078788
nm0000009	Richard Burton	1925	1984	actor,producer,soundtrack	tt0059749,tt0087803,tt0061184,tt0057877
""".strip('\n')


class Handler(http.server.SimpleHTTPRequestHandler):

    def do_GET(self):
        if self.path in [f'/{TEST_FILENAME}', f'/{TEST_FILENAME_INVALID}']:
            self._handle_success()
        else:
            self.send_error(404)
        return

    def _handle_success(self):
        self.send_response(200)
        self.send_header('Content-Disposition', f'attachment; filename={self.path[1:]}')
        self.send_header('Content-type', 'application/x-gzip')
        self.end_headers()
        self.wfile.write(_generate_gzipped_tvs_file_stream())


def _generate_gzipped_tvs_file_stream():
    output = io.StringIO()
    writer = csv.writer(output, delimiter=DELIMITER)
    for line in TEST_TVS_DATA.split('\n'):
        if line.strip():
            writer.writerow(line.split(DELIMITER))
    output.seek(0)
    return gzip.compress(output.read().encode())


class TestServer(http.server.HTTPServer):
    def __init__(self):
        super().__init__(('', TEST_HTTP_PORT), Handler)


def parse_dataset(file_path):
    with open(file_path) as fd:
        tsv_reader = csv.reader(fd, delimiter='\t')
        data_set_class = namedtuple('_', next(tsv_reader))
        for line in tsv_reader:
            yield data_set_class(*line)


def parse_names(file_path, get_titles_callback):
    for data_set_class in parse_dataset(file_path):
        name_line = Name(
            id=getattr(data_set_class, 'nconst'),
            primaryName=getattr(data_set_class, 'primaryName'),
            birthYear=getattr(data_set_class, 'birthYear'),
            deathYear=_get_null(getattr(data_set_class, 'deathYear')),
            primaryProfession=getattr(data_set_class, 'primaryProfession'),
            titles=get_titles_callback(getattr(data_set_class, 'knownForTitles').split(','))
        )

        yield name_line


def parse_titles(file_path):
    for data_set_class in parse_dataset(file_path):
        title_line = Title(
            id=getattr(data_set_class, 'tconst'),
            titleType=getattr(data_set_class, 'titleType'),
            primaryTitle=getattr(data_set_class, 'primaryTitle'),
            originalTitle=getattr(data_set_class, 'originalTitle'),
            isAdult=bool(getattr(data_set_class, 'isAdult')),
            startYear=_get_null(getattr(data_set_class, 'startYear')),
            endYear=_get_null(getattr(data_set_class, 'endYear')),
            runtimeMinutes=getattr(data_set_class, 'runtimeMinutes'),
            genres=getattr(data_set_class, 'genres'),
        )
        yield title_line


def _get_null(value):
    if value != '\\N':
        return value


# TODO: Implement DAL class for parsing data_sets into database
