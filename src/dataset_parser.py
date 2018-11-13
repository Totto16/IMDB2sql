import csv
import os
import sqlite3
from collections import namedtuple
from os.path import join, getsize, exists
from typing import Iterator, List, Dict

from memory_profiler import profile
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import src.models as models
from src.utils import overwrite_upper_line, get_int, get_footprint, get_pretty_int
from src.constants import SKIP_CYCLES_NUM

SQLITE_TYPE = 'sqlite:///'


class DatasetParser:
    def __init__(self, root, resume, max_footprint: int, dataset_paths: List):
        self.engine = None
        self.metadata = None
        self.resume = resume
        if self.resume is not None:
            idx = [el[0] for el in dataset_paths].index(self.resume)
            self.dataset_paths = dataset_paths[idx:]
        else:
            self.dataset_paths = dataset_paths

        self.root = root
        self.max_footprint = max_footprint
        self.echo = False
        self.db_uri = None
        self.name_title: List[Dict] = []

    def db_init(self, db_uri: str):
        if db_uri.endswith('.db') or db_uri.startswith(SQLITE_TYPE):
            sqlite_path = db_uri.split('///')[1]
            if exists(sqlite_path) and self.resume is None:
                os.remove(sqlite_path)
            sqlite3.connect(db_uri.split('///')[1])

        self.db_uri = db_uri or self.db_uri

        self.engine = create_engine(f'{self.db_uri}', echo=self.echo)
        self.metadata = models.Base.metadata
        self.metadata.create_all(bind=self.engine)
        self.metadata.reflect(bind=self.engine)

    def parse_data_sets(self):
        for table_name, dataset_path in self.dataset_paths:
            status_line = f"Parsing {dataset_path} into '{table_name}' table ..."
            parse_handler = self._get_parse_handler(table_name)
            dataset = parse_handler(join(self.root, dataset_path))
            self._insert_dataset(dataset, status_line)

    def _get_parse_handler(self, table_name):
        return getattr(self, f'_parse_{table_name}')

    @profile
    def _insert_dataset(self, dataset_iter: Iterator, status_line: str):
        start_progress = 0
        buffer = []
        print(f'{status_line}: 0%')
        for idx, (line, progress) in enumerate(dataset_iter):
            buffer.append(line)
            if progress - start_progress > 0.01:
                start_progress += 0.01

                if idx % SKIP_CYCLES_NUM == 0:
                    overwrite_upper_line(self._get_status_line(status_line, progress))

                    if self._is_time_for_commit():
                        overwrite_upper_line(
                            f'{self._get_status_line(status_line, progress)} committing ...'
                        )

                        self._commit_all(buffer)
                        buffer = []

                        if self.name_title:
                            self.engine.execute(models.NameTitle.insert(), self.name_title)
                            session = self._get_session()
                            session.commit()
                            self.name_title = []

                        overwrite_upper_line(f'{status_line}: {progress :.2f}%')

        self._commit_all(buffer)
        overwrite_upper_line(f'{status_line}: 100% Done')

    @staticmethod
    def _get_status_line(status_line, progress):
        return f'{status_line}: {progress:.2f}%\tmemory footprint: {get_pretty_int(get_footprint())}'

    def _is_time_for_commit(self):
        return self.max_footprint < get_footprint()

    def _commit_all(self, buffer):
        session = self._get_session()
        session.add_all(buffer)
        session.commit()

    def _get_session(self):
        return sessionmaker(bind=self.engine)()

    def _parse_title(self, dataset_path):
        self.clean_table(models.Title)
        for data_set_class, progress in self._parse_dataset(dataset_path):
            title_line = models.Title(
                id=get_int(getattr(data_set_class, 'tconst')),
                titleType=getattr(data_set_class, 'titleType'),
                primaryTitle=getattr(data_set_class, 'primaryTitle'),
                originalTitle=getattr(data_set_class, 'originalTitle'),
                isAdult=bool(getattr(data_set_class, 'isAdult')),
                startYear=self._get_null(getattr(data_set_class, 'startYear')),
                endYear=self._get_null(getattr(data_set_class, 'endYear')),
                runtimeMinutes=self._get_null(getattr(data_set_class, 'runtimeMinutes')),
                genres=getattr(data_set_class, 'genres'),
            )
            yield title_line, progress

    def _parse_principals(self, dataset_path):
        self.clean_table(models.Principals)
        for data_set_class, progress in self._parse_dataset(dataset_path):
            principals_line = models.Principals(
                ordering=getattr(data_set_class, 'ordering'),
                category=getattr(data_set_class, 'category'),
                job=self._get_null(getattr(data_set_class, 'job')),
                characters=self._get_null(getattr(data_set_class, 'characters')),
                name_id=get_int(getattr(data_set_class, 'nconst')),
                title_id=get_int(getattr(data_set_class, 'tconst'))
            )
            yield principals_line, progress

    def _parse_ratings(self, dataset_path):
        self.clean_table(models.Ratings)
        for data_set_class, progress in self._parse_dataset(dataset_path):
            ratings_line = models.Ratings(
                averageRating=getattr(data_set_class, 'averageRating'),
                numVotes=getattr(data_set_class, 'numVotes'),
                title_id=get_int(getattr(data_set_class, 'tconst'))
            )
            yield ratings_line, progress

    @staticmethod
    def _parse_dataset(file_path):
        size = getsize(file_path)
        read_size = 0
        with open(file_path) as fd:
            tsv_reader = csv.reader(fd, delimiter='\t')
            data_set_class = namedtuple('_', next(tsv_reader))
            for line in tsv_reader:
                read_size += len(''.join(line)) + len(line)

                data = None
                try:
                    data = data_set_class(*line)
                except TypeError:
                    line = line + [None] * (len(data_set_class._fields) - len(line))
                    data = data_set_class(*line)
                finally:
                    yield data, (read_size / size) * 100

    def _parse_name(self, dataset_path):
        self.clean_table(models.Name)
        for data_set_class, progress in self._parse_dataset(dataset_path):
            name_id = get_int(getattr(data_set_class, 'nconst'))
            titles = [get_int(el) for el in getattr(data_set_class, 'knownForTitles').split(',') if get_int(el)]
            for title_id in titles:
                self.name_title.append({'nameId': name_id, 'titleId': title_id})

            name_line = models.Name(
                id=get_int(getattr(data_set_class, 'nconst')),
                primaryName=getattr(data_set_class, 'primaryName'),
                birthYear=self._get_null(getattr(data_set_class, 'birthYear')),
                deathYear=self._get_null(getattr(data_set_class, 'deathYear')),
                primaryProfession=getattr(data_set_class, 'primaryProfession'),
            )

            yield name_line, progress

    @staticmethod
    def _get_null(value):
        if value != '\\N':
            return value

    def clean_up(self):
        for table in reversed(self.metadata.sorted_tables):
            self.engine.execute(table.delete())

    def clean_table(self, model):
        session = self._get_session()
        session.query(model).delete()
        session.commit()
