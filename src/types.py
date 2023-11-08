from argparse import Namespace
from typing import Literal, Optional


ResumeOptions = Literal["name", "principal", "rating"]


class CommandArgs(Namespace):
    root: str
    download: Optional[bool]
    extract: Optional[bool]
    parse: Optional[bool]
    load: Optional[bool]
    dburi: str
    resume: Optional[ResumeOptions]
    debug: Optional[bool]
    quiet: Optional[bool]
