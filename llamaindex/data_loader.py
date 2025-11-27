# custom_universal_reader.py

import asyncio
from pathlib import Path
import sqlite3
import pandas as pd

from typing import List

from llama_index.core import Document, SimpleDirectoryReader
from llama_index.core.readers.base import BaseReader

from llama_index.readers.web import SimpleWebPageReader
from llama_index.readers.file.tabular import CSVReader
from llama_index.readers.file.html import HTMLTagReader
from llama_index.readers.web import SimpleWebPageReader
from llama_index.readers.file.pymu_pdf import PyMuPDFReader
from llama_index.readers.file.docs import DocxReader


# ---------------------------------------------------------------------
# Helper: Detect File Type
# ---------------------------------------------------------------------
def detect_file(path: Path) -> str:
    ext = path.suffix.lower()

    if ext == ".pdf":
        return "pdf"
    if ext in {".txt", ".md"}:
        return "text"
    if ext == ".csv":
        return "csv"
    if ext in {".htm", ".html"}:
        return "html"
    if ext in {".db", ".sqlite"}:
        return "database"
    if ext == ".urls" or (ext == ".txt" and "web" in str(path.parent)):
        return "url_list"

    return "unknown"


# ---------------------------------------------------------------------
# Custom Universal Async Reader
# ---------------------------------------------------------------------
class AsyncUniversalReader(BaseReader):
    """
    A universal asynchronous reader that:
    - walks directories recursively
    - identifies file types
    - loads using LlamaIndex-native readers
    - fetches webpages (TXT file containing URLs)
    """

    def __init__(self):
        self.csv_loader = CSVReader()
        self.html_loader = HTMLTagReader()
        self.web_loader = SimpleWebPageReader()
        self.pdf_reader = PyMuPDFReader()
        self.collected = []
        # self.txt_reader = SimpleDirectoryReader()

    # Required by LlamaIndex
    async def aload_data(self, input_path: str) -> List[Document]:
        base = Path(input_path)
        docs = await self._scan(base)
        return docs

    # -----------------------------------------------------------------
    # Recursive Directory Walker
    # -----------------------------------------------------------------
    async def _scan(self, path: Path) -> List[Document]:
        # collected = []

        for item in path.iterdir():
            if item.is_dir():
                self.collected.extend(await self._scan(item))
                continue

            ftype = detect_file(item)

            match ftype:

                case "pdf" | "text":
                    # SimpleDirectoryReader handles both .txt & .pdf
                    docs = SimpleDirectoryReader(input_files=[str(item)]).load_data()
                    self.collected.extend(docs)

                case "csv":
                    docs = self.csv_loader.load_data(file=item)
                    self.collected.extend(docs)

                case "html":
                    docs = self.html_loader.load_data(input_file=item)
                    self.collected.extend(docs)

                case "database":
                    docs = self._load_db(item)
                    self.collected.extend(docs)

                case "url_list":
                    docs = await self._load_urls(item)
                    self.collected.extend(docs)

        return self.collected

    # -----------------------------------------------------------------
    # Database Loader (SQLite → Document)
    # -----------------------------------------------------------------
    def _load_db(self, db_path: Path) -> List[Document]:
        docs = []
        conn = sqlite3.connect(db_path)

        tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)

        for table in tables["name"]:
            df = pd.read_sql(f"SELECT * FROM {table}", conn)
            text = df.to_markdown(index=False)

            docs.append(
                Document(
                    text=text,
                    metadata={
                        "source": str(db_path),
                        "table": table,
                        "rows": len(df),
                        "source_type": "database",
                    },
                )
            )

        conn.close()
        return docs

    # -----------------------------------------------------------------
    # URL Loader (TXT containing URLs)
    # -----------------------------------------------------------------
    async def _load_urls(self, url_file: Path) -> List[Document]:
        urls = []
        with open(url_file, "r") as f:
            for line in f:
                if line.strip().startswith("http"):
                    urls.append(line.strip())

        if not urls:
            return []

        docs = self.web_loader.load_data(urls=urls)

        for d in docs:
            d.metadata["source_type"] = "web_url"

        return docs
