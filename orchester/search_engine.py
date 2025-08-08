# orchester/search_engine.py
import pandas as pd
import sqlite3
import faiss
import numpy as np
import os
from typing import List, Dict, Optional

from sentence_transformers import SentenceTransformer

class XLSXSearchEngine:
    """
    Мигрируем весь xlsx в sqlite, но в векторный индекс кладём только нужные поля:
    по умолчанию: 'УИД', 'Номенклатура', 'Наименование полное', 'Единица измерения'
    """
    def __init__(self, xlsx_path: str, db_path: str = "data.db", index_path: str = "faiss.index", embedding_model: str = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"):
        self.xlsx_path = xlsx_path
        self.db_path = db_path
        self.index_path = index_path
        self.embedding_model_name = embedding_model
        self.model = SentenceTransformer(self.embedding_model_name)

        # Keep these exact column names in sqlite table (we will attempt to read them)
        self.index_fields = ["УИД", "Номенклатура", "Наименование полное", "Единица измерения"]

        self.df = None
        self.index = None

        # If db missing => migrate whole xlsx to sqlite
        if not os.path.exists(self.db_path):
            self._xlsx_to_sqlite()

        # Build or load FAISS index
        if not os.path.exists(self.index_path):
            self._build_faiss_index()
        else:
            self._load_index()

    def _xlsx_to_sqlite(self):
        df = pd.read_excel(self.xlsx_path)
        # save whole sheet to sqlite
        conn = sqlite3.connect(self.db_path)
        df.to_sql(name="records", con=conn, index=False, if_exists="replace")
        conn.close()

    def _build_faiss_index(self):
        # read only needed fields for indexing (if exist)
        conn = sqlite3.connect(self.db_path)
        # try to select columns; if some columns missing, adjust
        table_df = pd.read_sql_query("SELECT * FROM records LIMIT 1", conn)
        available = [c for c in self.index_fields if c in table_df.columns]
        if not available or ("Номенклатура" not in available and "Наименование полное" not in available):
            raise RuntimeError("Не найдено полей для индексации (Номенклатура / Наименование полное).")
        # read all rows but only available index columns + УИД if present
        cols_to_read = [c for c in ["УИД", "Номенклатура", "Наименование полное", "Единица измерения"] if c in table_df.columns]
        self.df = pd.read_sql_query(f"SELECT {', '.join(cols_to_read)} FROM records", conn)
        conn.close()

        parts = []
        if "Номенклатура" in self.df.columns:
            parts.append(self.df["Номенклатура"].astype(str))
        if "Наименование полное" in self.df.columns:
            parts.append(self.df["Наименование полное"].astype(str))
        texts = (parts[0] if len(parts) == 1 else (parts[0] + " " + parts[1])).tolist()

        embeddings = self.model.encode(texts, show_progress_bar=True, convert_to_numpy=True)
        embeddings = np.array(embeddings).astype("float32")
        dim = embeddings.shape[1]
        index = faiss.IndexFlatL2(dim)
        index.add(embeddings)
        faiss.write_index(index, self.index_path)
        self.index = index

    def _load_index(self):
        self.index = faiss.read_index(self.index_path)
        conn = sqlite3.connect(self.db_path)
        self.df = pd.read_sql_query("SELECT * FROM records", conn)
        conn.close()

    def search(self, query: str, top_k: int = 5) -> List[Dict]:
        if self.index is None:
            raise RuntimeError("FAISS index not loaded")
        q_emb = self.model.encode([query], convert_to_numpy=True)[0].astype("float32")
        D, I = self.index.search(np.array([q_emb]), top_k)
        indices = I[0]
        results = []
        for idx in indices:
            if idx < 0 or idx >= len(self.df):
                continue
            row = self.df.iloc[idx].to_dict()
            results.append(row)
        return results
