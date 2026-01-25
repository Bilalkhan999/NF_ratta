from __future__ import annotations

import os
import socket
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

IS_VERCEL = os.getenv("VERCEL") is not None

DEFAULT_SQLITE_URL = "sqlite:////tmp/data.sqlite3" if IS_VERCEL else "sqlite:///./data.sqlite3"
DB_URL = os.getenv("DATABASE_URL", DEFAULT_SQLITE_URL).strip()

if DB_URL.startswith("postgres://"):
    DB_URL = DB_URL.replace("postgres://", "postgresql://", 1)

IS_SQLITE = DB_URL.startswith("sqlite")

if (DB_URL.startswith("postgresql://") or DB_URL.startswith("postgresql+")) and "supabase.co" in DB_URL and "sslmode=" not in DB_URL:
    parts = urlsplit(DB_URL)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    q.setdefault("sslmode", "require")
    parts = parts._replace(query=urlencode(q))
    DB_URL = urlunsplit(parts)

if DB_URL.startswith("postgresql://"):
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)

engine_kwargs: dict = {"pool_pre_ping": True}
if IS_SQLITE:
    engine_kwargs["connect_args"] = {"check_same_thread": False}
elif IS_VERCEL and "supabase.co" in DB_URL:
    try:
        parts = urlsplit(DB_URL)
        hostname = parts.hostname
        if hostname:
            infos = socket.getaddrinfo(hostname, parts.port or 5432, family=socket.AF_INET, type=socket.SOCK_STREAM)
            if infos:
                ipv4 = infos[0][4][0]
                engine_kwargs["connect_args"] = {"hostaddr": ipv4}
    except Exception:
        pass

engine = create_engine(DB_URL, **engine_kwargs)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()
