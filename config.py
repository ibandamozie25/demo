
# config.py
import os
from pathlib import Path
import mysql.connector # pip install mysql-connector-python


class BaseConfig:
    # ---------------------
    # Security & Logging
    # ---------------------
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-change-me")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")




    # MySQL (Render)
    #MYSQL_HOST = os.getenv("MYSQL_HOST", "")
    #MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3307"))
    #MYSQL_DB = os.getenv("MYSQL_DB", "")
    #MYSQL_USER = os.getenv("MYSQL_USER", "")
    #MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")

    # MySQL (only)
    MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
    MYSQL_DB = os.getenv("MYSQL_DB", "school_manager")
    MYSQL_USER = os.getenv("MYSQL_USER", "root")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "Moses@24")

    # ---------------------
    # Receipt / School Info
    # ---------------------
    RECEIPT_CHARS = int(os.getenv("RECEIPT_CHARS", 48))
    SCHOOL_NAME = os.getenv("SCHOOL_NAME", "CITIZENS DAY AND BOARDING PRIMARY SCHOOL")
    SCHOOL_ADDRESS_LINE1 = os.getenv("SCHOOL_ADDRESS_LINE1", "P.O. Box 31882, Kampala")
    SCHOOL_TAGLINE = os.getenv("SCHOOL_TAGLINE", "Strive for the best")

    SCHOOL_NAME_LINE1 = os.getenv("SCHOOL_NAME_LINE1", "")
    SCHOOL_NAME_LINE2 = os.getenv("SCHOOL_NAME_LINE2", "")
    SCHOOL_POBOX_LINE = os.getenv("SCHOOL_POBOX_LINE", "")

    RECEIPT_PRINTER_NAME = os.getenv("RECEIPT_PRINTER_NAME", r"GP-80220(Cut) Series")
    RECEIPT_LOGO_PATH = os.getenv("RECEIPT_LOGO_PATH", "static/logo.jpg")
    RECEIPT_PAPER_DOTS = int(os.getenv("RECEIPT_PAPER_DOTS", 576))
    RECEIPT_LOGO_MAX_DOTS = int(os.getenv("RECEIPT_LOGO_MAX_DOTS", 200))

class DevConfig(BaseConfig):
    DEBUG = True


class ProdConfig(BaseConfig):
    DEBUG = False
    TESTING = False


class TestConfig(BaseConfig):
    TESTING = True
    LOG_LEVEL = "WARNING"


def _mysql_dsn(cfg: BaseConfig) -> dict:
    """Build mysql-connector-python kwargs."""
    missing = [k for k, v in {
        "MYSQL_HOST": cfg.MYSQL_HOST,
        "MYSQL_DB": cfg.MYSQL_DB,
        "MYSQL_USER": cfg.MYSQL_USER,
        "MYSQL_PASSWORD": cfg.MYSQL_PASSWORD,
    }.items() if not v]
    if missing:
        raise RuntimeError(f"Missing MySQL env vars: {', '.join(missing)}")

    return {
        "host": cfg.MYSQL_HOST,
        "port": cfg.MYSQL_PORT,
        "database": cfg.MYSQL_DB,
        "user": cfg.MYSQL_USER,
        "password": cfg.MYSQL_PASSWORD,
        "autocommit": False,
    }


def get_db_connection():
    """
    Return a MySQL connection (no SQLite fallback).
    """
    cfg = BaseConfig()
    return mysql.connector.connect(**_mysql_dsn(cfg))