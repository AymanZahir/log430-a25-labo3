"""
Database connections
SPDX - License - Identifier: LGPL - 3.0 - or -later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""

import mysql.connector
import redis
import config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def get_mysql_conn():
    """Get a MySQL connection using env variables"""
    return mysql.connector.connect(
        host=config.DB_HOST,
        port=config.DB_PORT,
        user=config.DB_USER,
        password=config.DB_PASS,
        database=config.DB_NAME
    )

def get_redis_conn():
    """Get a Redis connection using env variables"""
    return redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB, decode_responses=True)

def get_sqlalchemy_session():
    """Get an SQLAlchemy ORM session using env variables"""
    connection_string = f'mysql+mysqlconnector://{config.DB_USER}:{config.DB_PASS}@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}'
    engine = create_engine(connection_string, connect_args={'auth_plugin': 'caching_sha2_password'})
    Session = sessionmaker(bind=engine)
    return Session()