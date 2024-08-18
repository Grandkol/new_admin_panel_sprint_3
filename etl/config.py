from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    redis_host: str = Field("redis", env="REDIS_HOST")
    redis_port: int = Field(6379, env="REDIS_PORT")

    elastic_host: str = Field("http://elasticsearch:9200", env="ELASTIC_HOST")

    db_name: str = Field('theatre', env="POSTGRES_DB")
    db_user: str = Field('postgres', env="POSTGRES_USER")
    db_password: str = Field('secret', env="POSTGRES_PASSWORD")
    db_host: str = Field('theatre-db', env="SQL_HOST")
    db_port: int = Field(5432, env="SQL_PORT")


settings = Settings()
