from sqlalchemy import Column, Integer, String
from utils.database import Base


class Shorturl(Base):
    __tablename__ = 'short_urls'
    id = Column(Integer, primary_key=True, autoincrement=True, default=100000)
    source = Column(String(65536))
