from sqlalchemy import Column, Integer, String, Text, DateTime, Float, Boolean, PickleType
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class DetalleLoteModel(Base):

    
    """Data model detalle_lote_site."""
    __tablename__ = "detalle_lote_site"
    __table_args__ = {"schema": "c##meli1"}

    id_lote = Column(Integer,
                    primary_key=True,
                    nullable=False)
    site    = Column(String(3),
                    primary_key=True,
                    nullable=False)
    id_item = Column(Integer,
                    primary_key=True,
                    nullable=True)
    start_time = Column(DateTime,
                    nullable=False)
    price = Column(Float,
                    nullable=False)
    descripcion = Column(String(200),
                    nullable=True)
    nickname = Column(String(200),
                    nullable=False)     
    name = Column(String(200),
                    nullable=False)                 
    categoria = Column(String(200),
                    nullable=False)

    def __repr__(self):
        return '<DetalleLote model {}>'.format(self.id)