from sqlalchemy import Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Backend(Base):
    __tablename__ = "backends"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(30))
    token: Mapped[str] = mapped_column(String(100))


class Agent(Base):
    __tablename__ = "agents"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    backend_id: Mapped[int] = mapped_column(Integer())
    name: Mapped[str] = mapped_column(String(30))
    token: Mapped[str] = mapped_column(String(100))


