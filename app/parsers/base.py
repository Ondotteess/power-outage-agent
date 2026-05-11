from abc import ABC, abstractmethod
from uuid import UUID

from app.models.schemas import RawRecordSchema


class BaseCollector(ABC):
    @abstractmethod
    async def fetch(self, url: str, trace_id: UUID, verify_ssl: bool = True) -> RawRecordSchema: ...
