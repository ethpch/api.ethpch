from pydantic import BaseModel
from utils.database import Base


class BaseModel(BaseModel):
    class Config:
        orm_mode = True

    @classmethod
    def modify_single_instance(cls, obj):
        pass

    @staticmethod
    def ensure_pure_instance(obj):
        if isinstance(obj, Base):
            obj = obj._pure_instance
        return obj

    @classmethod
    def from_orm(cls, obj):
        if isinstance(obj, (list, tuple, set)):
            for i in range(len(obj)):
                obj[i] = cls.ensure_pure_instance(obj[i])
                cls.modify_single_instance(obj[i])
        else:
            obj = cls.ensure_pure_instance(obj)
            cls.modify_single_instance(obj)
        return super().from_orm(obj)
