#!/usr/bin/env python3
import uuid
from typing import Optional
from pydantic import BaseModel, Field

class Flight(BaseModel):
    id: str = Field(default_factory=uuid.uuid4, alias="_id")
    airline: str = Field(...)
    From: str = Field(...)
    to: str = Field(...)
    day: int = Field(...)
    month: str = Field(...)
    year: int = Field(...)
    age: int = Field(...)
    gender: str = Field(...)
    reason: str = Field(...)
    stay: str = Field(...)
    transit: str = Field(...)
    connection: str = Field(...)
    wait: int = Field(...)

    class Config:
        allow_population_by_field_name = True
        schema_extra = {
            "example": {
                "_id": "066de609-b04a-4b30-b46c-32537c7f1f6e",
                "airline": "Volaris",
                "From": "PDX",
                "to": "SJC",
                "day": 11,
                "month": "June",
                "year": 2022,
                "age": 30,
                "gender": "unspecified",
                "reason": "	Business/Work",
                "stay": "Hotel",
                "transit": "Airport cab",
                "connection": "False",
                "wait": 0
            }
        }


class Aggregation(BaseModel):
    _id: int = Field(...)
    total_times: int = Field(...)

    class Config:
        allow_population_by_field_name = True
        schema_extra = {
            "example": {
                "_id": 1,
                "total_times": 3
            }
        }