from pydantic import BaseModel
from uuid import UUID
from datetime import datetime
from typing import Optional


class SuggestedFirstMessage(BaseModel):
    uuid: UUID
    created_on: datetime
    message: str
    language_id: Optional[int]
    country_id: Optional[int]
    grade_id: Optional[int]
    subject_id: Optional[int]


class Country(BaseModel):
    id: int
    english_name: str


class Subject(BaseModel):
    id: int
    country_id: int
    long_name: str


class Grade(BaseModel):
    id: int
    country_id: int
    long_name: str


class Language(BaseModel):
    id: int
    english_name: str


class Combination(BaseModel):
    country_id: int
    grade_id: int
    subject_id: int