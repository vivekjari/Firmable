from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field


class LocationData(BaseModel):
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    country: Optional[str] = None
    region: Optional[str] = None
    continent: Optional[str] = None
    fuzzy_match: Optional[bool] = None


class ProductData(BaseModel):
    full_text: Optional[str] = None
    name: Optional[str] = None
    release_type: Optional[str] = None
    release_version: Optional[str] = None
    fuzzy_match: Optional[bool] = None


class NewsEventAttributes(BaseModel):
    summary: str
    category: str
    found_at: datetime
    confidence: float
    article_sentence: str
    human_approved: bool
    planning: bool

    amount: Optional[str] = None
    amount_normalized: Optional[int] = None
    assets: Optional[str] = None
    assets_tags: List[str] = Field(default_factory=list)
    award: Optional[str] = None
    contact: Optional[str] = None
    division: Optional[str] = None
    effective_date: Optional[date] = None
    event: Optional[str] = None
    financing_type: Optional[str] = None
    financing_type_normalized: Optional[str] = None
    financing_type_tags: List[str] = Field(default_factory=list)
    headcount: Optional[int] = None
    job_title: Optional[str] = None
    job_title_tags: List[str] = Field(default_factory=list)
    location: Optional[str] = None
    location_data: List[LocationData] = Field(default_factory=list)
    product: Optional[str] = None
    product_data: Optional[ProductData] = None
    product_tags: List[str] = Field(default_factory=list)
    recognition: Optional[str] = None
    vulnerability: Optional[str] = None


class ResourceIdentifier(BaseModel):
    id: str
    type: str


class RelationshipData(BaseModel):
    data: Optional[Union[ResourceIdentifier, List[ResourceIdentifier]]] = None


class NewsEventResource(BaseModel):
    id: str
    type: Literal["news_event"]
    attributes: NewsEventAttributes
    # Relationship keys vary (company1/company2/most_relevant_source/etc.).
    relationships: Dict[str, RelationshipData]


class CompanyAttributes(BaseModel):
    domain: Optional[str] = None
    company_name: Optional[str] = None
    ticker: Optional[str] = None


class NewsArticleAttributes(BaseModel):
    author: Optional[str] = None
    body: Optional[str] = None
    image_url: Optional[str] = None
    url: Optional[str] = None
    published_at: Optional[datetime] = None
    title: Optional[str] = None


class CompanyIncludedResource(BaseModel):
    id: str
    type: Literal["company"]
    attributes: CompanyAttributes


class NewsArticleIncludedResource(BaseModel):
    id: str
    type: Literal["news_article"]
    attributes: NewsArticleAttributes


IncludedResource = Union[CompanyIncludedResource, NewsArticleIncludedResource]


class NewsEventRecord(BaseModel):
    data: List[NewsEventResource]
    included: List[IncludedResource]
