from typing import Optional

from pydantic import BaseModel, validator, Field

import re


def validate_slug(s):
    # only dashes and lowercase letters, digits
    if re.match(r"^[a-z-0-9]+$", s):
        return True
    # multiple dashes - not allowed
    if re.match(r"-{2,}", s):
        return False
    return False


class PredefinedTag(BaseModel):
    alias: str
    type: str
    name: str

    @validator("alias", "type")
    def validate_alias(cls, v):
        if not validate_slug(v):
            raise ValueError(f"Invalid slug '{v}'")
        return v

    @property
    def reference(self) -> "PredefinedEventGroup.TagReference":
        return PredefinedEventGroup.TagReference(alias=self.alias, type=self.type)


class PredefinedEventGroup(BaseModel):
    alias: str
    path: str
    name: str
    description: Optional[str] = None

    class TagReference(BaseModel):
        alias: str
        type: str

    tags: list[TagReference] = Field(default_factory=list)

    @validator("alias")
    def validate_alias(cls, v):
        if not validate_slug(v):
            raise ValueError(f"Invalid slug '{v}'")
        return v
