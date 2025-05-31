__all__ = [
    "InNoHassleEventsClient",
    "Output",
    "update_inh_event_groups",
    "ViewTag",
    "ViewEventGroup",
    "CreateTag",
    "CreateEventGroup",
]

import asyncio
import datetime
import json
import pathlib
import re
import warnings
from functools import partial
from typing import Any, Optional

import aiohttp
from pydantic import BaseModel, Field, validator

from src.logging_ import logger


class CreateTag(BaseModel):
    alias: str
    type: str
    name: str

    @validator("alias", "type")
    def validate_alias(cls, v):
        if not validate_slug(v):
            raise ValueError(f"Invalid slug '{v}'")
        return v


class CreateEventGroup(BaseModel):
    alias: str
    path: str
    name: str
    description: Optional[str] = None

    tags: list[CreateTag] = Field(default_factory=list)

    @validator("alias")
    def validate_alias(cls, v):
        if not validate_slug(v):
            raise ValueError(f"Invalid slug '{v}'")
        return v


class ViewTag(BaseModel):
    id: int
    alias: str
    type: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    satellite: Optional[dict] = None


class ViewEventGroup(BaseModel):
    """
    Represents a group instance from the database excluding sensitive information.
    """

    id: int
    alias: str
    path: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    tags: list["ViewTag"] = Field(default_factory=list)


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


class InNoHassleEventsClient:
    def __init__(self, api_url: str, parser_auth_key: str):
        self.api_url = api_url
        self.parser_auth_key = parser_auth_key

    def session(self) -> aiohttp.ClientSession:
        return aiohttp.ClientSession(
            headers={"Authorization": f"Bearer {self.parser_auth_key}"},
            json_serialize=partial(json.dumps, default=json_serial),
        )

    async def get_event_groups(self) -> list[ViewEventGroup]:
        async with self.session() as s:
            async with s.get(f"{self.api_url}/event-groups/") as response:
                response.raise_for_status()
                groups_dict = await response.json()
                return [ViewEventGroup(**group) for group in groups_dict["event_groups"]]

    async def batch_create_or_read_event_groups(self, event_groups: list[CreateEventGroup]) -> list[ViewEventGroup]:
        if not event_groups:
            return []
        data = {"event_groups": [group.dict() for group in event_groups]}

        async with self.session() as s:
            async with s.post(f"{self.api_url}/event-groups/batch-create-or-read", json=data) as response:
                response.raise_for_status()
                groups_dict = await response.json()
                return [ViewEventGroup(**group) for group in groups_dict["event_groups"]]

    async def update_ics(self, event_group_id: int, ics_content: bytes) -> int:
        data = aiohttp.FormData()
        data.add_field("ics_file", ics_content, content_type="text/calendar")

        async with self.session() as s:
            async with s.put(
                f"{self.api_url}/event-groups/{event_group_id}/schedule.ics",
                data=data,
            ) as response:
                if response.status == 200:
                    logger.debug(f"ICS file for event group {event_group_id} is not modified")
                    return 200

                if response.status == 201:
                    logger.debug(f"ICS file for event group {event_group_id} updated successfully")
                    return 201

                if response.content:
                    logger.error(await response.text())

                raise Exception(f"Unexpected response status: {response.status} for event group {event_group_id}")


class Output(BaseModel):
    event_groups: list[CreateEventGroup]
    tags: list[CreateTag]
    meta: dict[str, Any] = Field(default_factory=dict)

    def __init__(
        self,
        event_groups: list[CreateEventGroup],
        tags: list[CreateTag],
    ):
        # only unique (alias, type) tags
        visited = set()

        visited_tags = []

        for tag in tags:
            if (tag.alias, tag.type) not in visited:
                visited.add((tag.alias, tag.type))
                visited_tags.append(tag)

        # sort tags
        visited_tags = sorted(visited_tags, key=lambda x: (x.type, x.alias))

        super().__init__(event_groups=event_groups, tags=visited_tags)

        self.meta = {
            "event_groups_count": len(self.event_groups),
            "tags_count": len(self.tags),
        }


async def update_inh_event_groups(
    inh_client: InNoHassleEventsClient,
    mount_point: pathlib.Path,
    output: Output,
) -> dict:
    logger.info(f"Trying to create or read {len(output.event_groups)} event groups")
    inh_event_groups = await inh_client.batch_create_or_read_event_groups(output.event_groups)
    inh_event_groups_dict = {group.alias: group for group in inh_event_groups}
    updated = []
    same = []

    async def task(event_group):
        inh_event_group = inh_event_groups_dict.get(event_group.alias)

        if inh_event_group is None:
            warnings.warn(f"Event group `{event_group.alias}` not found")
        else:
            code = await inh_client.update_ics(
                event_group_id=inh_event_group.id,
                ics_content=(mount_point / event_group.path).read_bytes(),
            )
            if code == 200:
                logger.info(f"ICS is not modified for {event_group.alias} <{inh_event_group.id}>")
                same.append(inh_event_group.alias)
            elif code == 201:
                logger.info(f"ICS is modified for {event_group.alias} <{inh_event_group.id}>")
                updated.append(inh_event_group.alias)

    tasks = [asyncio.create_task(task(event_group)) for event_group in output.event_groups]
    await asyncio.gather(*tasks)

    return {"updated": updated, "same": same}


def validate_slug(s):
    # only dashes and lowercase letters, digits
    if re.match(r"^[a-z-0-9а-яА-ЯёЁ]+$", s):
        return True
    # multiple dashes - not allowed
    if re.match(r"-{2,}", s):
        return False
    return False
