__all__ = [
    "InNoHassleEventsClient",
    "Output",
    "update_inh_event_groups",
    "ViewTag",
    "ViewEventGroup",
]

import logging
import pathlib
from typing import Optional, Any

import aiohttp
from pydantic import BaseModel, Field

from schedule.models import PredefinedEventGroup, PredefinedTag


class ViewTag(BaseModel):
    id: int
    alias: str
    type: Optional[str] = None
    name: Optional[str] = None
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


class InNoHassleEventsClient:
    def __init__(self, api_url: str, parser_auth_key: str):
        self.api_url = api_url
        self.parser_auth_key = parser_auth_key

    def session(self) -> aiohttp.ClientSession:
        return aiohttp.ClientSession(
            headers={"Authorization": f"Bearer {self.parser_auth_key}"}
        )

    async def get_event_groups(self) -> list[ViewEventGroup]:
        async with self.session() as s:
            async with s.get(f"{self.api_url}/event-groups/") as response:
                response.raise_for_status()
                groups_dict = await response.json()
                return [ViewEventGroup(**group) for group in groups_dict["groups"]]

    async def update_ics(self, event_group_id: int, ics_content: bytes) -> None:
        data = aiohttp.FormData()
        data.add_field("ics_file", ics_content, content_type="text/calendar")

        async with self.session() as s:
            async with s.put(
                f"{self.api_url}/event-groups/{event_group_id}/schedule.ics",
                data=data,
            ) as response:
                if response.status == 200:
                    logging.info(
                        f"ICS file for event group {event_group_id} is not modified"
                    )
                    return

                if response.status == 201:
                    logging.info(
                        f"ICS file for event group {event_group_id} updated successfully"
                    )
                    return

                if response.content:
                    logging.error(await response.json())

                raise Exception(f"Unexpected response status: {response.status}")


class Output(BaseModel):
    event_groups: list[PredefinedEventGroup]
    tags: list[PredefinedTag]
    meta: dict[str, Any] = Field(default_factory=dict)

    def __init__(
        self,
        event_groups: list[PredefinedEventGroup],
        tags: list[PredefinedTag],
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
) -> None:
    inh_event_groups = await inh_client.get_event_groups()
    inh_event_groups_dict = {group.alias: group for group in inh_event_groups}

    for event_group in output.event_groups:
        inh_event_group = inh_event_groups_dict.get(event_group.alias)

        if inh_event_group is None:
            logging.warning(f"Event group {event_group.alias} not found")
        else:
            logging.info(f"Updating event group {event_group.alias}")
            await inh_client.update_ics(
                event_group_id=inh_event_group.id,
                ics_content=(mount_point / event_group.path).read_bytes(),
            )
