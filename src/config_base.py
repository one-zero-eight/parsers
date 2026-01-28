__all__ = ["BaseParserConfig"]

from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, SecretStr, field_validator


class BaseParserConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_default=True)

    mount_point: Path = Path("output")
    "Mount point for output files"
    save_ics_path: Path
    "Path to directory to save .ics files relative to MOUNT_POINT"
    save_json_path: Path
    "Path to save .json file"
    innohassle_api_url: str | None = None
    "URL to InNoHassle API"
    parser_auth_key: SecretStr | None = None
    "Parser auth key"

    @field_validator("save_json_path", "save_ics_path", mode="before")
    @classmethod
    def relative_path(cls, v, values):
        "If not absolute path, then with respect to the main directory"
        v = Path(v)
        if not v.is_absolute():
            v = values.data["mount_point"] / v

        # if not children of mount point, then raise error
        if not v.is_relative_to(values.data["mount_point"]):
            raise ValueError(f"save_ics_path must be children of mount_point, but got {v}")
        return v

    @field_validator("save_json_path", mode="after")
    @classmethod
    def create_parent_dir(cls, v):
        "Create parent directory if not exists"
        v = Path(v)
        v.parent.mkdir(parents=True, exist_ok=True)
        return v

    @field_validator("save_ics_path", mode="after")
    @classmethod
    def create_dir(cls, v):
        "Create directory if not exists"
        v = Path(v)
        v.mkdir(parents=True, exist_ok=True)
        return v

    @field_validator("parser_auth_key", mode="before")
    @classmethod
    def parser_key_from_env(cls, v):
        "Get PARSER_AUTH_KEY from environment variable"
        if v is None:
            from os import environ

            v = environ.get("PARSER_AUTH_KEY")

        return v

    @field_validator(
        "innohassle_api_url",
        mode="before",
    )
    @classmethod
    def api_url_from_env(cls, v):
        "Get INNOHASSLE_API_URL from environment variable"
        if v is None:
            from os import environ

            v = environ.get("INNOHASSLE_API_URL")
        return v

    @classmethod
    def from_yaml(cls, path: Path):
        "Load config from yaml file"
        with open(path) as f:
            yaml_config = yaml.safe_load(f)
        return cls.model_validate(yaml_config)
