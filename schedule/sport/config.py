__all__ = ['sports_config', 'SportsParserConfig']

from pathlib import Path

import requests
from pydantic import BaseModel, validator, SecretStr

from schedule.utils import get_project_root

PROJECT_ROOT = get_project_root()

CONFIG_PATH = Path(__file__).parent / "config.json"


class Credentials(BaseModel):
    email: str
    password: SecretStr


class Token(BaseModel):
    token: SecretStr


class SportsParserConfig(BaseModel):
    website_url: str = "https://sport.innopolis.university"
    api_url: str = "https://sport.innopolis.university/api"
    credentials_path: Path = CONFIG_PATH.parent / "credentials.json"
    token_path: Path = CONFIG_PATH.parent / "token.json"

    @validator('credentials_path', pre=True)
    def credentials_path_validator(cls, v):
        if not isinstance(v, Path):
            v = Path(v)
        if not v.is_absolute():
            v = PROJECT_ROOT / v
        return v

    @validator('token_path', pre=True)
    def token_path_validator(cls, v):
        if not isinstance(v, Path):
            v = Path(v)
        if not v.is_absolute():
            v = PROJECT_ROOT / v
        return v

    @property
    def credentials(self) -> Credentials | None:
        """
        Credentials for the API

        :return: credentials for the API
        :rtype: Credentials
        """
        try:
            return Credentials.parse_file(self.credentials_path)
        except ValueError:
            return None

    @property
    def token(self) -> Token | None:
        """
        Token for the API

        :return: token for the API
        :rtype: Token
        """
        try:
            return Token.parse_file(self.token_path)
        except FileNotFoundError:
            # get from credentials
            creds = self.credentials
            login_url = f'{self.website_url}/oauth2/login'
            token = get_token(creds.email, creds.password.get_secret_value(), login_url)
            token = Token(token=SecretStr(token))
            with open(self.token_path, 'w') as f:
                f.write(token.json())
            return token

    class Config:
        validate_assignment = True


def get_token(email: str, password: str, login_url: str) -> str:
    s = requests.Session()
    res = s.get(login_url)

    if res.status_code != 200:
        raise ConnectionError('Server is down')

    oauth_url = res.url

    res = s.post(oauth_url, data={
        'UserName': email,
        'Password': password,
        'AuthMethod': 'FormsAuthentication',
        'Kmsi': True,
    }, allow_redirects=True)

    if res.status_code != 200:
        raise RuntimeError('Authentication failed')

    # get csrftoken from cookies
    csrftoken = s.cookies['csrftoken']

    return csrftoken


sports_config: SportsParserConfig = SportsParserConfig.parse_file(CONFIG_PATH)
