#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from abc import ABC
from datetime import datetime, date, timedelta
from os.path import exists
from typing import Any, List, Mapping, Tuple, Optional, MutableMapping, Iterable
import base64

import backoff
import requests
from airbyte_cdk import AirbyteLogger
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from requests.auth import HTTPBasicAuth


class RobinAuthenticator:
    def __init__(self, username: str, password: str, gateway_url: str):
        self.access_token = None
        self.username = username
        self.password = password
        self.url = gateway_url

    def login(self) -> HTTPBasicAuth:
        auth = requests.auth.HTTPBasicAuth(self.username, self.password)
        return auth


# Basic full refresh stream
class RobinOdataStream(HttpStream):
    def __init__(
            self,
            gateway_url: str,
            client: RobinAuthenticator,
            start_date: str = None,
            end_date: str = None,
    ):
        self.gateway_url = gateway_url
        self.start_date = date.fromisoformat(start_date) or date.today()
        self.end_date = date.fromisoformat(end_date) if end_date else None
        self.client = client
        super().__init__(self.client.login())

    @property
    def url_base(self) -> str:
        return f"{self.gateway_url}"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        try:
            metadata = response.json()["odata.nextLink"]
            next_offset = metadata.split("$skip=", 1)[1]
            return {"$skip": next_offset}
        except:
            print("Not more than 1000 rows found")
            return None

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Optional[Mapping[str, Any]]:
        return next_page_token

    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=3)
    def should_retry(self, response: requests.Response) -> bool:
        # There is no refresh token, so users need to log in again when the token expires
        if response.status_code == 401:
            self._session.auth = self.client.login()
            response.request.headers["Authorization"] = f"{self.client.access_token}"
            # change the response status code to 571, so should_give_up in rate_limiting.py
            # does not evaluate to true
            response.status_code = 571
            return True
        return response.status_code == 429 or 500 <= response.status_code < 600

    def unnest(self, key: str, data: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        ROBIN loves to nest fields, but nested fields cannot be used in an
        incremental cursor. This method grabs the hash where the increment field
        is nested and puts it at the top level
        """
        nested = data.pop(key)
        return {**data, **nested}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        results = response.json().get("value")
        for result in results:
            yield result


# Basic incremental stream
class IncrementalRobinOdataStream(RobinOdataStream, ABC):
    cursor_field = "CreationDateTime"

    # Checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    @property
    def state_checkpoint_interval(self) -> int:
        # 1000 is the default page size
        return 1000

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        latest_cursor = latest_record.get(self.cursor_field) or ""
        current_cursor = current_stream_state.get(self.cursor_field) or ""
        return {self.cursor_field: max(current_cursor, latest_cursor)}

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {}
        latest_cursor = stream_state.get(self.cursor_field) or self.start_date
        if latest_cursor:
            params = {"$filter": f"{self.cursor_field} gt datetime'{latest_cursor}'"}
        if next_page_token:
            params = {**params, **next_page_token}
        return params


class Accounts(RobinOdataStream):
    primary_key = "UniqueId"
    use_cache = True

    def path(self, **kwargs) -> str:
        return "../accounts"


class Categories(RobinOdataStream):
    primary_key = "UniqueId"
    use_cache = True

    def path(self, **kwargs) -> str:
        return "../categories"


class Channels(RobinOdataStream):
    primary_key = "UniqueId"
    use_cache = True

    def path(self, **kwargs) -> str:
        return "../channels"


class ChannelAccounts(RobinOdataStream):
    primary_key = "UniqueId"
    use_cache = True

    def path(self, **kwargs) -> str:
        return "../channelaccounts"


class ConversationActions(IncrementalRobinOdataStream):
    primary_key = "UniqueId"

    def path(self, **kwargs) -> str:
        return "../conversationactions"

    def request_params(self, stream_slice: Optional[Mapping[str, Any]], **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs) or {}
        print(params)
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        results = response.json().get("value")
        for result in results:
            yield result


class ConversationTags(IncrementalRobinOdataStream):
    primary_key = "UniqueId"

    def path(self, **kwargs) -> str:
        return "../conversationtags"

    def request_params(self, stream_slice: Optional[Mapping[str, Any]], **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs) or {}
        print(params)
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        results = response.json().get("value")
        for result in results:
            yield result


class Conversations(IncrementalRobinOdataStream):
    primary_key = "UniqueId"

    def path(self, **kwargs) -> str:
        return "../conversations"

    def request_params(self, stream_slice: Optional[Mapping[str, Any]], **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs) or {}
        print(params)
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        results = response.json().get("value")
        for result in results:
            yield result


class DailyOpeningHours(RobinOdataStream):
    primary_key = "UniqueId"
    use_cache = True

    def path(self, **kwargs) -> str:
        return "../dailyopeninghours"


class Messages(IncrementalRobinOdataStream):
    primary_key = "UniqueId"

    def path(self, **kwargs) -> str:
        return "../messages"

    def request_params(self, stream_slice: Optional[Mapping[str, Any]], **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs) or {}
        print(params)
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        results = response.json().get("value")
        for result in results:
            yield result


class Participations(RobinOdataStream):
    primary_key = "UniqueId"
    use_cache = True

    def path(self, **kwargs) -> str:
        return "../participations"


class People(RobinOdataStream):
    primary_key = "UniqueId"
    use_cache = True

    def path(self, **kwargs) -> str:
        return "../people"


class Profiles(RobinOdataStream):
    primary_key = "UniqueId"
    use_cache = True

    def path(self, **kwargs) -> str:
        return "../profiles"


class SalesPerformances(RobinOdataStream):
    primary_key = "UniqueId"
    use_cache = True

    def path(self, **kwargs) -> str:
        return "../salesperformances"


class ServiceHoursTemplates(RobinOdataStream):
    primary_key = "UniqueId"
    use_cache = True

    def path(self, **kwargs) -> str:
        return "../servicehourstemplates"


class Tags(RobinOdataStream):
    primary_key = "UniqueId"
    use_cache = True

    def path(self, **kwargs) -> str:
        return "../tags"


class TagCategories(RobinOdataStream):
    primary_key = "UniqueId"
    use_cache = True

    def path(self, **kwargs) -> str:
        return "../tagcategories"


class Webstores(RobinOdataStream):
    primary_key = "UniqueId"
    use_cache = True

    def path(self, **kwargs) -> str:
        return "../webstores"


class WebstoreUsers(RobinOdataStream):
    primary_key = "UniqueId"
    use_cache = True

    def path(self, **kwargs) -> str:
        return "../webstoreusers"


class SourceRobinOdata(AbstractSource):
    @staticmethod
    def gateway_url() -> str:
        return f"https://api.robinhq.com/odata/accounts/"

    def check_connection(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        try:
            gateway_url = self.gateway_url()
            client = RobinAuthenticator(config["username"], config["password"], gateway_url)
            client.login()
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        gateway_url = self.gateway_url()
        client = RobinAuthenticator(config["username"], config["password"], gateway_url)
        kwargs = {
            "gateway_url": gateway_url,
            "client": client,
            "start_date": config.get("start_date"),
            "end_date": config.get("end_date")
        }
        return [
            Accounts(**kwargs),
            Categories(**kwargs),
            Channels(**kwargs),
            ChannelAccounts(**kwargs),
            ConversationActions(**kwargs),
            Conversations(**kwargs),
            ConversationTags(**kwargs),
            DailyOpeningHours(**kwargs),
            Messages(**kwargs),
            Participations(**kwargs),
            People(**kwargs),
            Profiles(**kwargs),
            SalesPerformances(**kwargs),
            ServiceHoursTemplates(**kwargs),
            Tags(**kwargs),
            TagCategories(**kwargs),
            Webstores(**kwargs),
            WebstoreUsers(**kwargs)
        ]
