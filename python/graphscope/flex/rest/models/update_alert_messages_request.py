# coding: utf-8

"""
    GraphScope FLEX HTTP SERVICE API

    This is a specification for GraphScope FLEX HTTP service based on the OpenAPI 3.0 specification. You can find out more details about specification at [doc](https://swagger.io/specification/v3/).  Some useful links: - [GraphScope Repository](https://github.com/alibaba/GraphScope) - [The Source API definition for GraphScope Interactive](https://github.com/GraphScope/portal/tree/main/httpservice)

    The version of the OpenAPI document: 0.9.1
    Contact: graphscope@alibaba-inc.com
    Generated by OpenAPI Generator (https://openapi-generator.tech)

    Do not edit the class manually.
"""  # noqa: E501


from __future__ import annotations
import pprint
import re  # noqa: F401
import json


from typing import Any, ClassVar, Dict, List, Optional
from pydantic import BaseModel, StrictBool, StrictStr, field_validator
from pydantic import Field
from graphscope.flex.rest.models.alert_message import AlertMessage
try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

class UpdateAlertMessagesRequest(BaseModel):
    """
    UpdateAlertMessagesRequest
    """ # noqa: E501
    messages: Optional[List[AlertMessage]] = None
    batch_status: Optional[StrictStr] = Field(default=None, description="Override the status of each message")
    batch_delete: Optional[StrictBool] = Field(default=False, description="True will delete all the messages in request body")
    __properties: ClassVar[List[str]] = ["messages", "batch_status", "batch_delete"]

    @field_validator('batch_status')
    def batch_status_validate_enum(cls, value):
        """Validates the enum"""
        if value is None:
            return value

        if value not in ('solved', 'unsolved', 'dealing'):
            raise ValueError("must be one of enum values ('solved', 'unsolved', 'dealing')")
        return value

    model_config = {
        "populate_by_name": True,
        "validate_assignment": True,
        "protected_namespaces": (),
    }


    def to_str(self) -> str:
        """Returns the string representation of the model using alias"""
        return pprint.pformat(self.model_dump(by_alias=True))

    def to_json(self) -> str:
        """Returns the JSON representation of the model using alias"""
        # TODO: pydantic v2: use .model_dump_json(by_alias=True, exclude_unset=True) instead
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> Self:
        """Create an instance of UpdateAlertMessagesRequest from a JSON string"""
        return cls.from_dict(json.loads(json_str))

    def to_dict(self) -> Dict[str, Any]:
        """Return the dictionary representation of the model using alias.

        This has the following differences from calling pydantic's
        `self.model_dump(by_alias=True)`:

        * `None` is only added to the output dict for nullable fields that
          were set at model initialization. Other fields with value `None`
          are ignored.
        """
        _dict = self.model_dump(
            by_alias=True,
            exclude={
            },
            exclude_none=True,
        )
        # override the default output from pydantic by calling `to_dict()` of each item in messages (list)
        _items = []
        if self.messages:
            for _item in self.messages:
                if _item:
                    _items.append(_item.to_dict())
            _dict['messages'] = _items
        return _dict

    @classmethod
    def from_dict(cls, obj: Dict) -> Self:
        """Create an instance of UpdateAlertMessagesRequest from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return cls.model_validate(obj)

        _obj = cls.model_validate({
            "messages": [AlertMessage.from_dict(_item) for _item in obj.get("messages")] if obj.get("messages") is not None else None,
            "batch_status": obj.get("batch_status"),
            "batch_delete": obj.get("batch_delete") if obj.get("batch_delete") is not None else False
        })
        return _obj


