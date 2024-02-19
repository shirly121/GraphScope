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
from pydantic import BaseModel, StrictStr, field_validator
from pydantic import Field
try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

class JobStatus(BaseModel):
    """
    JobStatus
    """ # noqa: E501
    job_id: Optional[StrictStr] = None
    type: Optional[StrictStr] = None
    status: Optional[StrictStr] = None
    start_time: Optional[StrictStr] = None
    end_time: Optional[StrictStr] = None
    log: Optional[StrictStr] = Field(default=None, description="URL or log string")
    detail: Optional[Dict[str, Any]] = None
    __properties: ClassVar[List[str]] = ["job_id", "type", "status", "start_time", "end_time", "log", "detail"]

    @field_validator('status')
    def status_validate_enum(cls, value):
        """Validates the enum"""
        if value is None:
            return value

        if value not in ('RUNNING', 'SUCCESS', 'FAILED', 'CANCELLED', 'WAITING'):
            raise ValueError("must be one of enum values ('RUNNING', 'SUCCESS', 'FAILED', 'CANCELLED', 'WAITING')")
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
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> Self:
        """Create an instance of JobStatus from a JSON string"""
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
        return _dict

    @classmethod
    def from_dict(cls, obj: Dict) -> Self:
        """Create an instance of JobStatus from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return cls.model_validate(obj)

        _obj = cls.model_validate({
            "job_id": obj.get("job_id"),
            "type": obj.get("type"),
            "status": obj.get("status"),
            "start_time": obj.get("start_time"),
            "end_time": obj.get("end_time"),
            "log": obj.get("log"),
            "detail": obj.get("detail")
        })
        return _obj


