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
from graphscope.flex.rest.models.procedure_params_inner import ProcedureParamsInner
try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

class Procedure(BaseModel):
    """
    Procedure
    """ # noqa: E501
    name: Optional[StrictStr] = None
    bound_graph: Optional[StrictStr] = None
    description: Optional[StrictStr] = None
    type: Optional[StrictStr] = None
    query: Optional[StrictStr] = None
    enable: Optional[StrictBool] = None
    runnable: Optional[StrictBool] = None
    params: Optional[List[ProcedureParamsInner]] = None
    returns: Optional[List[ProcedureParamsInner]] = None
    __properties: ClassVar[List[str]] = ["name", "bound_graph", "description", "type", "query", "enable", "runnable", "params", "returns"]

    @field_validator('type')
    def type_validate_enum(cls, value):
        """Validates the enum"""
        if value is None:
            return value

        if value not in ('cpp', 'cypher'):
            raise ValueError("must be one of enum values ('cpp', 'cypher')")
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
        """Create an instance of Procedure from a JSON string"""
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
        # override the default output from pydantic by calling `to_dict()` of each item in params (list)
        _items = []
        if self.params:
            for _item in self.params:
                if _item:
                    _items.append(_item.to_dict())
            _dict['params'] = _items
        # override the default output from pydantic by calling `to_dict()` of each item in returns (list)
        _items = []
        if self.returns:
            for _item in self.returns:
                if _item:
                    _items.append(_item.to_dict())
            _dict['returns'] = _items
        return _dict

    @classmethod
    def from_dict(cls, obj: Dict) -> Self:
        """Create an instance of Procedure from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return cls.model_validate(obj)

        _obj = cls.model_validate({
            "name": obj.get("name"),
            "bound_graph": obj.get("bound_graph"),
            "description": obj.get("description"),
            "type": obj.get("type"),
            "query": obj.get("query"),
            "enable": obj.get("enable"),
            "runnable": obj.get("runnable"),
            "params": [ProcedureParamsInner.from_dict(_item) for _item in obj.get("params")] if obj.get("params") is not None else None,
            "returns": [ProcedureParamsInner.from_dict(_item) for _item in obj.get("returns")] if obj.get("returns") is not None else None
        })
        return _obj


