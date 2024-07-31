# coding: utf-8

"""
    GraphScope FLEX HTTP SERVICE API

    This is a specification for GraphScope FLEX HTTP service based on the OpenAPI 3.0 specification. You can find out more details about specification at [doc](https://swagger.io/specification/v3/).  Some useful links: - [GraphScope Repository](https://github.com/alibaba/GraphScope) - [The Source API definition for GraphScope Interactive](https://github.com/GraphScope/portal/tree/main/httpservice)

    The version of the OpenAPI document: 1.0.0
    Contact: graphscope@alibaba-inc.com
    Generated by OpenAPI Generator (https://openapi-generator.tech)

    Do not edit the class manually.
"""  # noqa: E501


from __future__ import annotations
import pprint
import re  # noqa: F401
import json

from pydantic import BaseModel, StrictStr
from typing import Any, ClassVar, Dict, List, Optional
from graphscope.flex.rest.models.column_mapping import ColumnMapping
from typing import Optional, Set
from typing_extensions import Self

class VertexMapping(BaseModel):
    """
    VertexMapping
    """ # noqa: E501
    type_name: StrictStr
    inputs: List[StrictStr]
    column_mappings: Optional[List[ColumnMapping]] = None
    __properties: ClassVar[List[str]] = ["type_name", "inputs", "column_mappings"]

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
    def from_json(cls, json_str: str) -> Optional[Self]:
        """Create an instance of VertexMapping from a JSON string"""
        return cls.from_dict(json.loads(json_str))

    def to_dict(self) -> Dict[str, Any]:
        """Return the dictionary representation of the model using alias.

        This has the following differences from calling pydantic's
        `self.model_dump(by_alias=True)`:

        * `None` is only added to the output dict for nullable fields that
          were set at model initialization. Other fields with value `None`
          are ignored.
        """
        excluded_fields: Set[str] = set([
        ])

        _dict = self.model_dump(
            by_alias=True,
            exclude=excluded_fields,
            exclude_none=True,
        )
        # override the default output from pydantic by calling `to_dict()` of each item in column_mappings (list)
        _items = []
        if self.column_mappings:
            for _item in self.column_mappings:
                if _item:
                    _items.append(_item.to_dict())
            _dict['column_mappings'] = _items
        return _dict

    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        """Create an instance of VertexMapping from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return cls.model_validate(obj)

        _obj = cls.model_validate({
            "type_name": obj.get("type_name"),
            "inputs": obj.get("inputs"),
            "column_mappings": [ColumnMapping.from_dict(_item) for _item in obj["column_mappings"]] if obj.get("column_mappings") is not None else None
        })
        return _obj


