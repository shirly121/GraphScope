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
from graphscope.flex.rest.models.schema_mapping_loading_config_data_source import SchemaMappingLoadingConfigDataSource
from graphscope.flex.rest.models.schema_mapping_loading_config_format import SchemaMappingLoadingConfigFormat
try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

class SchemaMappingLoadingConfig(BaseModel):
    """
    SchemaMappingLoadingConfig
    """ # noqa: E501
    data_source: Optional[SchemaMappingLoadingConfigDataSource] = None
    import_option: Optional[StrictStr] = None
    format: Optional[SchemaMappingLoadingConfigFormat] = None
    __properties: ClassVar[List[str]] = ["data_source", "import_option", "format"]

    @field_validator('import_option')
    def import_option_validate_enum(cls, value):
        """Validates the enum"""
        if value is None:
            return value

        if value not in ('init', 'overwrite'):
            raise ValueError("must be one of enum values ('init', 'overwrite')")
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
        """Create an instance of SchemaMappingLoadingConfig from a JSON string"""
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
        # override the default output from pydantic by calling `to_dict()` of data_source
        if self.data_source:
            _dict['data_source'] = self.data_source.to_dict()
        # override the default output from pydantic by calling `to_dict()` of format
        if self.format:
            _dict['format'] = self.format.to_dict()
        return _dict

    @classmethod
    def from_dict(cls, obj: Dict) -> Self:
        """Create an instance of SchemaMappingLoadingConfig from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return cls.model_validate(obj)

        _obj = cls.model_validate({
            "data_source": SchemaMappingLoadingConfigDataSource.from_dict(obj.get("data_source")) if obj.get("data_source") is not None else None,
            "import_option": obj.get("import_option"),
            "format": SchemaMappingLoadingConfigFormat.from_dict(obj.get("format")) if obj.get("format") is not None else None
        })
        return _obj


