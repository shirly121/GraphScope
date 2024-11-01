from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from gscoordinator.flex.models.base_model import Model
from gscoordinator.flex import util


class CreateStoredProcResponse(Model):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.
    """

    def __init__(self, stored_procedure_id=None):  # noqa: E501
        """CreateStoredProcResponse - a model defined in OpenAPI

        :param stored_procedure_id: The stored_procedure_id of this CreateStoredProcResponse.  # noqa: E501
        :type stored_procedure_id: str
        """
        self.openapi_types = {
            'stored_procedure_id': str
        }

        self.attribute_map = {
            'stored_procedure_id': 'stored_procedure_id'
        }

        self._stored_procedure_id = stored_procedure_id

    @classmethod
    def from_dict(cls, dikt) -> 'CreateStoredProcResponse':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The CreateStoredProcResponse of this CreateStoredProcResponse.  # noqa: E501
        :rtype: CreateStoredProcResponse
        """
        return util.deserialize_model(dikt, cls)

    @property
    def stored_procedure_id(self) -> str:
        """Gets the stored_procedure_id of this CreateStoredProcResponse.


        :return: The stored_procedure_id of this CreateStoredProcResponse.
        :rtype: str
        """
        return self._stored_procedure_id

    @stored_procedure_id.setter
    def stored_procedure_id(self, stored_procedure_id: str):
        """Sets the stored_procedure_id of this CreateStoredProcResponse.


        :param stored_procedure_id: The stored_procedure_id of this CreateStoredProcResponse.
        :type stored_procedure_id: str
        """
        if stored_procedure_id is None:
            raise ValueError("Invalid value for `stored_procedure_id`, must not be `None`")  # noqa: E501

        self._stored_procedure_id = stored_procedure_id
