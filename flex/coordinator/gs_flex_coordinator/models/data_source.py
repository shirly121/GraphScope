from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from gs_flex_coordinator.models.base_model import Model
from gs_flex_coordinator.models.edge_data_source import EdgeDataSource
from gs_flex_coordinator.models.vertex_data_source import VertexDataSource
from gs_flex_coordinator import util

from gs_flex_coordinator.models.edge_data_source import EdgeDataSource  # noqa: E501
from gs_flex_coordinator.models.vertex_data_source import VertexDataSource  # noqa: E501

class DataSource(Model):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.
    """

    def __init__(self, vertices_datasource=None, edges_datasource=None):  # noqa: E501
        """DataSource - a model defined in OpenAPI

        :param vertices_datasource: The vertices_datasource of this DataSource.  # noqa: E501
        :type vertices_datasource: List[VertexDataSource]
        :param edges_datasource: The edges_datasource of this DataSource.  # noqa: E501
        :type edges_datasource: List[EdgeDataSource]
        """
        self.openapi_types = {
            'vertices_datasource': List[VertexDataSource],
            'edges_datasource': List[EdgeDataSource]
        }

        self.attribute_map = {
            'vertices_datasource': 'vertices_datasource',
            'edges_datasource': 'edges_datasource'
        }

        self._vertices_datasource = vertices_datasource
        self._edges_datasource = edges_datasource

    @classmethod
    def from_dict(cls, dikt) -> 'DataSource':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The DataSource of this DataSource.  # noqa: E501
        :rtype: DataSource
        """
        return util.deserialize_model(dikt, cls)

    @property
    def vertices_datasource(self) -> List[VertexDataSource]:
        """Gets the vertices_datasource of this DataSource.


        :return: The vertices_datasource of this DataSource.
        :rtype: List[VertexDataSource]
        """
        return self._vertices_datasource

    @vertices_datasource.setter
    def vertices_datasource(self, vertices_datasource: List[VertexDataSource]):
        """Sets the vertices_datasource of this DataSource.


        :param vertices_datasource: The vertices_datasource of this DataSource.
        :type vertices_datasource: List[VertexDataSource]
        """

        self._vertices_datasource = vertices_datasource

    @property
    def edges_datasource(self) -> List[EdgeDataSource]:
        """Gets the edges_datasource of this DataSource.


        :return: The edges_datasource of this DataSource.
        :rtype: List[EdgeDataSource]
        """
        return self._edges_datasource

    @edges_datasource.setter
    def edges_datasource(self, edges_datasource: List[EdgeDataSource]):
        """Sets the edges_datasource of this DataSource.


        :param edges_datasource: The edges_datasource of this DataSource.
        :type edges_datasource: List[EdgeDataSource]
        """

        self._edges_datasource = edges_datasource
