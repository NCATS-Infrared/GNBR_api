import connexion
import six

from neo4j.v1 import GraphDatabase
from swagger_server.models.beacon_concept_type import BeaconConceptType  # noqa: E501
from swagger_server.models.beacon_predicate import BeaconPredicate  # noqa: E501
from swagger_server import util
from swagger_server.metadata.predicates import predicate_map


def get_concept_types():  # noqa: E501
    """get_concept_types

    Get a list of types and # of instances in the knowledge source, and a link to the API call for the list of equivalent terminology  # noqa: E501


    :rtype: List[BeaconConceptType]
    """
    query="""
    MATCH (n) 
    RETURN DISTINCT count(labels(n)) as count, labels(n) as types;
    """
    driver = GraphDatabase.driver('bolt://db:7687', auth=('',''))
    with driver.session() as neo4j:
        results = neo4j.run(query)
    thing = [BeaconConceptType(id=','.join(record['types']), frequency=record['count']) for record in results]
    return thing


def get_predicates():  # noqa: E501
    """get_predicates

    Get a list of predicates used in statements issued by the knowledge source  # noqa: E501


    :rtype: List[BeaconPredicate]
    """
    # query="""
    # MATCH (t:Theme)
    # UNWIND keys(t) AS key
    # WITH DISTINCT key
    # ORDER by key
    # RETURN key
    # """
    # driver = GraphDatabase.driver('bolt://172.18.0.2:7687', auth=('',''))
    # with driver.session() as neo4j:
    #     results = neo4j.run(query)
    # predicates = []
    # for record in results:
    #     key = record['key']
    #     if key.endswith('_ind'):
    #         continue
    #     value = predicate_map[key]
    #     predicates.append(BeaconPredicate(id=key, name=value))
    predicates = [BeaconPredicate(id=key, name=value) for key, value in predicate_map.items()]
    return predicates

