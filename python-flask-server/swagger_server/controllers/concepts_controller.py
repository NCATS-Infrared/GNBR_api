import connexion
import six

from neo4j.v1 import GraphDatabase
from swagger_server.models.beacon_concept import BeaconConcept  # noqa: E501
from swagger_server.models.beacon_concept_detail import BeaconConceptDetail
from swagger_server.models.beacon_concept_with_details import BeaconConceptWithDetails  # noqa: E501
from swagger_server import util


def get_concept_details(conceptId):  # noqa: E501
    """get_concept_details

    Retrieves details for a specified concepts in the system, as specified by a (url-encoded) CURIE identifier of a concept known the given knowledge source.  # noqa: E501

    :param conceptId: (url-encoded) CURIE identifier of concept of interest
    :type conceptId: str

    :rtype: List[BeaconConceptWithDetails]
    """
    # query= """
    # MATCH (:Entity {uri: {entity_id} })-[r:IN_SENTENCE]-(s:Sentence)
    # WITH DISTINCT s, r.raw_string as name
    # MATCH (e:Entity)-[:IN_SENTENCE]-(s)
    # WITH DISTINCT s, name, count(e) as ecount
    # MATCH (s)-[:HAS_THEME]-(th:Theme)
    # RETURN s, name, ecount,  reduce(accumulator = 0.0, key IN keys(th) | accumulator + th[key]) as values
    # """
    
    # order_by = {
    # 'score': 'ORDER BY log(values) DESC LIMIT 5',
    # 'complexity': 'ORDER BY ecount DESC LIMIT 5',
    # 'informative': 'ORDER BY ecount + log(values) DESC LIMIT 5'
    # }

    query= """
    MATCH (:Entity {uri: {entity_id} })-[r:IN_SENTENCE]-(s:Sentence)
    WITH DISTINCT s, r.raw_string as name
    MATCH (s)-[:HAS_THEME]-(th:Theme)
    RETURN s, name, sum(reduce(accumulator = 0.0, key IN keys(th) | accumulator + th[key])) as values
    ORDER BY log(values) DESC
    """

    entity_id = conceptId
    driver = GraphDatabase.driver('bolt://172.18.0.2:7687', auth=('',''))
    with driver.session() as neo4j:
        results = neo4j.run(query, {"entity_id" : entity_id})
    details, synonyms = zip( *[ ( BeaconConceptDetail(tag='pmid:' + record['s']['pmid'], value=record['s']['text'] ), record['name']) for record in results ] )
    print(details)
    return [BeaconConceptWithDetails(id=conceptId, synonyms=synonyms, details=details)]


def get_concepts(keywords, types=None, pageNumber=None, pageSize=None):  # noqa: E501
    """get_concepts

    Retrieves a (paged) list of whose concept in the  beacon knowledge base with names and/or synonyms  matching a set of keywords or substrings.  The (possibly paged) results returned should generally  be returned in order of the quality of the match,  that is, the highest ranked concepts should exactly  match the most keywords, in the same order as the  keywords were given. Lower quality hits with fewer  keyword matches or out-of-order keyword matches,  should be returned lower in the list.  # noqa: E501

    :param keywords: a (urlencoded) space delimited set of keywords or substrings against which to match concept names and synonyms
    :type keywords: str
    :param types: a (url-encoded) space-delimited set of semantic groups (specified as codes gene, pathway, etc.) to which to constrain concepts matched by the main keyword search (see [Biolink Model](https://biolink.github.io/biolink-model) for the full list of codes) 
    :type types: str
    :param pageNumber: (1-based) number of the page to be returned in a paged set of query results 
    :type pageNumber: int
    :param pageSize: number of concepts per page to be returned in a paged set of query results 
    :type pageSize: int

    :rtype: List[BeaconConcept]
    """
    if types.lower() == 'chemical':
        match = """
        MATCH (m:Chemical)-[:IN_SENTENCE {raw_string: {keyword}}]-(:Sentence) 
        WITH m LIMIT 1 
        """
 
    elif types.lower() == 'disease':
        match = """
        MATCH (m:Disease)-[:IN_SENTENCE {raw_string: {keyword}}]-(:Sentence) 
        WITH m LIMIT 1 
        """
 
    elif types.lower() == 'gene':
        match = """
        MATCH (m:Gene)-[:IN_SENTENCE {raw_string: {keyword}}]-(:Sentence) 
        WITH m LIMIT 1 
        """
    

    else:
        match = """ MATCH (m:Entity)-[:IN_SENTENCE {raw_string: {keyword}}]-(:Sentence) 
        WITH m LIMIT 1
        """

    collect = """
    MATCH (m)-[r:IN_SENTENCE]-(:Sentence)
    RETURN m, collect(distinct r.raw_string) as syns, labels(m) as labels
    """
    query = match + collect

    # query = """
    # MATCH (m:Entity)-[:IN_SENTENCE {raw_string: {keyword}}]-(:Sentence) 
    # WITH m LIMIT 1 
    # MATCH (m)-[r:IN_SENTENCE]-(:Sentence)
    # RETURN m, collect(distinct r.raw_string) as syns, labels(m) as labels
    # """

    """
    MATCH (m)-[r:IN_SENTENCE]-(:Sentence)
    return m.uri as uri, m.name as name, collect(distinct r.raw_string) as syns
    """
    word = keywords
    driver = GraphDatabase.driver('bolt://172.18.0.2:7687', auth=('',''))
    with driver.session() as neo4j:
        results = neo4j.run(query, {"keyword" : keywords})
    return [BeaconConcept(id=record['m']['uri'], name=record['m']['name'], type=','.join(record['labels']), synonyms=record['syns']) for record in results]
    # return [BeaconConcept(id=record['uri'], name=record['name'], synonyms=record['syns']) for record in results]


def get_exact_matches_to_concept(conceptId):  # noqa: E501
    """get_exact_matches_to_concept

    Retrieves a list of qualified identifiers of \&quot;exact match\&quot; concepts, [sensa SKOS](http://www.w3.org/2004/02/skos/core#exactMatch) associated with a specified (url-encoded) CURIE (without brackets) concept object identifier,  typically, of a concept selected from the list of concepts originally returned by a /concepts API call on a given KS.   # noqa: E501

    :param conceptId: (url-encoded) CURIE identifier of the concept to be matched
    :type conceptId: str

    :rtype: List[str]
    """
    query = """
    MATCH (e:Entity {uri: {entity_id}})
    RETURN e
    """

    entity_id = conceptId
    driver = GraphDatabase.driver('bolt://172.18.0.2:7687', auth=('',''))
    with driver.session() as neo4j:
        results = neo4j.run(query, {"entity_id" : entity_id})
    return [record['e']['uri'] for record in results]


def get_exact_matches_to_concept_list(c):  # noqa: E501
    """get_exact_matches_to_concept_list

    Given an input list of [CURIE](https://www.w3.org/TR/curie/) identifiers of known exactly matched concepts [*sensa*-SKOS](http://www.w3.org/2004/02/skos/core#exactMatch), retrieves the list of [CURIE](https://www.w3.org/TR/curie/) identifiers of additional concepts that are deemed by the given knowledge source to be exact matches to one or more of the input concepts **plus** whichever identifiers from the input list which specifically matched these new additional concepts.  If an empty set is returned, the it can be assumed that the given  knowledge source does not know of any new equivalent concepts matching the input set.  # noqa: E501

    :param c: set of [CURIE-encoded](https://www.w3.org/TR/curie/) identifiers of exactly matching concepts, to be used in a search for additional exactly matching concepts [*sensa*-SKOS](http://www.w3.org/2004/02/skos/core#exactMatch). 
    :type c: List[str]

    :rtype: List[str]
    """
    return []
