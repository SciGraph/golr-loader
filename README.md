[![Build Status](https://travis-ci.org/SciGraph/golr-loader.svg?branch=master)](https://travis-ci.org/SciGraph/golr-loader)

# golr-loader

Convert [SciGraph](https://github.com/SciGraph/SciGraph) query results into JSON 
that can be loaded by a [Golr](http://wiki.geneontology.org/index.php/GOlr) instance.

## Example

golr-loader applies conventions to a Cypher execution result and produces Golr loadable JSON facilitating the creation of "Golr quads": 

* node ID
* node ID closure 
* node rdfs:label 
* node rdfs:label closure

Processing resolves around the Cypher `RETURN` clause. 

* Literals are mapped directly to JSON literals with the same name as the Cypher projection
* Nodes are mapped to Golr quads with the same root name as the Cypher projection
* Relationships are resolved to their object property nodes and mapped to Golr quads
* Paths are combined with any property container above to produce "evidence" which is then stored as a Golr quad and materialized as a JSON graph

By default golr-load traverses rdf:type and rdf:subClassOf when calculating closures.

The executable JAR produced by `mvn package` takes the following arguments:

* The location of SciGraph YAML file
* The location of a golr-loader query YAML file
* An optional location to save the generated JSON  
* An optional Solr server to POST the generated JSON

Here's an example configuration:

````
query: |
        MATCH path = (subject:gene)-[relation:RO_HOM0000001!]-(object:gene) 
        RETURN path, 
        subject, object, relation, 
        'gene' AS subject_category, 'gene' AS object_category, 'direct' AS qualifier
  
````

which results in the following JSON structure:

````
[
    {
        "evidence": [
            "MONARCH:b0246c04ec0245c5c32e88f79dea1b8b"
        ],
        "evidence_closure": [
            "MONARCH:b0246c04ec0245c5c32e88f79dea1b8b",
            "Annotation:"
        ],
        "evidence_closure_label": [
            "MONARCH:b0246c04ec0245c5c32e88f79dea1b8b",
            "Annotation:"
        ],
        "evidence_graph": "", // Omitted for brevity
        "evidence_label": [
            ""
        ],
        "object": "ENSEMBL:ENSG00000183304",
        "object_category": "gene",
        "object_closure": [
            "ENSEMBL:ENSG00000183304"
        ],
        "object_closure_label": [
            "ENSEMBL:ENSG00000183304"
        ],
        "object_label": "",
        "qualifier": "direct",
        "relation": "RO:HOM0000017",
        "relation_closure": [
            "RO:HOM0000017",
            "RO:HOM0000007",
            "RO:HOM0000001",
            "RO:HOM0000000",
            "RO:0002158",
            "RO:0002320"
        ],
        "relation_closure_label": [
            "in orthology relationship with",
            "in historical homology relationship with",
            "in homology relationship with",
            "in similarity relationship with",
            "shares ancestor with",
            "evolutionarily related to"
        ],
        "relation_label": "in orthology relationship with",
        "subject": "ENSEMBL:ENSMUSG00000096881",
        "subject_category": "gene",
        "subject_closure": [
            "ENSEMBL:ENSMUSG00000096881"
        ],
        "subject_closure_label": [
            "ENSEMBL:ENSMUSG00000096881"
        ],
        "subject_label": ""
    }
]
````



