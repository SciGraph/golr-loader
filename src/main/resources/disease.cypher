MATCH (subject)<-[:GENO_0000410!*0..1]-(variant)-[:sameAs*0..1]-(ve)-[:RO_0002200|RO_0002610|RO_0002326!]->(disease:disease)
WHERE ID(subject) = {id}
RETURN disease
UNION
MATCH path = (subject)<-[:GENO_0000410!*0..1]-(variant)-[:sameAs*0..1]-(ve)<-[:BFO_0000051!*]-(genotype)-[:RO_0002200|RO_0002610|RO_0002326!]->(disease:disease)
WHERE ID(subject) = {id}
RETURN disease
UNION
MATCH path = (subject)<-[:GENO_0000410!*0..1]-(variant)-[:sameAs*0..1]-(ve)<-[:BFO_0000051!*]-(genotype)<-[:GENO_0000222]-(person)-[:RO_0002200!]->(disease:disease)
WHERE ID(subject) = {id}
RETURN disease