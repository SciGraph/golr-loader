MATCH (subject)<-[:GENO:0000410!*0..1]-(variant)-[:sameAs*0..1]-(ve)-[:RO:0002200|RO:0002610|RO:0002326!]->(disease:disease)
WHERE ID(subject) = {id}
RETURN disease
UNION
MATCH path = (subject)<-[:GENO:0000410!*0..1]-(variant)-[:sameAs*0..1]-(ve)<-[:BFO:0000051!*]-(genotype)-[:RO:0002200|RO:0002610|RO:0002326!]->(disease:disease)
WHERE ID(subject) = {id}
RETURN disease
UNION
MATCH path = (subject)<-[:GENO:0000410!*0..1]-(variant)-[:sameAs*0..1]-(ve)<-[:BFO:0000051!*]-(genotype)<-[:GENO:0000222]-(person)-[:RO:0002200!]->(disease:disease)
WHERE ID(subject) = {id}
RETURN disease