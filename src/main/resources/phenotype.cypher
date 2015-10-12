MATCH (subject)<-[:GENO:0000410!*0..1]-(feature)-[:RO:0002200|RO:0002610|RO:0002326!]->(phenotype:Phenotype)
WHERE ID(subject) = {id}
RETURN phenotype
UNION
MATCH (subject)<-[:GENO:0000410!*0..1]-(feature)-[:RO:0002200|RO:0002610|RO:0002326!*2..]->(phenotype:Phenotype)
WHERE ID(subject) = {id}
RETURN phenotype
UNION
MATCH (subject)<-[:GENO:0000410!*0..1]-(feature)<-[:BFO:0000051!*]-(genotype)-[:RO:0002200|RO:0002610|RO:0002326!*]->(phenotype:Phenotype)
WHERE ID(subject) = {id}
RETURN phenotype
UNION
MATCH (subject)<-[:GENO:0000410!*0..1]-(feature)<-[:BFO:0000051!*]-(genotype)<-[:GENO:0000222]-(person)-[:RO:0002200|RO:0002610|RO:0002326!*]->(phenotype:Phenotype)
WHERE ID(subject) = {id}
RETURN phenotype