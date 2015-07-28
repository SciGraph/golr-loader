MATCH (subject)<-[:GENO_0000410!*0..1]-(feature)-[:RO_0002200|RO_0002610|RO_0002326!]->(phenotype:Phenotype)
WHERE ID(subject) = {id}
RETURN phenotype
UNION
MATCH (subject)<-[:GENO_0000410!*0..1]-(feature)-[:RO_0002200|RO_0002610|RO_0002326!*2..]->(phenotype:Phenotype)
WHERE ID(subject) = {id}
RETURN phenotype
UNION
MATCH (subject)<-[:GENO_0000410!*0..1]-(feature)<-[:BFO_0000051!*]-(genotype)-[:RO_0002200|RO_0002610|RO_0002326!*]->(phenotype:Phenotype)
WHERE ID(subject) = {id}
RETURN phenotype
UNION
MATCH (subject)<-[:GENO_0000410!*0..1]-(feature)<-[:BFO_0000051!*]-(genotype)<-[:GENO_0000222]-(person)-[:RO_0002200|RO_0002610|RO_0002326!*]->(phenotype:Phenotype)
WHERE ID(subject) = {id}
RETURN phenotype