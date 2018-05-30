// Find sentences relating a chemical, gene, and a disease
match p=(:Chemical)-[:IN_SENTENCE]-(s:Sentence),
(s)-[:IN_SENTENCE]-(:Gene),
(s)-[:IN_SENTENCE]-(:Disease) 
return s limit 25

// Find sentences that relate 2 chemicals and a disease
match p=(:Chemical)-[:IN_SENTENCE]-(s:Sentence),
(s)-[:IN_SENTENCE]-(:Chemical),
(s)-[:IN_SENTENCE]-(:Disease) 
return s limit 25


// Fanconi Anemia and TNFa, diseases
match p=(:Disease{uri:"MESH:D005199"})-[:IN_SENTENCE]-(s:Sentence), 
(s)-[:IN_SENTENCE]-(g:Gene{uri: "ncbigene:21926"}) 
return p limit 25

// Find Diseases linked to FANCA
match p=(g:Gene{uri:"ncbigene:2175"})-[s:STATEMENT]-(:Disease) 
where s.u_ind >0 return p limit 25
