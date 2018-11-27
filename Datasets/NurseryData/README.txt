The “nursery” dataset hosted on UCI contains approximately 12,960 observations, with 8 variables. Nursery Database was derived from a hierarchical decision model originally developed to rank applications for nursery schools. It was used during several years in 1980's when there was excessive enrollment to these schools in Ljubljana, Slovenia, and the rejected applications frequently needed an objective explanation. The final decision depended on three sub-problems: occupation of parents and child's nursery, family structure and financial standing, and social and health picture of the family. The model was developed within expert system shell for decision. The hierarchical model ranks nursery-school applications according to the following concept structure.
NURSERY Evaluation of applications for nursery schools
. EMPLOY Employment of parents and child's nursery
. . parents Parents' occupation
. . has_nurs Child's nursery
. STRUCT_FINAN Family structure and financial standings
. . STRUCTURE Family structure
. . . form Form of the family
. . . children Number of children
. . housing Housing conditions
. . finance Financial standing of the family
. SOC_HEALTH Social and health picture of the family
. . social Social conditions
. . health Health conditions
The Nursery data contains examples with the structural information removed, i.e., directly relates NURSERY to the 8 input attributes: parents, has_nurs, form, children, housing, finance,social, health.

Attributes(8): 
parents: Categorical. Values: usual, pretentious, great_pret.

has_nurs: Categorical. Values: proper, less_proper, improper, critical, very_crit.

form: Categorical. Values:  complete, completed, incomplete, foster.

children: Categorical. Values:  1, 2, 3, more.

housing: Categorical. Values:  convenient, less_conv, critical.

finance: Binary. Values: convenient, inconv.

social: Categorical. Values: non-prob, slightly_prob, problematic.

health: Categorical. Values: recommended, priority, not_recom.

dependent variable: Categorical. 
Values: not_recom, recommend, very_recom, priority, spec_prior.

