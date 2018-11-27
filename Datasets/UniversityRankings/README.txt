Normalization Formula: (Code in preprocess_data.py)
normalized value = current value - minimum value / maximum value - minimum value
normalized value in [0,1]

Reoriented Formula: (Code in preprocess_data.py)
reoriented value = 1 - normalized value
reoriented value in [0,1] higher is better for the target ranking variable.



Parsed data is processed through the following steps: (Code in utility_preprocess.py)

(1) Delete all the useless charachter attributes (usually done by manually check and delete). 

(2) Transform the useful charachter attributes into numerical value. The mapping is defined in the mapping files under the same folder.(If needed)

(3) Normalize all the attributes using the above normailize formula. (Manuaaly check to define what attributes need to be normalized)

(4) Reoriented all the related attributes using the above reoriented formula.Manuaaly check to define what attributes need to be normalized)

(5) Replace all empty attribues with -1. Then output processed data into new csv file.