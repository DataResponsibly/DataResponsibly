import pandas as pd
import numpy as np
from math import log
from preprocess_data import * # import the function from process module

# read into source file
source_file = "LendingClubDenied_Normalized"
data = pd.read_csv(source_file+'.csv')

# first normalize then reverse

# for lending club denied data set normalize
noweight_norm=['Risk_Score','State','Employment Length']
norm_data = normalizeDataset(data,noweight_norm)

# for lending club denied data set reverse
noweight_reverse=['Risk_Score','State','Employment Length','Amount Requested']
proc_data=reverseDataset(norm_data,noweight_reverse)

# for cs ranking data set
# noweight_norm=['CID'] # defined by manually check
# norm_data=normalizeDataset(data,noweight_norm)

# fill all the empty attribute with -1 
proc_data=proc_data.fillna(value=-1)
# save processed data into a new csv file
save_file = source_file+"_Normalized"
proc_data.to_csv(save_file+".csv", sep=',')