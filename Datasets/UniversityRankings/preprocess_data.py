def normalizeDataset(inputdata,noweightlist=[]):
    """
        inputdata is a dataframe stored all the data read from a csv source file
        noweightlist is a array like data structure stored the attributes which should be ignored in the normalization process.
        return the processed inputdata
    """
    df = inputdata.loc[:,inputdata.columns.difference(noweightlist)] # remove no weight attributes
    #normalize attributes
    for attri in df.columns:
        df = (df - df.min()) / (df.max() - df.min())
    inputdata.loc[:,inputdata.columns.difference(noweightlist)] = df
    return inputdata
def logNormalized(inputdata,noweightlist=[]):
    """
        inputdata is a dataframe stored all the data read from a csv source file
        noweightlist is a array like data structure stored the attributes which should be ignored in the normalization process.
        return the processed inputdata
    """
    df = inputdata.loc[:,inputdata.columns.difference(noweightlist)] # remove no weight attributes
    df2 = inputdata.loc[:,inputdata.columns.difference(noweightlist)]
    for attri in df.columns:
        df = (df - df.min())
    for attri in df2.columns:
        df2 =  (df2.max() - df2.min())
    for attri in df.columns:   
        df[attri] = np.log2(df[attri],dtype='float64') - np.log2(df2[attri],dtype='float64')
    inputdata.loc[:,inputdata.columns.difference(noweightlist)] = df
    return inputdata
def reverseDataset(inputdata,noweightlist=[]):
    """
        inputdata is a dataframe stored all the data read from a csv source file
        noweightlist is a array like data structure stored the attributes which should be ignored in the reverse process.
        return the processed inputdata
    """
    df = inputdata.loc[:,inputdata.columns.difference(noweightlist)] # remove the attributes which have no weight in this process
    #reverse attributes
    for attri in df.columns:
        df = 1.0 - df
    inputdata.loc[:,inputdata.columns.difference(noweightlist)] = df
    return inputdata
