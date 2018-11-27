from __future__ import division
import dataGenerator
import measures

KL_DIVERGENCE="rKL" # represent kl-divergence group fairness measure
ND_DIFFERENCE="rND" # represent normalized difference group fairness measure
RD_DIFFERENCE="rRD" # represent ratio difference group fairness measure
NORM_ITERATION=100 # max iterations used in normalizer computation

def main(_user_N,_pro_N,_gfmeasure,_cut_point,_rez_fn):
    """
        Run the group fairness experiments of synthetic unfair rankings.
        Output group fairness results as csv file.        
        
        :param _user_N: The total user number of input ranking
        :param _pro_N: The size of protected group in the input ranking        
        :param _gfmeassure: The group fairness measure to be used in calculation 
                            one of "rKL", "rND" and "rRD" defined as constant in this py file 
        :param _cut_point: The cut off point of set-wise group fairness calculation               
        :param _rez_fn: The file name to output group fairness results
        
        :return: no returns.
    """

    # define the input fairness probability
    f_probs=[i/10 for i in range(10)] 
    f_probs.append(0.98) #using 0.98 as extreme case considering random generator
    

    #define output file
    output_fn=_rez_fn+"_user"+str(_user_N)+"_pro"+str(_pro_N)+".csv"
    with open(output_fn,'w') as mf:
        mf.write(""+"\n")
    rez_file=open(output_fn, 'a')
    # calculate the normalizer of the input user number and protected group
    max_GF=measures.getNormalizer(_user_N,_pro_N,_gfmeasure) 
    # generate a random input ranking and protected group
    input_ranking=[x for x in range(_user_N)]
    sensi_idx=[x for x in range(_pro_N)]
    
    gf_results=[] 
    # loop the input fairness probabilities
    for fpi in range(len(f_probs)): 
        fp=f_probs[fpi]
        gf_iters=0 
        for iteri in range(1,NORM_ITERATION+1):
            sRFair=dataGenerator.generateUnfairRanking(input_ranking,sensi_idx,fp)                   
            gf=measures.calculateNDFairness(sRFair,sensi_idx,_cut_point,_gfmeasure,max_GF) 
            gf_iters=gf_iters+gf 
        gf_results.append(gf_iters/NORM_ITERATION) #record average result
        print "Finished fp ",fp,'at pro N ',_pro_N

    # output results into csv file    
    fline=""
    for item in gf_results:
        fline=fline+str(item)+","

    rez_file.write(fline+"\n")
    rez_file.close()


if __name__ == "__main__":
    main()