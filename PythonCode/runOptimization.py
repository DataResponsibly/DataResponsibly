from __future__ import division
import numpy as np
import scipy.optimize as optim
import time
import optimization
import measures
import utility

KL_DIVERGENCE="rKL" # represent kl-divergence group fairness measure
ND_DIFFERENCE="rND" # represent normalized difference group fairness measure
RD_DIFFERENCE="rRD" # represent ratio difference group fairness measure

SCORE_DIVERGENCE="scoreDiff" # represent average score difference -ranking accuracy measure
POSITION_DIFFERENCE="positionDiff" # represent average position difference -ranking accuracy measure
KENDALL_DIS="kendallDis" # represent kendall distance -ranking accuracy measure
SPEARMAN_COR="spearmanDis" # represent spearman correlation -ranking accuracy measure
PEARSON_COR="pearsonDis" # represent pearson correlation -ranking accuracy measure

def main(_csv_fn,_target_cols,_sensi_bound,_k,_accmeasure,_cut_point,_rez_fn):
    """
        Run the optimization process.
        Output evaluation results as csv file.
        Output results (accuracy, group fairness in op, values of group fairness measures) during optimization as txt files. 
        
        :param _csv_fn: The file name of input data stored in csv file
                        In csv file, one column represents one attribute of user
                        one row represents the feature vector of one user
        :param _target_cols: The target attribute ranked on i.e. score of ranking
        :param _sensi_bound: The value of sensitve attribute to use as protected group, 0 or 1, usually 1 represnts belonging to protected group 
                             Applied for binary sensitve attribute
        :param _k: The number of clusters in the intermediate layer of neural network
        :param _accmeasure: The accuracy measure used in this function, one of constant string defined in this py file
        :param _cut_point: The cut off point of set-wise group fairness calculation
        :param _rez_fn: The file name to output optimization results
        :return: no returns.
    """
    # change to function to get the user number and size of protected group        
    user_N=1000
    pro_N=190

    # get the maximum value first to run fast        
    max_rKL=measures.getNormalizer(user_N,pro_N,KL_DIVERGENCE) 
    max_rND=measures.getNormalizer(user_N,pro_N,ND_DIFFERENCE)
    max_rRD=measures.getNormalizer(user_N,pro_N,RD_DIFFERENCE)

    # initialize the outputted csv file
    result_fn=_rez_fn+".csv"
    with open(result_fn,'w') as mf:
        mf.write("UserN,pro_N,K,TargetAtt,AccMeasure,acc_value,rKL,rKL_value,rND,rND_value,rRD,rRD_value,timeSpent\n")
    rez_file=open(result_fn, 'a')

    for targeti in _target_cols:
        data,input_scores,pro_data,unpro_data,pro_index=utility.readCSV(_csv_fn,targeti,_sensi_bound)
        input_ranking=sorted(range(len(input_scores)), key=lambda k: input_scores[k],reverse=True)


        input_rKL=measures.calculateNDFairness(input_ranking,pro_index,_cut_point,KL_DIVERGENCE,max_rKL)
        input_rND=measures.calculateNDFairness(input_ranking,pro_index,_cut_point,ND_DIFFERENCE,max_rND)
        input_rRD=measures.calculateNDFairness(input_ranking,pro_index,_cut_point,RD_DIFFERENCE,max_rRD)

        # record the start time of optimization
        start_time = time.time()
        print "Starting optimization @ ",_k,"ACCM ",_accmeasure," time: ", start_time
        
        # initialize the optimization
        rez,bnd=optimization.initOptimization(data,_k)
        # record the accuracy and group fairness during the optimization
        acc_ops=[] 
        gf_ops=[] 
        gf_rKLs=[]
        gf_rNDs=[]
        gf_rRDs=[]
        
        optimization.lbfgsOptimize.iters=0                
        rez = optim.fmin_l_bfgs_b(lbfgsOptimize, x0=rez, disp=1, epsilon=1e-5, 
                       args=(pro_data, unpro_data, data, input_scores,_accmeasure, _k, 0.01,
                             1, 100, 0), bounds = bnd,approx_grad=True, factr=1e12, pgtol=1e-04,maxfun=15000, maxiter=15000)
        end_time = time.time()
        print "Ending optimization @ ",_k,"ACCM ",_accmeasure," time: ", end_time
        # get the evaluation result after converged
        estimate_scores,acc_value=optimization.calculateEvaluateRez(rez,data,input_scores,_k,_accmeasure)
                

        estimate_ranking=sorted(range(len(estimate_scores)), key=lambda k: estimate_scores[k],reverse=True)
        
        eval_rKL=measures.calculateNDFairness(estimate_ranking,pro_index,_cut_point,KL_DIVERGENCE,max_rKL)
        eval_rND=measures.calculateNDFairness(estimate_ranking,pro_index,_cut_point,ND_DIFFERENCE,max_rND)
        eval_rRD=measures.calculateNDFairness(estimate_ranking,pro_index,_cut_point,RD_DIFFERENCE,max_rRD)
        
        # prepare the result line to write
        rez_fline=str(user_N)+","+str(pro_N)+","+str(_k)+","+str(targeti)+","+str(_accmeasure)+","+str(acc_value)+","+str(input_rKL)+","+str(eval_rKL)+","+str(input_rND)+","+str(eval_rND)+","+str(input_rRD)+","+str(eval_rRD)+","+str(end_time-start_time)+"\n"
        rez_file.write(rez_fline)
        # output the recorded group fairness and accuracy during the optimization
        acc_op_fn="AccOP_Target"+str(targeti)+"_proN"+str(pro_N)+"_K"+str(_k)+"_Acc"+str(_accmeasure)+".txt"
        gf_op_fn="GfOP_Target"+str(targeti)+"_proN"+str(pro_N)+"_K"+str(_k)+"_Acc"+str(_accmeasure)+".txt"
        rKL_op_fn="rKL_Target"+str(targeti)+"_proN"+str(pro_N)+"_K"+str(_k)+"_Acc"+str(_accmeasure)+".txt"
        rND_op_fn="rND_Target"+str(targeti)+"_proN"+str(pro_N)+"_K"+str(_k)+"_Acc"+str(_accmeasure)+".txt"
        rRD_op_fn="rRD_Target"+str(targeti)+"_proN"+str(pro_N)+"_K"+str(_k)+"_Acc"+str(_accmeasure)+".txt"

        np.savetxt(acc_op_fn,np.array(acc_ops))
        np.savetxt(gf_op_fn,np.array(gf_ops))
        np.savetxt(rKL_op_fn,np.array(gf_rKLs))
        np.savetxt(rND_op_fn,np.array(gf_rNDs))
        np.savetxt(rRD_op_fn,np.array(gf_rRDs))

    rez_file.close()

if __name__ == "__main__":
    main()




            
            
            
            