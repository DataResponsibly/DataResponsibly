1-  This directory contains three  datasets, i.e., Census Income dataset, Communities and Crimes dataset
and  German credit dataset available in "A. Asuncion and D. Newman. UCI machine learning repository.
2007  http://archive.ics.uci.edu/ml/datasets.html ". These datasets are provided in ARFF format which can
be used directly in Weka.

2- For these datasets, dependency parameters for the datasets in this directory are kept as follows:

<Sensitve Attribute>
         Sensitive Attribute is an attribute on which class labels are dependent, e.g., gender, race
	   is positioned as a first attribute in these datasets (default position of SA).

 <Desired class index>
         Desired class is class from which the deprived community is denied like Job=yes. Second value
	   of class attribute is selected as desired class.

<Deprived community>
         Value of deprived community of sensitive attribute (SA), e.g., female, black
          First value of SA, i.e., at index 0 of SA is used as Derived Community.
<Favored community>
         Value of favored community of sensitive attribute (SA), e.g., male, white
         Second value of SA, i.e., at index 1 of SA is used as Derived Community.
