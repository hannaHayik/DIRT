# DIRT
Discovery of Inference Rules from Text, Using Hadoop &amp; Amazon EMR  

Objective: Implement DIRT algorithm according to the attached research paper, design a MapReduce program and evaluate the results.  
  
For Example: The program finds the similarity measure between these two sentences according to the dataset: (X leads to Y) and (X cause Y) where X & Y are nouns.  

Our goal is to implement an application that can measure the similarity between two sentences according to the DIRT algorithm in the given article.  
Using the Biarcs dataset and AWS ElasticMapReduce service we explain the design of our application in this document.  
The whole application was made in EMR, apart from a pre-processing of the input beforehand. My process has 3 components:  
 - HadoopRunner.java  
 - StepsRunner.java  
 - AWS ElasticMapReduce Steps 1-7  
  
The EMR application contains 7 steps, that checks the valid biarcs, constructs paths and perform the necessary calculations that are mentioned in the DIRT article.  
  
The program also calculates, as a middle result, the "Mutual Information" or simply "mi(p, slot, w)" for every triple: path, slot and word.  

For more technical explanations please refer to the .pdf documents in the repo.  

Acknowldgment: The research paper is made by Dekang Lin & Patrick Pantel from the University of Alberta.
