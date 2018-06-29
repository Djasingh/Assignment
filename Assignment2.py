from pyspark import SparkContext
import math
import time
start_time=time.time()
sc = SparkContext()
infinite_val=500
Dataset=sc.textFile("ass2-eda-18.txt")
result_file_ptr = open('ass2-eda-result.txt', 'w')
result_file_ptr.write("c0\tc1\tc2\tc3\tc4\n")

test_instance=[t.split() for t in Dataset.toLocalIterator()]

for t in test_instance:
    def class_Edist_map(line): # for each test instance, calculating the distance from remaining instances in Dataset 
        line=line.split()
	l=len(line)-1
        class_label=line[l]
        feature=line[:-1]
        dist=0
	if line==t:             # Excluding test instance from Dataset		
		return(class_label,infinite_val)
        for i,j in zip(feature,t[:-1]):
            dist += math.pow((float(i)-float(j)),2)
        Edist=math.sqrt(dist)
        return(class_label, Edist)
    dataset_map=Dataset.map(class_Edist_map)


    sorted_neighbour=dataset_map.takeOrdered(50, key=lambda x: x[1])
    result =sc.parallelize(map(lambda x: (x[0],1),sorted_neighbour)).reduceByKey(lambda x, y: (x + y))
    
    if(result.count()<5):
	result=[(k,v) for k,v in result.toLocalIterator()]
	class_label=[int(k) for k,v in result]
	for i in range(0,5):
		if i not in class_label:
		    result.append((unicode(i),0))
	result=sc.parallelize(result)	
	
	
    class_prop=sc.parallelize(result.takeOrdered(5, key=lambda x: int(x[0]))).map(lambda x: float(x[1])/float(50))
    


    for item in class_prop.toLocalIterator():
      result_file_ptr.write("%s\t" % item)
    result_file_ptr.write("\n")
 

print("------%s seconds --------------------------------------------" % (time.time() - start_time))
