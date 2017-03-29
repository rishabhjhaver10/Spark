from pyspark import SparkConf, SparkContext
import string
import itertools
conf = SparkConf().setMaster("local").setAppName("shakespeare1")
sc = SparkContext(conf = conf)

#Creating an RDD (file path, text in file)
data = sc.wholeTextFiles("file:///C:/Users/risha/Downloads/Shakespeare")

#Creating an RDD (text in file, file name)
data_1 = data.map(lambda (x, y) : (y, x)).mapValues(lambda x : x.split('/')[-1])

#Creating an RDD (word, file name)
data_2 = data_1.flatMap(lambda (x, y) : zip(x.split(), itertools.repeat(y)))

#Function to remove punctuation
remove_punct = lambda x : x not in string.punctuation

#Creating an RDD with words(keys) withot punctuation
data_3 = data_2.map(lambda (x, y) : (filter(remove_punct, x), y))

#Creating an RDD with words(keys) in lower case and removing words with lenght 0
data_4 = data_3.map(lambda (x, y) : (x.lower(), y)).filter(lambda (x,y) : len(x) > 0)

#Creating an RDD grouped and sorted by words(keys), also mapping words(keys) as strings
data_5 = data_4.groupByKey().mapValues(list).sortByKey().map(lambda (x,y) : (str(x),y))

#Function that returns list with unique file names
def unique_list(item):
    new_list = []
    for i in item: 
        if i not in new_list:
            new_list.append(str(i))
    return new_list

#Creating an RDD with words as keys and list of file names as values    
data_6 = data_5.map(lambda (x,y) : (x,unique_list(y)))

#Creating an RDD with first 50 lines of required output
data_7 = data_6.take(50)

#To print the contents of RDD
for i in data_7:
    print i