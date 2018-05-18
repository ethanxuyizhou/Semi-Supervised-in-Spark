#!/usr/bin/env python
from __future__ import division
import argparse
import sys
from pyspark import SparkContext, SparkConf
from operator import add

def create_parser():
  parser = argparse.ArgumentParser()
  parser.add_argument('--iterations', type=int, default=2,
                      help='Number of iterations of label propagation')
  parser.add_argument('--edges_file', default=None,
                      help='Input file of edges')
  parser.add_argument('--seeds_file', default=None,
                      help='File that contains labels for seed nodes')
  parser.add_argument('--eval_file', default=None,
                      help='File that contains labels of nodes to be evaluated')
  parser.add_argument('--number_of_excutors', type=int, default=8,
                      help='Number of iterations of label propagation')
  return parser


class LabelPropagation:
    def __init__(self, graph_file, seed_file, eval_file, iterations, number_of_excutors):
        conf = SparkConf().setAppName("LabelPropagation")
        conf = conf.setMaster('local[%d]'% number_of_excutors)\
                 .set('spark.executor.memory', '3G')\
                 .set('spark.driver.memory', '3G')\
                 .set('spark.driver.maxResultSize', '3G')
        self.spark = SparkContext(conf=conf)
        self.graph_file = graph_file
        self.seed_file = seed_file
        self.eval_file = eval_file
        self.n_iterations = iterations
        self.n_partitions = number_of_excutors * 2


    def run(self):
        lines = self.spark.textFile(self.graph_file, self.n_partitions)

        def parseLines(a):
          node1, node2, weight = a.split('\t')
          return [(node1, (node2, int(weight))), (node2, (node1, int(weight)))]


        def seedParse(a):
          node1, label = a.split('\t')
          return (node1, label)

      
        def normalizeWeights(a,b):
            total = 0
            for (c, d) in b:
              total += d

            newB = []
            for (c,d) in b:
              newB += [(c,d/total)]
            return (a,newB)

        def getProbs(a, E, storageLabels, tellMe):

            d = []
            for i in range (len(storageLabels)):
              d += [0]
            (b,c) = E
            if (c is not None):
                d[tellMe[c]] = 1

            return (a, (b, c, d))


        def upper (a,b,c,d):
          newB = []
          for (key,weight) in b:
            newB += [(key, (weight, d))]

          return newB

        def updateMap(key, weight, d, storageLabels, tellMe):
          newD = []
          for i in range (len(d)):
            newD += [d[i]*weight]

          return (key, newD)
          

        def updateReduce(d1, d2, storageLabels, tellMe):
          newD = []

          for i in range (len(d1)):
            newD += [d1[i] + d2[i]]

          return newD

        def lineMap(a, bList, clas, dic, c, storageLabels, tellMe):

          if (c is None):
            return (a, bList, clas, dic)

          else:
            newDic = []

            for i in range (len(dic)):
              newDic += [dic[i] + c[i]]

            return (a, bList, clas, newDic)

        def normalizer(a, bList, clas, dic):
          total = 0
          for i in range (len(dic)):
            total += dic[i]

          if (total == 0): return (a, (bList, clas, dic))

          newDic = []
          for i in range (len(dic)):
            newDic += [dic[i]/total]

          return (a, (bList, clas, newDic))


        def rectify(a, bList, clas, dic, storageLabels, tellMe):
          if (clas is None): return (a, (bList, clas, dic))
          newDic = []
          for i in range (len(storageLabels)):
            newDic += [0]
          newDic[tellMe[clas]] = 1
          return (a, (bList, clas, newDic))


        lines = lines.flatMap(lambda a : parseLines(a))


        line = self.spark.textFile(self.seed_file, self.n_partitions)
        line = line.map(lambda a : seedParse(a))

        labels = line.map(lambda (node, label) : label).distinct()

        collectedLabels = labels.collect()


        storageLabels = []
        tellMe = dict()

        ite = 0
        for i in collectedLabels:
            storageLabels += [i.encode('utf-8')]
            tellMe[i.encode('utf-8')] = ite
            ite += 1

        newLines = lines.groupByKey()
        newLines = newLines.map(lambda (a,b) : normalizeWeights(a,b))

        supLines = newLines.leftOuterJoin(line)

        self.probLines = supLines.map(lambda (a,E) : getProbs(a,E, storageLabels, tellMe))

        # [TODO]

        for t in range(self.n_iterations):

            #propagate

            updateLines = self.probLines.flatMap(lambda (a,(b,c,d)) : upper(a,b,c,d))
                
          
            updateLines = updateLines.map(lambda (key,(weight, d)) : updateMap(key, weight, d, storageLabels, tellMe))            

            updateLines = updateLines.reduceByKey(lambda d1, d2 : updateReduce(d1, d2, storageLabels, tellMe))

            temLines = self.probLines.leftOuterJoin(updateLines)

            temLines = temLines.map(lambda (a, ((bList, clas, dic), c)) : lineMap(a, bList, clas, dic, c, storageLabels, tellMe)) 

            self.probLines = temLines.map(lambda (a, bList, clas, dic) : normalizer(a, bList, clas, dic))

            self.probLines = self.probLines.map(lambda (a, (bList, clas, dic)) : rectify(a, bList, clas, dic, storageLabels, tellMe))
           
        self.storageLabels = storageLabels
        self.tellMe = tellMe
        

    def eval(self):

        def evalParse(a):
            node = a.split('\t')[0]
            return node

        def printer(key, dic):
            newDict = dict()
            sorts = set()
            for i in range (len(dic)):
              if (not dic[i] in newDict): newDict[dic[i]] = [self.storageLabels[i]]
              else: newDict[dic[i]] += [self.storageLabels[i]] 

              sorts.add(dic[i])

            newSorts = list(sorts)
            newSorts = sorted(newSorts)


            outputstr = key
            
            for i in range (len(newSorts)-1, -1, -1):
              temp = newDict[newSorts[i]]

              temp = sorted(temp)
              for tem in temp:
                outputstr += '\t'
                outputstr += tem

            print outputstr

        def mapper(a,b,c,d):
            return (a.encode('utf-8'), (b,c,d))



        lines = self.spark.textFile(self.eval_file, self.n_partitions)

        lines = lines.map(lambda a : evalParse(a))

        lines = lines.map(lambda x : x.encode('utf-8'))

        self.probLines = self.probLines.map(lambda (a, (b,c,d)) : mapper(a,b,c,d))
        sup = self.probLines.collect()

        hugeDict = dict()
        for i in sup:
          (a, (b,c,d)) = i
          hugeDict[a] = d

        tha = lines.collect()
        for i in tha:
          supI = i
          printer(supI, hugeDict[supI])

        #newLines = lines.leftOuterJoin(self.probLines)

      
        #final = newLines.map(lambda (key, (dummy, j)) : finalMapper(key, j))

        #c = final.collect()
        #for i in c: print(i)

        # [TODO]



if __name__ == "__main__":
    args = create_parser().parse_args()
    lp = LabelPropagation(args.edges_file, args.seeds_file, args.eval_file, args.iterations, args.number_of_excutors)
    lp.run()
    lp.eval()
    lp.spark.stop()
