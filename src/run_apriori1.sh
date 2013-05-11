echo "Cleaning up existing hadoop directories .."
hadoop dfs -rmr /user/hduser/mrapriori*

echo "Recreating input hadoop directory .."
hadoop dfs -copyFromLocal /home/hduser/hadoop/apriori/HadoopApriori/data /user/hduser/mrapriori-T10I4D100K

echo "Compiling the java classes .."
cd /home/hduser/hadoop/apriori/HadoopApriori/src/apriori

javac -classpath /home/hduser/hadoop/hadoop-core-1.0.4.jar -d ../../classes/ MRApriori.java ../model/Transaction.java ../model/ItemSet.java ../model/HashTreeNode.java ../utils/AprioriUtils.java ../utils/HashTreeUtils.java

echo "Creating the jar .."
jar -cvf ../../jars/mrapriori.jar -C ../../classes/ .

echo "Launching the hadoop job .."
cd ../../jars;
hadoop jar mrapriori.jar apriori.MRApriori /user/hduser/mrapriori-T10I4D100K /user/hduser/mrapriori-out- 4 2.0 98395
