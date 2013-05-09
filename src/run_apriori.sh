echo "Cleaning up existing hadoop directories .."
hadoop dfs -rmr /user/hduser/mrapriori-out*

echo "Compiling the java classes .."
cd /home/hduser/workspace/CS764/src/apriori

javac -classpath /home/hduser/hadoop/hadoop-core-0.20.1.jar -d ../../classes/ MRApriori.java ../model/Transaction.java ../model/ItemSet.java ../model/HashTreeNode.java ../utils/AprioriUtils.java ../utils/HashTreeUtils.java

echo "Creating the jar .."
jar -cvf ../../jars/mrapriori.jar -C ../../classes/ .

echo "Launching the hadoop job .."
cd ../../jars;
hadoop jar mrapriori.jar apriori.MRApriori
