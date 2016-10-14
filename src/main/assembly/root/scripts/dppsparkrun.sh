#!/bin/sh

export JAVA_HOME=/opt/jdk1.8.0_101/jre

#export JRE_HOME=$JAVA_HOME/jre

export PATH=JAVA_HOME/bin:$PATH

export CLASSPATH=$CLASSPATH:`hadoop classpath`

JARS=`find ../lib -name '*.jar'`
OTHER_JARS=""

for jarinlib in $JARS ; do
OTHER_JARS=$jarinlib,$OTHER_JARS
case "$jarinlib" in
    *DppAsgment*)
    MAINJAR=$jarinlib
esac
done

echo "OTHER_JARS-----------------------------------"
echo $OTHER_JARS

echo "CLASSPATH-------------------------------------------"
echo $CLASSPATH

echo "PATH---------------------------------------"
echo $PATH

spark-submit --verbose --class com.test.dpp.EnronSparkProcessor \
--master yarn-client \
--num-executors 4 \
--driver-memory 2G \
--executor-cores 4 \
--conf "spark.executor.extraClassPath=$JAVA_HOME/bin:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/lib/hadoop/hadoop-aws.jar:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*:/usr/share/aws/emr/security/conf:/usr/share/aws/emr/security/lib/*" \
--conf "spark.driver.extraClassPath=$JAVA_HOME/bin:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/lib/hadoop/hadoop-aws.jar:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*:/usr/share/aws/emr/security/conf:/usr/share/aws/emr/security/lib/*" \
--jars $OTHER_JARS,$MAINJAR \
10 "$1" -DprocEnron

echo "--- started DPP ---"

echo "--- started DPP ---"

