#!/bin/sh

export JAVA_HOME=/usr/java/jdk1.8.0_25/jre
export CLASSPATH=%CLASSPATH%:`hadoop classpath`

JARS=`find ../lib -name '*.jar'`
OTHER_JARS=""

for jarinlib in $JARS ; do
OTHER_JARS=$jarinlib,$OTHER_JARS
case "$jarinlib" in
    *DppAsgment*)
    MAINJAR=$jarinlib
esac
done

echo $OTHER_JARS

nohup spark-submit --verbose --class com.test.dpp.EnronSparkProcessor \
--master yarn-client \
--num-executors 4 \
--driver-memory 4G \
--executor-cores 4 \
--jars $OTHER_JARS,$MAINJAR -Dfile.pattern=zip \
> ../../logs/dpp.err 2> ../../logs/dpp.log < /dev/null &

echo "--- started DPP ---"