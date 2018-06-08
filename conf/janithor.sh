#!/bin/bash

#export JAVA_DEBUG="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5009"
export LOG_4_JAVA="-Dlog4j.configuration=file:./log4j.xml"

java -cp "lib/*" $LOG_4_JAVA $JAVA_DEBUG com.stratio.mesos.Janithor $@
