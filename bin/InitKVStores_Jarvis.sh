java -Dlog4j.configuration=file:$KAMANJA_HOME/config/log4j.properties -jar $KAMANJA_HOME/bin/KVInit-1.0 --typename System.Jarvis         --config $KAMANJA_HOME/config/Engine1Config.properties --datafiles /Users/dhavalkolapkar/Documents/Workspace/jarvis/data/jarvis_data.csv      --keyfields id --delimiter "," --ignorerecords 1 --format "delimited"
