build:
	mvn package

run:
	FLINK_CONF_DIR=./conf flink run target/NWayJoiner-1.0-SNAPSHOT.jar 

clean:
	mvn clean
	rm -rf checkpoints/
	rm -rf savepoints/