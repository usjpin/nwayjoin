build:
	mvn package

run1:
	FLINK_CONF_DIR=./conf flink run target/IntegerJoiner.jar

run2:
	FLINK_CONF_DIR=./conf flink run target/PurchaseAttributionJoiner.jar

clean:
	mvn clean
	rm -rf checkpoints/
	rm -rf savepoints/