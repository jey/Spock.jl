BASE_CLASSPATH := lib/spark.jar
BUILD_CLASSPATH := $(BASE_CLASSPATH)
#BUILD_CLASSPATH := $(BUILD_CLASSPATH):lib/scala-library-2.10.4.jar
RUNTIME_CLASSPATH := conf:$(BASE_CLASSPATH):spock.jar

check: spock.jar
	CLASSPATH=$(RUNTIME_CLASSPATH) julia test/runtests.jl 2> stderr.log

spock.jar: $(shell find src/ -name \*.java)
	mkdir -p bin
	javac -Xlint -d bin -cp $(BUILD_CLASSPATH) $^
	jar cf spock.jar -C bin .

.PHONY: clean
clean:
	rm -rf bin spock.jar
