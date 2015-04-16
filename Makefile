BUILD_CLASSPATH := lib/spark.jar:$(CLASSPATH)

check: lib/spock.jar
	julia test/runtests.jl 2> stderr.log

lib/spock.jar: $(shell find src/ -name \*.java)
	mkdir -p build
	javac -Xlint -d build -cp $(BUILD_CLASSPATH) $^
	jar cf lib/spock.jar -C build .

.PHONY: clean
clean:
	rm -rf build lib/spock.jar
