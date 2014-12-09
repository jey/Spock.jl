CLASSPATH := lib/spark.jar:lib/commons-io-2.4.jar:lib/scala-library-2.10.4.jar

check: spock.jar
	julia test/runtests.jl

spock.jar: $(shell find src/ -name \*.java)
	javac -cp spark.jar:$(CLASSPATH) $^
	cd src && find . -name \*.class -print0 | xargs -0 jar cf ../$@
