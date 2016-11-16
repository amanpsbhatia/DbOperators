JDKPATH = /usr
LIBPATH = lib/*

CLASSPATH = .:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac
JAVA = $(JDKPATH)/bin/java

PROGS = xx

all: $(PROGS)

compile:src/*/*.java
	$(JAVAC) -cp $(CLASSPATH) -d bin src/*/*.java

xx : compile
	$(JAVA) -cp $(CLASSPATH):bin tests.ROTest
	$(JAVA) -cp $(CLASSPATH):bin tests.QEPTest $(ARG)
