Spark & Jupyter Notebook
============

Data exploration without visualisation or tools to interactively explore the data
is no fun. There exists several way of mixing Spark with notebook. Among those
[Zeppelin](https://zeppelin.apache.org/) and Jupyter Notebook
with the [Apache Toree](https://toree.incubator.apache.org/) kernel are great.

Installing Apache Toree as kernel in Jupyter
============

The easiest way to install the Toree kernel is

```bash
## To see the list of available kernels
$ jupyter kernelspec list
## Download and install the kernel on your machine
$ pip install toree
## SPARK_HOME must be defined!
$ jupyter toree install --spark_home=${SPARK_HOME}
```

However, if you use Spark v2+ and/or Scala v2.11+, you might run into trouble

```bash
julien$ jupyter notebook
...
[I 15:55:23.722 NotebookApp] Kernel started: ...
Starting Spark Kernel with SPARK_HOME=...
log4j:WARN No appenders could be found for logger (org.apache.toree.Main$$anon$1).
log4j:WARN Please initialize the log4j system properly.
log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
Exception in thread "main" java.lang.NoSuchMethodError: scala.collection.immutable.HashSet$.empty()Lscala/collection/immutable/HashSet;
	at akka.actor.ActorCell$.<init>(ActorCell.scala:336)
	at akka.actor.ActorCell$.<clinit>(ActorCell.scala)
	at akka.actor.RootActorPath.$div(ActorPath.scala:185)
	at akka.actor.LocalActorRefProvider.<init>(ActorRefProvider.scala:465)
	at akka.actor.LocalActorRefProvider.<init>(ActorRefProvider.scala:453)
	at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
	at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
	at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
	at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
	at akka.actor.ReflectiveDynamicAccess$$anonfun$createInstanceFor$2.apply(DynamicAccess.scala:78)
	at scala.util.Try$.apply(Try.scala:192)
```

This is a known issue as of mid-2018 (e.g. [TOREE-354](https://issues.apache.org/jira/browse/TOREE-354) and [TOREE-336](https://issues.apache.org/jira/browse/TOREE-336)), and the way
to solve it is to install the latest SNAPSHOT version (not the default release):

```bash
pip install https://dist.apache.org/repos/dist/dev/incubator/toree/0.2.0/snapshots/dev1/toree-pip/toree-0.2.0.dev1.tar.gz
jupyter toree install --spark_home=${SPARK_HOME}
```

Once done, just launch a notebook `jupyter notebook` and choose the kernel Toree.
You can also play with the provided examples (including visualisation using [smile](https://github.com/haifengl/smile)!).
