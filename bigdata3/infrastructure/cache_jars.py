"""Resolve connectors against the Hadoop/Scala actually bundled with Spark."""
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess

import pyspark

spark_home = Path(pyspark.__file__).parent
jars = spark_home / 'jars'
hadoop = next(jars.glob('hadoop-client-runtime-*.jar'))
hadoop_version = re.fullmatch(r'hadoop-client-runtime-(.+)\.jar', hadoop.name).group(1)
scala = next(jars.glob('scala-library-*.jar'))
scala_version = re.fullmatch(r'scala-library-(.+)\.jar', scala.name).group(1)
scala_binary = '.'.join(scala_version.split('.')[:2])
coordinates = f'io.delta:delta-spark_{scala_binary}:4.0.1,org.apache.hadoop:hadoop-aws:{hadoop_version}'
probe = Path('/tmp/lab3_cache_probe.py')
probe.write_text('from pyspark.sql import SparkSession\n'
                 's=SparkSession.builder.getOrCreate()\n'
                 'assert s.range(1).collect()[0][0] == 0\n'
                 's.stop()\n', encoding='utf-8')
subprocess.run([
    str(spark_home / 'bin/spark-submit'), '--master', 'local[1]',
    '--packages', coordinates,
    '--repositories', 'https://repo.maven.apache.org/maven2',
    '--conf', 'spark.jars.ivy=/opt/lab3/ivy',
    '--conf', 'spark.driver.host=127.0.0.1',
    str(probe),
], check=True)
resolved = sorted(Path('/opt/lab3/ivy/jars').glob('*.jar'))
if not resolved:
    raise RuntimeError('No connector JARs resolved')
lock = []
for artifact in resolved:
    filename = artifact.name.split('_', 1)[1]
    destination = jars / filename
    if not destination.exists():
        shutil.copy2(artifact, destination)
    lock.append({'file': filename, 'sha256': hashlib.sha256(destination.read_bytes()).hexdigest()})
Path('/opt/lab3/jars.lock.json').write_text(json.dumps({
    'hadoop': hadoop_version, 'scala': scala_version,
    'coordinates': coordinates, 'jars': lock,
}, indent=2) + '\n', encoding='utf-8')
print('Connectors cached for Hadoop', hadoop_version, 'and Scala', scala_binary)
