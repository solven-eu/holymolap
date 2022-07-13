# HolyMolap

A simple yet efficient library enabling aggregation queries over large sets of *things*

## Motivations

A free

## Connectivity with Excel

None

## Very large instances

HolyMolap shall be usable in production for very large instances. Such workloads may be loaded into NUMA hardware.

JDK14 enables G1 to be NUMA-aware: https://openjdk.org/jeps/345

## Architecture principles

All keys are Strings. Values may be anything (e.g. String, int, Number, double[], List, ...).
All public data-structures may be bigger than Integer.MAX_VALUE, but always smaller than Long.MAX_VALUE. Implementations would generally be partitionned.
