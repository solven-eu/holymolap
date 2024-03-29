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
All data-structures are append-only. Removal can be done through masking (i.e. queries can be executed over a chain, where elements may be removals, hiding some templates from the rest of the chain).
We prefer consistency over performance: while we expect generally good performances, we prefer lower performances but easy monitoring of the result progress, support for cancellation, etc.