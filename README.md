# galcode2data

## Bring the code to the data for Galaxy experiments

### Toward a Galaxy Autoflocker - data driven BoF flocking

Users who share tool usage patterns may have common research interests. For example, regular users of the GATK toolkit, are likely to be interested in sequence variation,
making them potential Birds of a Feather perhaps.

Similarly, usage patterns for other tools and toolkits may help distinguish them from users more interested in (e.g.) microRNA, Sars-cov2, or climate
science.

This repository is the starting point for an experimental BoF auto-flocker.

First step is to see if there is any useful structure in the data. That can be done by "seeing" to what extent users form distinct clusters in a multidimensional mathematical "space"
derived from their individual tool frequency patterns. A method like Multi-Dimensional Scaling (MDS) is a good start. It needs more or less
"idenfiable" individual user tool usage data, so sending the code to each individual site is the only way we are ever likely to be able to run it.

The idea is that a server admin might agree to run some code after taking a close look to make sure it is not malicious.
That code is produces pdf plots only - images - so no way to identify any of the individual dots. The admin would run the code, check that the images
are harmless and return them to the requester. All the identifiable data remains secured.

Proof of concept is **galumds.py** - ready to try - it has been tested only on a very small table, but it *should* produce a pdf of all users in tool space - expect lots of clusters - BoF to be explored.
Script needs to be adjusted to replace the postgresql credentials - currently set for Bjoern's docker images.
Start and end dates should be constrained on large sites - no idea how long it will run - MDS calculation time really bogs down for very large problems.
NCPU defaults to 2 for the mds calculation - more cpus will improve run times but not likely to be linear.

plotusermds.py was the development code and has a test data generator.
DODENDRO should be false for any data with more than 50-100 users - even with the fake data, the output is not very informative.

Please let me see any outputs :)

### Note to self:

In SQL, it seems easier to generate *long* data, but the pivot capability in postgres is ugly, painful and apparently version dependent.

Trivial in pandas. To convert the "long" output from a simple SQL query into a tool usage count table with users as rows
and tools as columns requires:

> wjobs = jobs.pivot(index='user_id', columns='tool_id', values='nruns')

