# galcode2data

## Bring the code to the data for Galaxy experiments

### 1. Toward a Galaxy Autoflocker - data driven research BoF flocking

Proof of concept code to test out on galaxyproject postgresql databases - run locally and check outputs to avoid privacy concerns

These will run happily in jupyter notebooks!

galumds.py is ready to try - it should produce a pdf of all users in tool space - expect lots of clusters - BoF to be explored.
Script needs to be adjusted to replace the postgresql setup for Bjoern's docker images.
NCPU defaults to 2 for the mds calculation - more will make big runs faster but definitely not linear.

plotusermds.py was the development code
DEDENDRO should be false for anything big - even the fake data is a bit over the top.

Please let me see any outputs :)

