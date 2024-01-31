# Furness via Python

Python process to build the shortest paths between bus node in the Belfast network.

## Instructions

Inputs files are:
- body_processing.csv = series of bus services with node numbers which will be connected by a set of links in the model.
- N6_Distances_AM.csv = list of Start/End nodes with associated distances. It is used to pick up the shortest connection between the nodes.

The script considers by default three paths maximum before selecting the shortest path, but this number can be editing at the beginning of the script in the "TO EDIT" section in the code (line 41).

Once the input files are checked, simply run PT_lines_Bus.py.

## Process

The script builds the shortest paths between the start and end nodes in the sequence. It calculates the total distance of each path and selects the shortest path.
Note that the most direct path between the start and end node will be the most likely candidate for the bus route joining the two stops.
Once the script has selected the shortest path, it stores it in a dictionary so that the process does not have to be executed another time the next time the same pair of nodes is met. The script then inserts the intermediate nodes between the start and end nodes in the output file and identifies the intermediate nodes as “0” in the STOP column.

The most recent version of the script includes two main improvements which are:
-	The introduction of two python dictionaries: one that stores the different paths and another one that stores the selected shortest path for each pair of nodes. The two dictionaries significantly improve the performance of the script and allow the script to print the shortest path without having to repeat the process when a pair of nodes is repeated several times in the sequence.
-	A timer for each pair of nodes: It is set to thirty seconds and prevents the script from looping over a long period of time when no more than one or two paths can be found for a pair of nodes. After 30 seconds, the script will consider the paths found (even it is less than 3 in the dictonnary) and will select the shortest one.
-	A check at the beginning of the process to ensure that all the bus nodes in the series are present in the list.

## Output

The script exports 5 output files: 
-	Output_ALL: an output file that includes all the data
-	Output_AM: The AM data (between 0700-0959)
-	Output_IP: The IP data (between 1000-1559)
-	Output_PM: The PM data (between 1600-1859)
-	Output_OP: The OP data (between 1900-0659)

## Current limitations

No known limitations currently.

## Prerequisites

```
Python 3
```

## Versioning

| Version | Originator | Date       | Comments             |
|  ------ | ---------- | ---------- | -------------------- |
|  v0.3   | CS         | 08/02/2023 | most recent version  |


## Maintainers

* Charlotte Schnoebelen - initial point of contact

