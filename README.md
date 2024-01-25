# Centrality-Algorithms
Public repository for centrality algorithms based on subgraph counting

## Source code and testing

### Requirements

1. Create a python enviroment with Python >= 3.8
2. Install requirements with `python pip install -r requirements.txt`

### Overview

We provide two implementations of the exact approach to compute ll-Subgraphs and All-Trees centrality measures in a network.
1. Non parallelized version
2. Parallelized version
The latter operates distributing the computation in a multicore machine.

### How to run tests

To run any of the implementations of the All-Subgraphs and All-Trees centrality algorithms, you can run the following call with `pwd` in this README directory
```
python main.py -i <path/to/input/folder> -o <path/to/output/folder> -c <number-of-cores> -p -t
```
The parameters are detailed as follows
* `-i` (mandatory) is the path to the folder where `.edges` and `.mtx` files are located. We support only this two formats for networks.
* `-o` (mandatory) is the path to the output folder, where a `.txt` file will be created for each network in the input folder. Each line of these output files will contain a pair `label,centrality_value`. The name of the output files replaces the extension of the input file with `.txt`. When the `-t` flag is specified, the output file will also include the substring `_trees`.
* `-c` is the number of cores to be used for the parallel algorithm
* `-p` is a flag that indicates to run the parallel version of the algorithm
* `-t` is a flag that indicates to compute All-Trees instead of All-Subgraphs centrality.
