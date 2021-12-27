# GRainDB

GRainDB is a relation-graph database that extends DuckDB to support seamlessly querying of graph and tables.

### Requirements

GRainDB requires [CMake](https://cmake.org) (version >= 2.8.12) to be installed and a `C++11` compliant compiler.

#### Compiling

Run `make` in the root directory to compile the sources. For development, use `make debug` to build a non-optimized
debug version.

### Restful Server

After compiling, run `./build/release/tools/rest/duckdb_rest_server` in the projection root directory
to start it in the default mode.

### Web Server
Please see the [graindb-web](https://github.com/nafisaanzum13/graindb-web) repo.

### Contact

- Guodong Jin (jinguodong@ruc.edu.cn)
- Nafisa Anzum (nanzum@uwaterloo.ca)
