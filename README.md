riplog
======

A simple utility that evaluates complex queries against log files. Currently only supporting nginx.

Installation
------------

### Source

Clone this repo

    git clone git@github.com:gilbertw1/riplog.git
    
Build riplog:

    cargo build --release
  
Run riplog:

    ./target/release/riplog

Usage
-----

    riplog <file-or-dir> <query>


Query Syntax
------------

The basic elements are (each are optional):

    <FILTER(S)> | <GROUPINGS> | <SHOW> | <SORT> | <LIMIT>

Example:

    path = "/some/path" && method = "POST" && date > d"04-03-2019 15:27:42" | group ip | show count(*) | sort count(*) desc | limit 20
