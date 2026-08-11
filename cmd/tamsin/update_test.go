package main

import "flag"

// update refreshes golden output embedded in the testscript archives.
var update = flag.Bool("update", false, "update testscript golden files")
