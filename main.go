package main

import (
	"flag"
	repl "github.com/horriblename/simpqle/repl"
)

func main() {
	dbFilePath := flag.String("db_file", ".db", "Path to the db file")
	repl.Start(*dbFilePath)
}
