package repl

import (
	"bufio"
	"fmt"
	sql "github.com/horriblename/simpqle/sql"
	"log"
	"os"
)

const prompt string = "> "

func pErrorf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format, args...)
}

func Start() {
	table := &sql.Table{}
	scanner := bufio.NewScanner(os.Stdin)
	for {
		print(prompt)
		if scanned := scanner.Scan(); !scanned {
			if err := scanner.Err(); err != nil {
				log.Println(err)
				continue
			} else {
				// EOF
				break
			}
		}

		input := scanner.Text()

		if input == ".exit" {
			return
		}

		if len(input) >= 1 && input[0] == '.' {
			handleCmd(input)
		} else {
			stmt, err := sql.PrepareStmt(input)
			if err != nil {
				pErrorf("Error Parsing Statement: %s", err)
				continue
			}

			err = table.ExecuteStmt(&stmt)
			if err != nil {
				pErrorf("Error: %s", err)
			}
		}
	}
}

func handleCmd(cmd string) {
	switch cmd {
	default:
		pErrorf("Unknown command: %s\n", cmd)
	}
}
