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
	fmt.Fprintf(os.Stderr, format+"\n", args...)
}

func Start(dbFilePath string) {
	table, err := sql.DbOpen(dbFilePath)
	if err != nil {
		panic(fmt.Sprintf("Error opening DB: %s", err))
	}
	defer func() {
		err := table.Close()
		if err != nil {
			pErrorf("error closing database: %s", err)
		}
	}()

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

		if len(input) >= 1 && input[0] == '.' {
			if err := sql.DoMetaCommand(input, table); err != nil {
				if err == sql.ExitCmd {
					return
				}

				pErrorf("%s", err)
			}
			continue
		}

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
