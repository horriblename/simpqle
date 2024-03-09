package repl

import (
	"bufio"
	"fmt"
	"log"
	"os"
)

const prompt string = "> "

func pErrorf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format, args...)
}

func Start() {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		print(prompt)
		if scanned := scanner.Scan(); !scanned {
			if err := scanner.Err(); err != nil {
				log.Println(err)
			} else {
				// EOF
				break
			}
		}

		input := scanner.Text()

		if input == ".exit" {
			return
		}

		if input[0] == '.' {
			handleCmd(input)
		} else {
			println(scanner.Text())
		}
	}
}

func handleCmd(cmd string) {
	switch cmd {
	default:
		pErrorf("Unknown command: %s\n", cmd)
	}
}
