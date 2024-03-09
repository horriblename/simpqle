package repl

import (
	"bufio"
	"log"
	"os"
)

const prompt string = "> "

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

		println(scanner.Text())
	}
}
