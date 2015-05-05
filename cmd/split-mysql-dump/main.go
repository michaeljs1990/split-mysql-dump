package main

import (
	"fmt"
	"os"

	"github.com/michaeljs1990/split-mysql-dump"
)

func main() {
	output := splitmysqldump.Run(os.Args[1])

	for _, v := range output.Files {
		fmt.Println("Rendered File:", v)
	}
}
