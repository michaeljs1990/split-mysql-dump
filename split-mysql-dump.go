// Package splitmysqldump was copied from
// github.com/ripienaar/mysql-dump-split
// but it's in ruby and I don't like ruby
// plus it's a pain to run the ruby command
// and get a nice list of files parsed so I
// can run it in multiple goroutines
package splitmysqldump

import (
	"bufio"
	"bytes"
	"os"
	"strings"
)

// ParsedFiles contains all the
// files that were created when
// running the Run function
type ParsedFiles struct {
	Files []string
}

// NewParsedFiles handles the work of
// creating a ParsedFiles struct
func NewParsedFiles() *ParsedFiles {
	return &ParsedFiles{Files: make([]string, 0)}
}

// Run is the function that is used
// to bind all of the other functions
// together
func Run(file string) *ParsedFiles {
	return findTables(openFile(file))
}

// openFile takes a string and trys to open
// it based on the current directory. If the
// current file is not availabel for opening
// panic since we can't do anything with the file
func openFile(file string) *os.File {
	f, err := os.Open(file)
	if err != nil {
		panic(err)
	}

	return f
}

// bufio.ReadLine does not gurantee that
// the entire line is returned to ensure
// that we do not break anything when parsing
// the file this ensures entire lines are
// returned when it is called
func getFullLine(r *bufio.Reader) []byte {
	var isPrefix = true
	var err error
	var line []byte
	var buffer bytes.Buffer

	for isPrefix && err == nil {
		line, isPrefix, err = r.ReadLine()
		buffer.Write(line)
	}

	return buffer.Bytes()
}

func makeNewFile(file string) *os.File {
	f, err := os.Create(file)
	if err != nil {
		panic(err)
	}

	return f
}

// Parse the regex above and return only the
// name of the table that we are parsing
func getTableName(name []byte) string {
	sections := strings.Split(string(name), "`")
	return sections[1]
}

// findTables parses the passed in file
func findTables(file *os.File) *ParsedFiles {
	var tableFile *os.File
	var buffer bytes.Buffer
	createdFiles := NewParsedFiles()
	inTable := false

	reader := bufio.NewReader(file)
	// Buffered makes sure we have more bytes
	// that can be read.
	for _, err := reader.Peek(1); err == nil; {
		line := getFullLine(reader)

		if contains := bytes.Contains(line, []byte("-- Table structure for table")); contains {
			if inTable {
				tableFile.Close()
			}
			tableName := getTableName(line)
			createdFiles.Files = append(createdFiles.Files, tableName)
			tableFile = makeNewFile(tableName)
			inTable = true
		}

		buffer.Write(line)
		buffer.Write([]byte("\n"))

		tableFile.Write(buffer.Bytes())
		buffer.Reset()

		_, err = reader.Peek(1)
	}

	return createdFiles
}
