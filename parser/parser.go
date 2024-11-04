// RDB parser core
package parser

import (
	"bufio"
	"io"
)

type Parser struct {
	input  *bufio.Reader
	buffer []byte
}

func NewParser(reader io.Reader) *Parser {
	parser := new(Parser)
	parser.input = bufio.NewReader(reader)
	parser.buffer = make([]byte, 8)
	return parser
}

var magicNumber = []byte("REDIS")

const (
	minVersion = 1
	maxVersion = 9
)
