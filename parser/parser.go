// RDB parser core
package parser

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"
)

const (
	opCodeIdle         = 248 /* LRU idle time. */
	opCodeFreq         = 249 /* LFU frequency. */
	opCodeAux          = 250 /* RDB aux field. */
	opCodeResizeDB     = 251 /* Hash table resize hint. */
	opCodeExpireTimeMs = 252 /* Expire time in milliseconds. */
	opCodeExpireTime   = 253 /* Old expire time in seconds. */
	opCodeSelectDB     = 254 /* DB number of the following keys. */
	opCodeEOF          = 255
)

const (
	typeString = iota
	typeList
	typeSet
	typeZset
	typeHash
	typeZset2 /* ZSET version 2 with doubles stored in binary. */
	typeModule
	typeModule2 // Module should import module entity, not support at present.
	_
	typeHashZipMap
	typeListZipList
	typeSetIntSet
	typeZsetZipList
	typeHashZipList
	typeListQuickList
	typeStreamListPacks
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

func (p *Parser) parse(cb func(object RedisObject) bool) error {
	var dbIndex int
	var expireMs int64
	for {
		b, err := p.input.ReadByte()
		if err != nil {
			return nil
		}
		if b == opCodeEOF {
			break
		} else if b == opCodeSelectDB {
			dbIndex64, _, err := p.readLength()
			if err != nil {
				return err
			}
			dbIndex = int(dbIndex64)
			continue
		} else if b == opCodeExpireTime {
			_, err = io.ReadFull(p.input, p.buffer)
			if err != nil {
				return err
			}
			expireMs = int64(binary.LittleEndian.Uint64(p.buffer)) * 1000
			continue
		} else if b == opCodeExpireTimeMs {
			_, err = io.ReadFull(p.input, p.buffer)
			if err != nil {
				return err
			}
			expireMs = int64(binary.LittleEndian.Uint64(p.buffer))
			continue
		} else if b == opCodeResizeDB {
			_, _, err := p.readLength()
			if err != nil {
				return err
			}
			_, _, err = p.readLength()
			if err != nil {
				err = errors.New("Parse dbsize value failed: " + err.Error())
				break
			}
			// todo
			continue
		} else if b == opCodeFreq {
			_, err = p.input.ReadByte()
			if err != nil {
				return err
			}
		} else if b == opCodeIdle {
			_, _, err = p.readLength()
			if err != nil {
				return err
			}
		}
		key, err := p.readString()
		if err != nil {
			return err
		}
		base := &BaseObject{
			DB:  dbIndex,
			Key: string(key),
		}
		if expireMs > 0 {
			expiration := time.Unix(0, expireMs*int64(time.Millisecond))
			base.Expiration = &expiration
		}
		obj, err := p.readObject(b, base)
		if err != nil {
			return err
		}
		toBeContinued := cb(obj)
		if !toBeContinued {
			break
		}
	}
	return nil
}

func (p *Parser) readObject(flag byte, base *BaseObject) (RedisObject, error) {
	switch flag {
	case typeString:
		bs, err := p.readString()
		if err != nil {
			return nil, err
		}
		return &StringObject{BaseObject: base, Value: bs}, nil
	case typeList:
		list, err := p.readList()
		if err != nil {
			return nil, err
		}
		return &ListObject{BaseObject: base, Values: list}, nil
	case typeSet:
		set, err := p.readSet()
		if err != nil {
			return nil, err
		}
		return &SetObject{BaseObject: base, Members: set}, nil
	case typeHash:
		hash, err := p.readHashMap()
		if err != nil {
			return nil, err
		}
		return &HashObject{BaseObject: base, Hash: hash}, nil
	}
	return nil, fmt.Errorf("unknown type: %b", flag)
}

// checkHeader checks whether input has valid RDB file header
func (p *Parser) checkHeader() error {
	header := make([]byte, 9)
	_, err := io.ReadFull(p.input, header)
	if err == io.EOF {
		return errors.New("empty file")
	}
	if err != nil {
		return fmt.Errorf("io error: %v", err)
	}
	if !bytes.Equal(header[0:5], magicNumber) {
		return errors.New("file is not a RDB file")
	}
	version, err := strconv.Atoi(string(header[5:]))
	if err != nil {
		return fmt.Errorf("%s is not valid version number", string(header[5:]))
	}
	if version < minVersion || version > maxVersion {
		return fmt.Errorf("cannot parse version: %d", version)
	}
	return nil
}

// Parse parses rdb and callback
func (p *Parser) Parse(cb func(object RedisObject) bool) error {
	err := p.checkHeader()
	if err != nil {
		return err
	}
	return p.parse(cb)
}
