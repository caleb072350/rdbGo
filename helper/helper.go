package helper

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/caleb072350/rdbGo/core"
	"github.com/caleb072350/rdbGo/model"
)

// ToJsons read rdb file and convert to json each line as redis object
func ToJsons(rdbFilename string, jsonFilename string) error {
	if rdbFilename == "" {
		return errors.New("src file path is required")
	}
	if jsonFilename == "" {
		return errors.New("output file path is required")
	}
	rdbFile, err := os.Open(rdbFilename)
	if err != nil {
		return fmt.Errorf("open rdb file %serror: %v", rdbFilename, err)
	}
	defer func() {
		_ = rdbFile.Close()
	}()

	jsonFile, err := os.Create(jsonFilename)
	if err != nil {
		return fmt.Errorf("create json file %s error: %v", jsonFilename, err)
	}
	defer func() {
		_ = jsonFile.Close()
	}()
	_, _ = jsonFile.WriteString("[\n")
	empty := true

	p := core.NewDecoder(rdbFile)
	err = p.Parse(func(object model.RedisObject) bool {
		data, err := json.Marshal(object)
		if err != nil {
			fmt.Printf("json marshal error: %v\n", err)
			return true
		}
		data = append(data, ',', '\n')
		_, err = jsonFile.Write(data)
		if err != nil {
			fmt.Printf("write json error: %v\n", err)
			return true
		}
		_, err = jsonFile.Write([]byte("\n"))
		if err != nil {
			fmt.Printf("write json error: %v\n", err)
			return true
		}
		empty = false
		return true
	})
	if err != nil {
		return err
	}
	if !empty {
		_, err = jsonFile.Seek(-3, 2)
		if err != nil {
			return fmt.Errorf("error during seek in file: %v", err)
		}
	}
	_, err = jsonFile.WriteString("\n]")
	if err != nil {
		return fmt.Errorf("write json error: %v", err)
	}
	return nil
}

// ToAOF read rdb file and convert to aof file (Redis Serialization)
func ToAOF(rdbFilename string, aofFilename string) error {
	if rdbFilename == "" {
		return errors.New("src file path is required")
	}
	if aofFilename == "" {
		return errors.New("output file path is required")
	}
	rdbFile, err := os.Open(rdbFilename)
	if err != nil {
		return fmt.Errorf("open rdb file %serror: %v", rdbFilename, err)
	}
	defer func() {
		_ = rdbFile.Close()
	}()
	aofFile, err := os.Create(aofFilename)
	if err != nil {
		return fmt.Errorf("create aof file %s error: %v", aofFilename, err)
	}
	defer func() {
		_ = aofFile.Close()
	}()
	p := core.NewDecoder(rdbFile)
	err = p.Parse(func(object model.RedisObject) bool {
		cmdLines := ObjectToCmd(object)
		data := CmdLinesToResp(cmdLines)
		_, err := aofFile.Write(data)
		if err != nil {
			fmt.Printf("write aof error: %v\n", err)
			return true
		}
		return true
	})
	return err
}
