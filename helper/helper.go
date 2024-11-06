package helper

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/caleb072350/rdbGo/core"
	"github.com/caleb072350/rdbGo/model"
)

// ToJsons read rdb file and convert to json each line as redis object
func ToJsons(rdbFilename string, jsonFilename string) error {
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
