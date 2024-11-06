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

	p := core.NewDecoder(rdbFile)
	return p.Parse(func(object model.RedisObject) bool {
		data, err := json.Marshal(object)
		if err != nil {
			fmt.Printf("json marshal error: %v\n", err)
			return true
		}
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
		return true
	})
}
