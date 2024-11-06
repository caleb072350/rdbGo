package main

import (
	"flag"
	"fmt"

	"github.com/caleb072350/rdbGo/helper"
)

func main() {
	var cmd string
	var output string
	flag.StringVar(&cmd, "c", "", "command for rdb: json")
	flag.StringVar(&output, "o", "", "output file path")
	flag.Parse()
	src := flag.Arg(0)
	switch cmd {
	case "json":
		if src == "" {
			println("src file is required")
			return
		}
		if output == "" {
			println("output file is required")
			return
		}
		err := helper.ToJsons(src, output)
		if err != nil {
			fmt.Printf("error: %v\n", err)
			return
		}
	case "memory":
		{
			if src == "" {
				println("src file is required")
				return
			}
			if output == "" {
				println("output file is required")
				return
			}
			err := helper.MemoryProfile(src, output)
			if err != nil {
				fmt.Printf("error: %v\n", err)
				return
			}
		}
	default:
		println("unknown command")
	}
}
