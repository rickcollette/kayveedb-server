package cmd

import (
	"fmt"
)

// Version of KayVeeDB Server
const Version = "v1.2.0"

func ShowVersion() {
	fmt.Printf("KayVeeDB Server version: %s\n", Version)
}
