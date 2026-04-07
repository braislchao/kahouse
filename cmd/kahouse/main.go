package main

import (
	"flag"

	"github.com/braislchao/kahouse/internal/app"
)

func main() {
	flag.Parse()
	app.Run()
}
