// Command gphoto-sync mirrors Google Photos Takeout exports into a local library.
package main

import (
	"os"
	_ "time/tzdata" // embed zone data for the distroless image

	"gphoto-sync/internal/cli"
)

var version = "dev"

func main() {
	os.Exit(cli.Main(os.Args[1:], version))
}
