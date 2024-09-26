package extract_columns

import (
	"github.com/spf13/cobra"
	"os"
)

func main() {
	err := cmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

var cmd = cobra.Command{
	Use:     "extract-columns FILE",
	Short:   "extracts columns from a series of JSON objects",
	Args:    cobra.ExactArgs(2),
	Version: "0.1.0",
	RunE:    runE,
}

func runE(_ *cobra.Command, args []string) error {
	inPath := args[0]
	outDir := args[1]

	_ = inPath + outDir

	return nil
}
