package main

import (
	"compress/gzip"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/willbeason/bondsmith/jsonio"
	"github.com/willbeason/software-mentions/pkg/jsonl"
	"io"
	"os"
	"strings"
)

func main() {
	err := cmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

var cmd = cobra.Command{
	Use:     "sort-jsonl FILE",
	Short:   "sort entries in a JSONL file by their UUID",
	Args:    cobra.ExactArgs(1),
	Version: "0.1.0",
	RunE:    runE,
}

func runE(_ *cobra.Command, args []string) error {
	inPath := args[0]

	return sortFile(inPath)
}

func sortFile(inPath string) error {
	file, err := os.Open(inPath)
	if err != nil {
		return fmt.Errorf("opening %q: %w", inPath, err)
	}

	var reader io.Reader = file
	if strings.HasSuffix(inPath, ".gz") {
		reader, err = gzip.NewReader(reader)
		if err != nil {
			return fmt.Errorf("starting gzip reader stream for %q: %w", inPath, err)
		}
	}

	entries := jsonio.NewReader(reader, func() *map[string]any {
		v := make(map[string]any)
		return &v
	})

	sorted, err := jsonl.Sort(entries.Read())
	if err != nil {
		return err
	}

	outFile, err := os.Create(inPath + ".new")
	if err != nil {
		return err
	}

	defer func() {
		err = outFile.Close()
		if err != nil {
			fmt.Println(err)
		}
	}()

	writer := gzip.NewWriter(outFile)
	defer func() {
		err = writer.Close()
		if err != nil {
			fmt.Println(err)
		}
	}()
	defer func() {
		err = writer.Close()
		if err != nil {
			fmt.Println(err)
		}
	}()

	jsonWriter := jsonio.NewWriter(writer, sorted)
	return jsonWriter.Write()
}
