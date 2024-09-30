package main

import (
	"compress/gzip"
	"errors"
	"fmt"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/compress"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	"github.com/spf13/cobra"
	bondsmith_io "github.com/willbeason/bondsmith-io"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	err := cmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

var cmd = cobra.Command{
	Use:     "extract-columns [papers|software] IN_DIR OUT_DIR",
	Short:   "converts parts of the dataset into the Apache Parquet format",
	Args:    cobra.ExactArgs(3),
	Version: "0.1.0",
	RunE:    runE,
}

func runE(_ *cobra.Command, args []string) error {
	if args[0] != "papers" {
		return fmt.Errorf("only papers is supported")
	}
	inPath := args[1]
	outDir := args[2]

	inFile, err := os.Open(inPath)
	if err != nil {
		return err
	}
	defer func() {
		err = inFile.Close()
		if err != nil {
			fmt.Println(err)
		}
	}()

	var reader io.Reader = inFile
	if strings.HasSuffix(inPath, ".gz") {
		reader, err = gzip.NewReader(reader)
		if err != nil {
			return err
		}
	}

	papers := bondsmith_io.NewJsonReader(reader, func() *Paper {
		return &Paper{}
	})

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "uuid", Type: arrow.BinaryTypes.String},
		{Name: "year", Type: arrow.PrimitiveTypes.Uint16},
	}, nil)

	outPath := filepath.Join(outDir, "papers.parquet")
	outFile, err := os.Create(outPath)
	if err != nil {
		return err
	}
	// Don't close outFile; parquet handles closing it.

	allocator := memory.NewGoAllocator()
	paperRecordBuilder := array.NewRecordBuilder(allocator, schema)
	defer paperRecordBuilder.Release()

	for paper, err := range papers.Read() {
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}
			break
		}

		paperRecordBuilder.Field(0).(*array.StringBuilder).
			Append(paper.File[:36])
		paperRecordBuilder.Field(1).(*array.Uint16Builder).
			Append(paper.Year)
	}

	record := paperRecordBuilder.NewRecord()
	defer record.Release()

	writer, err := pqarrow.NewFileWriter(
		schema,
		outFile,
		parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Gzip)),
		pqarrow.DefaultWriterProps(),
	)
	if err != nil {
		return err
	}
	defer func() {
		err = writer.Close()
		if err != nil {
			fmt.Println(err)
		}
	}()

	err = writer.Write(record)
	if err != nil {
		return err
	}

	return nil
}

type Paper struct {
	File string `json:"file"`
	Year uint16 `json:"year"`
}
