package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
	"golang.org/x/crypto/ssh/terminal"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

func main() {
	err := cmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

var cmd = cobra.Command{
	Use:     "license-count FILE",
	Short:   "Collect statistics about keys and values in .jsonl files",
	Args:    cobra.ExactArgs(1),
	Version: "0.1.0",
	RunE:    runE,
}

var ErrJsonStats = errors.New("getting JSON statistics")

func runE(_ *cobra.Command, args []string) error {
	inPath := args[0]
	if ext := filepath.Ext(inPath); ext != ".jsonl" {
		return fmt.Errorf("%w: got file extension %q but want %q", ErrJsonStats, ext, ".jsonl")
	}

	file, err := os.Open(inPath)
	if err != nil {
		return fmt.Errorf("%w: opening %q: %w", ErrJsonStats, inPath, err)
	}

	reader := bufio.NewReader(file)

	stats, err := file.Stat()
	if err != nil {
		return fmt.Errorf("%w: reading stats of %q: %w", ErrJsonStats, inPath, err)
	}

	width, _, err := terminal.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return fmt.Errorf("%w: getting terminal size: %w", ErrJsonStats, err)
	}
	p := mpb.New(mpb.WithWidth(width))
	bar := p.AddBar(stats.Size(),
		mpb.PrependDecorators(decor.AverageSpeed(decor.UnitKiB, "%.1f")),
		mpb.AppendDecorators(decor.AverageETA(decor.ET_STYLE_GO)))

	keyValueSets := make(map[string]map[string]int)

	start := time.Now()
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		entry := make(map[string]interface{})
		err = json.Unmarshal(line, &entry)
		if err != nil {
			return err
		}

		err = AddKVs(".", entry, keyValueSets)
		if err != nil {
			return err
		}

		bar.IncrBy(len(line), time.Since(start))
	}

	var pkcs []PathKeyCount
	for path, keyCounts := range keyValueSets {
		fmt.Println(path)
		if len(keyCounts) > 100 {
			continue
		}
		for key, count := range keyCounts {
			pkcs = append(pkcs, PathKeyCount{Path: path, Key: key, Count: count})
		}
	}

	sort.Slice(pkcs, func(i, j int) bool {
		if pkcs[i].Path != pkcs[j].Path {
			return pkcs[i].Path < pkcs[j].Path
		}
		return pkcs[i].Count > pkcs[j].Count
	})

	for _, pkc := range pkcs {
		fmt.Printf("%s %q\t%d\n", pkc.Path, pkc.Key, pkc.Count)
	}

	return nil
}

type PathKeyCount struct {
	Path  string
	Key   string
	Count int
}

func AddKVs(path string, obj interface{}, kvs map[string]map[string]int) error {
	if len(kvs[path]) > 100 {
		return nil
	}

	pathCounts := kvs[path]
	if pathCounts == nil {
		pathCounts = make(map[string]int)
	}

	switch o := obj.(type) {
	case string:
		pathCounts[o]++
	case []interface{}:
		for _, v := range o {
			err := AddKVs(fmt.Sprintf("%s[]", path), v, kvs)
			if err != nil {
				return err
			}
		}
	case map[string]interface{}:
		for k, v := range o {
			err := AddKVs(fmt.Sprintf("%s%s.", path, k), v, kvs)
			if err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("%w: got unhandled type %T", ErrJsonStats, o)
	}

	kvs[path] = pathCounts

	return nil
}
