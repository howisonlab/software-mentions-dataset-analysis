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
	"regexp"
	"sort"
	"time"
)

func main() {
	cmd.Flags().String("pattern", "", "suffix to match against file names")
	cmd.Flags().String("out", "", "output file path (default: stdout)")

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

func runE(cmd *cobra.Command, args []string) error {
	inPath := args[0]

	f, err := os.Stat(inPath)
	if err != nil {
		return fmt.Errorf("%w: stat %q: %w", ErrJsonStats, inPath, err)
	}

	keyValueSets := make(map[string]map[string]int)

	pattern, err := cmd.Flags().GetString("pattern")
	if err != nil {
		return err
	}

	width, _, err := terminal.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return fmt.Errorf("%w: getting terminal size: %w", ErrJsonStats, err)
	}
	p := mpb.New(mpb.WithWidth(width))

	if f.IsDir() {
		var matcher *regexp.Regexp
		if pattern != "" {
			matcher, err = regexp.Compile(pattern)
			if err != nil {
				return fmt.Errorf("%w: compiling pattern %q: %w", ErrJsonStats, pattern, err)
			}
		}
		err = processDirectory(inPath, p, 0, matcher, keyValueSets)
		if err != nil {
			return err
		}
	} else if filepath.Ext(inPath) == ".jsonl" {
		err = processJsonlFile(inPath, p, keyValueSets)
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("%w: file %q is neither a directory nor a .jsonl file", ErrJsonStats, inPath)
	}

	paths := make([]string, len(keyValueSets))
	i := 0
	for path := range keyValueSets {
		paths[i] = path
		i++
	}
	sort.Strings(paths)
	for _, path := range paths {
		fmt.Println(path)
	}

	var pkcs []PathKeyCount
	for path, keyCounts := range keyValueSets {
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

	outPath, err := cmd.Flags().GetString("out")
	if err != nil {
		return err
	}

	outFile := os.Stdout
	if outPath != "" {
		outFile, err = os.Create(outPath)
		if err != nil {
			return err
		}
	}

	for _, pkc := range pkcs {
		_, err = fmt.Fprintf(outFile, "%s;%q;%d\n", pkc.Path, pkc.Key, pkc.Count)
	}

	return nil
}

func processDirectory(inPath string, p *mpb.Progress, depth int, matcher *regexp.Regexp, keyValueSets map[string]map[string]int) error {
	names, err := os.ReadDir(inPath)
	if err != nil {
		return fmt.Errorf("%w: stat %q: %w", ErrJsonStats, inPath, err)
	}

	pathName := filepath.Base(inPath)

	var bar *mpb.Bar
	var now time.Time
	if depth < 2 {
		bar = p.AddBar(int64(len(names)),
			mpb.AppendDecorators(decor.AverageETA(decor.ET_STYLE_GO)),
			mpb.PrependDecorators(decor.Name(pathName)),
			mpb.PrependDecorators(decor.CountersNoUnit("%d/%d", decor.WCSyncSpace)),
			mpb.BarRemoveOnComplete())
		now = time.Now()
	}

	sort.Slice(names, func(i, j int) bool {
		return names[i].Name() < names[j].Name()
	})

	for _, name := range names {
		entryPath := filepath.Join(inPath, name.Name())
		if name.IsDir() {
			err = processDirectory(entryPath, p, depth+1, matcher, keyValueSets)
			if err != nil {
				return err
			}
		} else if matcher != nil {
			if matcher.MatchString(name.Name()) {
				err = processJsonFile(entryPath, keyValueSets)
				if err != nil {
					return err
				}
			}
		} else {
			err = processJsonFile(entryPath, keyValueSets)
			if err != nil {
				return err
			}
		}

		if bar != nil {
			bar.IncrBy(1, time.Since(now))
		}
	}

	return nil
}

func processJsonFile(inPath string, keyValueSets map[string]map[string]int) error {
	file, err := os.Open(inPath)
	if err != nil {
		return fmt.Errorf("%w: opening %q: %w", ErrJsonStats, inPath, err)
	}

	entry := make(map[string]interface{})
	err = json.NewDecoder(file).Decode(&entry)
	if err != nil {
		return fmt.Errorf("%w: decoding %q: %w", ErrJsonStats, inPath, err)
	}

	err = addKVs(".", entry, keyValueSets)
	if err != nil {
		return err
	}

	return nil
}

func processJsonlFile(inPath string, p *mpb.Progress, keyValueSets map[string]map[string]int) error {
	file, err := os.Open(inPath)
	if err != nil {
		return fmt.Errorf("%w: opening %q: %w", ErrJsonStats, inPath, err)
	}

	reader := bufio.NewReader(file)

	stats, err := file.Stat()
	if err != nil {
		return fmt.Errorf("%w: reading stats of %q: %w", ErrJsonStats, inPath, err)
	}

	bar := p.AddBar(stats.Size(),
		mpb.PrependDecorators(decor.AverageSpeed(decor.UnitKiB, "%.1f")),
		mpb.AppendDecorators(decor.AverageETA(decor.ET_STYLE_GO)),
		mpb.BarRemoveOnComplete())

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

		err = addKVs(".", entry, keyValueSets)
		if err != nil {
			return err
		}

		bar.IncrBy(len(line), time.Since(start))
	}
	return nil
}

type PathKeyCount struct {
	Path  string
	Key   string
	Count int
}

var ErrUnhandledType = errors.New("unhandled type")

func addKVs(path string, obj interface{}, kvs map[string]map[string]int) error {
	if len(kvs[path]) > 100 {
		return nil
	}

	pathCounts := kvs[path]
	if pathCounts == nil {
		pathCounts = make(map[string]int)
	}

	switch o := obj.(type) {
	case nil:
		// ignore
	case bool:
		if o {
			pathCounts["true"]++
		} else {
			pathCounts["false"]++
		}
	case string:
		pathCounts[o]++
	case float64:
		pathCounts[fmt.Sprintf("%f", o)]++
	case []interface{}:
		for _, v := range o {
			err := addKVs(fmt.Sprintf("%s[]", path), v, kvs)
			if err != nil {
				return err
			}
		}
	case map[string]interface{}:
		for k, v := range o {
			err := addKVs(fmt.Sprintf("%s%s.", path, k), v, kvs)
			if err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("%w: %T", ErrUnhandledType, o)
	}

	kvs[path] = pathCounts

	return nil
}
