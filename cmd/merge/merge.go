package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
	"golang.org/x/crypto/ssh/terminal"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

func main() {
	cmd.Flags().String("mention-counts", "", "file to write mention counts to")

	err := cmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

var cmd = cobra.Command{
	Use:     "merge IN OUT",
	Short:   "Merge JSON files in directory into .jsonl.bzip2 files",
	Args:    cobra.ExactArgs(2),
	Version: "0.1.0",
	RunE:    runE,
}

var ErrCompress = fmt.Errorf("compressing mentions")

func runE(_ *cobra.Command, args []string) error {
	inDir := args[0]
	outDir := args[1]

	err := os.MkdirAll(outDir, os.ModePerm)
	if err != nil {
		return err
	}

	err = ProcessDir(inDir, outDir)
	if err != nil {
		return err
	}

	return nil
}

const UUIDPattern = `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`

var (
	PaperPattern    = regexp.MustCompile(UUIDPattern + `\.json$`)
	SoftwarePattern = regexp.MustCompile(UUIDPattern + `\.software\.json$`)
)

func ProcessDir(inDir, outDir string) error {
	entries, err := os.ReadDir(inDir)
	if err != nil {
		return err
	}

	width, _, err := terminal.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return fmt.Errorf("%w: getting terminal size: %w", ErrCompress, err)
	}
	p := mpb.New(mpb.WithWidth(width))
	nTotal := int64(len(entries))
	bar := p.AddBar(nTotal,
		mpb.AppendDecorators(decor.AverageETA(decor.ET_STYLE_HHMMSS)),
		mpb.PrependDecorators(decor.CountersNoUnit("%3d/%3d", decor.WCSyncSpace)),
		mpb.BarRemoveOnComplete())

	start := time.Now()
	for _, entry := range entries {
		err = processEntry2(p, inDir, outDir, entry)
		if err != nil {
			return err
		}

		bar.IncrBy(1, time.Since(start))
	}

	return nil
}

func processEntry2(p *mpb.Progress, inDir, outDir string, entry os.DirEntry) error {
	patterns := []string{
		".jats.software.json",
		".pub2tei.tei.json",
		".latex.tei.software.json",
		".grobid.tei.software.json",
	}

	compressedSuffix := "l.gz"

	writers := make(map[string]io.Writer)
	for _, pattern := range patterns {
		file, err := os.Create(filepath.Join(outDir, entry.Name()+pattern+compressedSuffix))
		if err != nil {
			return err
		}
		defer func() {
			err2 := file.Close()
			if err2 != nil {
				fmt.Println(err2)
			}
		}()

		gzipWriter := gzip.NewWriter(file)
		defer func() {
			err2 := gzipWriter.Close()
			if err2 != nil {
				fmt.Println(err2)
			}
		}()

		writers[pattern] = bufio.NewWriter(gzipWriter)
	}

	for pattern, writer := range writers {
		regex := regexp.MustCompile(UUIDPattern + pattern + "$")
		err := processDir(p, filepath.Join(inDir, entry.Name()), regex, writer)
		if err != nil {
			return err
		}
	}

	return nil
}

func processEntry(p *mpb.Progress, inDir, outDir string, entry os.DirEntry) (bool, error) {
	paperPath := filepath.Join(outDir, entry.Name()+".jsonl.gz")
	softwarePath := filepath.Join(outDir, entry.Name()+".software.jsonl.gz")

	if !entry.IsDir() {
		return true, nil
	}

	_, err := os.Stat(paperPath)
	if !os.IsNotExist(err) {
		_, err = os.Stat(softwarePath)
		if !os.IsNotExist(err) {
			return true, nil
		}
	}

	paperFile, err := os.Create(paperPath)
	if err != nil {
		return false, err
	}
	defer func(paperFile *os.File) {
		_ = paperFile.Close()
	}(paperFile)

	paperWriter := gzip.NewWriter(paperFile)

	err = processDir(p, filepath.Join(inDir, entry.Name()), PaperPattern, paperWriter)
	if err != nil {
		return false, err
	}
	err = paperWriter.Close()
	if err != nil {
		return false, err
	}

	softwareFile, err := os.Create(softwarePath)
	if err != nil {
		return false, err
	}
	defer func(softwareFile *os.File) {
		_ = softwareFile.Close()
	}(softwareFile)

	softwareWriter := gzip.NewWriter(softwareFile)

	err = processDir(p, filepath.Join(inDir, entry.Name()), SoftwarePattern, softwareWriter)
	if err != nil {
		return false, err
	}
	err = softwareWriter.Close()
	if err != nil {
		return false, err
	}

	return false, nil
}

func processDir(p *mpb.Progress, dir string, pattern *regexp.Regexp, out io.Writer) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	var bar *mpb.Bar
	var start time.Time
	if p != nil {
		bar = p.AddBar(int64(len(entries)),
			mpb.AppendDecorators(decor.AverageETA(decor.ET_STYLE_HHMMSS)),
			mpb.PrependDecorators(decor.Name(filepath.Base(dir))),
			mpb.PrependDecorators(decor.CountersNoUnit("%3d/%3d", decor.WCSyncSpace)),
			mpb.BarRemoveOnComplete())
		start = time.Now()
	}

	for _, entry := range entries {
		if entry.IsDir() {
			err = processDir(nil, filepath.Join(dir, entry.Name()), pattern, out)
			if err != nil {
				return err
			}
		} else if pattern.MatchString(entry.Name()) {
			err = processFile(filepath.Join(dir, entry.Name()), out)
		}

		if bar != nil {
			bar.IncrBy(1, time.Since(start))
		}
	}

	return nil
}

func processFile(inPath string, out io.Writer) error {
	inFile, err := os.Open(inPath)
	if err != nil {
		return err
	}
	defer func(inFile *os.File) {
		_ = inFile.Close()
	}(inFile)

	var entry map[string]interface{}
	err = json.NewDecoder(inFile).Decode(&entry)
	if err != nil {
		return err
	}

	entry["file"] = filepath.Base(inPath)

	// The Encoder automatically writes a newline after each JSON object.
	err = json.NewEncoder(out).Encode(entry)
	if err != nil {
		return err
	}

	return nil
}
