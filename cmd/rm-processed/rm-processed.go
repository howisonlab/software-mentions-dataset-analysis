package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
	"golang.org/x/crypto/ssh/terminal"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

const UUIDPattern = `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`

var (
	PaperPattern    = regexp.MustCompile(UUIDPattern + `\.json$`)
	SoftwarePattern = regexp.MustCompile(UUIDPattern + `\.software\.json$`)
)

func main() {
	cmd.Flags().String("mention-counts", "", "file to write mention counts to")

	err := cmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

var cmd = cobra.Command{
	Use:     "rm-processed IN",
	Short:   "Remove already-processed JSON files",
	Args:    cobra.ExactArgs(1),
	Version: "0.1.0",
	RunE:    runE,
}

var ErrRm = fmt.Errorf("removing processed files")

func runE(_ *cobra.Command, args []string) error {
	inDir := args[0]

	err := ProcessDir(inDir)
	if err != nil {
		return err
	}

	return nil
}

func ProcessDir(inDir string) error {
	entries, err := os.ReadDir(inDir)
	if err != nil {
		return err
	}

	width, _, err := terminal.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return fmt.Errorf("%w: getting terminal size: %w", ErrRm, err)
	}
	p := mpb.New(mpb.WithWidth(width))
	nTotal := int64(len(entries))
	bar := p.AddBar(nTotal,
		mpb.AppendDecorators(decor.AverageETA(decor.ET_STYLE_HHMMSS)),
		mpb.PrependDecorators(decor.CountersNoUnit("%3d/%3d", decor.WCSyncSpace)),
		mpb.BarRemoveOnComplete())

	start := time.Now()
	for _, entry := range entries {
		entryPath := filepath.Join(inDir, entry.Name())
		err := processEntry(p, entryPath, entry)
		if err != nil {
			return err
		}

		bar.IncrBy(1, time.Since(start))
	}

	p.Wait()

	return nil

}

func processEntry(p *mpb.Progress, entryPath string, entry os.DirEntry) error {
	if !entry.IsDir() {
		return nil
	}

	beforeEntries, err := os.ReadDir(entryPath)
	if err != nil {
		return err
	}

	var bar *mpb.Bar
	var start time.Time
	if p != nil {
		bar = p.AddBar(int64(len(beforeEntries)),
			mpb.AppendDecorators(decor.AverageETA(decor.ET_STYLE_HHMMSS)),
			mpb.PrependDecorators(decor.Name(entry.Name())),
			mpb.PrependDecorators(decor.CountersNoUnit("%3d/%3d", decor.WCSyncSpace)),
			mpb.BarRemoveOnComplete())
		start = time.Now()
	}

	for _, beforeEntry := range beforeEntries {
		beforeEntryPath := filepath.Join(entryPath, beforeEntry.Name())
		if beforeEntry.IsDir() {
			err = processEntry(nil, beforeEntryPath, beforeEntry)
			if err != nil {
				return err
			}
		} else {
			err = processFile(beforeEntryPath)
			if err != nil {
				return err
			}
		}

		if bar != nil {
			bar.IncrBy(1, time.Since(start))
		}
	}

	afterEntries, err := os.ReadDir(entryPath)
	if err != nil {
		return err
	}

	if len(afterEntries) == 0 {
		err = os.Remove(entryPath)
		if err != nil {
			return err
		}
	}

	return nil
}

func processFile(path string) error {
	name := filepath.Base(path)
	if PaperPattern.MatchString(name) {
		return os.Remove(path)
	} else if SoftwarePattern.MatchString(name) {
		return os.Remove(path)
	}

	return nil
}
