package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
	bondsmith_io "github.com/willbeason/bondsmith-io"
	"golang.org/x/crypto/ssh/terminal"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"time"
)

func main() {
	cpuprofile = cmd.Flags().String("cpuprofile", "", "write cpu profile to `file`")

	err := cmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

var cmd = cobra.Command{
	Use:     "mentions-count DIR",
	Short:   "Count the licenses in a directory of .software.jsonl.gz files",
	Args:    cobra.ExactArgs(1),
	Version: "0.1.0",
	RunE:    runE,
}

var cpuprofile *string

var ErrCountLicenses = errors.New("counting software mentions")

func runE(_ *cobra.Command, args []string) error {
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer func() {
			err := f.Close()
			if err != nil {
				log.Fatal("could not close CPU profile: ", err)
			}
		}()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	width, _, err := terminal.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return fmt.Errorf("%w: getting terminal size: %w", ErrCountLicenses, err)
	}
	p := mpb.New(mpb.WithWidth(width))

	inPath := args[0]

	softwareMap := make(map[string]int)

	entries, err := os.ReadDir(inPath)
	if err != nil {
		return fmt.Errorf("%w: reading directory %q: %w", ErrCountLicenses, inPath, err)
	}

	entries = filterEntries(entries)
	//entries = entries[:2]

	bar := p.AddBar(int64(len(entries)),
		mpb.PrependDecorators(decor.CountersNoUnit("%3d/%3d")),
		mpb.AppendDecorators(decor.AverageETA(decor.ET_STYLE_HHMMSS)),
		mpb.BarRemoveOnComplete())

	start := time.Now()
	mergeWg := sync.WaitGroup{}
	mergeWg.Add(1)

	maps := make(chan map[string]int, 10)
	go func() {
		for entrySoftwareMap := range maps {
			for name, count := range entrySoftwareMap {
				softwareMap[name] += count
			}
		}
		mergeWg.Done()
	}()

	for _, entry := range entries {
		entryPath := filepath.Join(inPath, entry.Name())

		entrySoftwareMap, err := processFile(entryPath)
		if err != nil {
			return err
		}

		maps <- entrySoftwareMap

		bar.IncrBy(1, time.Since(start))
	}
	bar.SetTotal(int64(len(entries)), true)

	close(maps)
	mergeWg.Wait()

	var software []string
	for k, count := range softwareMap {
		if count < 100 {
			continue
		}
		software = append(software, k)
	}

	for _, s := range software {
		allowMap[s] = true
	}

	sort.Slice(software, func(i, j int) bool {
		return softwareMap[software[i]] > softwareMap[software[j]]
	})

	for _, entry := range software[:100] {
		fmt.Printf("%s;%d\n", entry, softwareMap[entry])
	}

	dyadMap := make(map[MentionDyad]int)

	merge2Wg := sync.WaitGroup{}
	merge2Wg.Add(1)
	maps2 := make(chan map[MentionDyad]int, 10)
	go func() {
		for entryDyadMap := range maps2 {
			for dyad, count := range entryDyadMap {
				dyadMap[dyad] += count
			}
		}
		merge2Wg.Done()
	}()

	bar2 := p.AddBar(int64(len(entries)),
		mpb.PrependDecorators(decor.CountersNoUnit("%3d/%3d")),
		mpb.AppendDecorators(decor.AverageETA(decor.ET_STYLE_HHMMSS)),
		mpb.BarRemoveOnComplete())

	for _, entry := range entries {
		entryPath := filepath.Join(inPath, entry.Name())

		entryDyadMap, err := processFile2(entryPath)
		if err != nil {
			return err
		}

		maps2 <- entryDyadMap

		bar2.IncrBy(1, time.Since(start))
	}
	bar2.SetTotal(int64(len(entries)), true)

	close(maps2)
	merge2Wg.Wait()

	var dyads []MentionDyad
	for k, count := range dyadMap {
		if count < 100 {
			continue
		}
		dyads = append(dyads, k)
	}

	sort.Slice(dyads, func(i, j int) bool {
		return dyadMap[dyads[i]] > dyadMap[dyads[j]]
	})

	for i, entry := range dyads {
		if i > 100 {
			break
		}
		fmt.Printf("%s;%s;%d\n", entry._1, entry._2, dyadMap[entry])
	}

	// Add newline to prevent last line of output from being consumed by progress bar.
	fmt.Println()

	return nil
}

func filterEntries(entries []os.DirEntry) []os.DirEntry {
	filtered := make([]os.DirEntry, 0, len(entries))
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".software.jsonl.gz") {
			continue
		}
		filtered = append(filtered, entry)
	}
	return filtered
}

type MentionDyad struct {
	_1 string
	_2 string
}

var allowMap = make(map[string]bool)

const IncrEvery = 1 << 10

func processFile(inPath string) (map[string]int, error) {
	file, err := os.Open(inPath)
	if err != nil {
		return nil, fmt.Errorf("%w: opening %q: %w", ErrCountLicenses, inPath, err)
	}
	defer func() {
		err := file.Close()
		if err != nil {
			log.Printf("error closing %q: %v\n", inPath, err)
		}
	}()

	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		return nil, fmt.Errorf("%w: opening gzip reader for %q: %w", ErrCountLicenses, inPath, err)
	}
	defer func() {
		err := gzipReader.Close()
		if err != nil {
			log.Printf("error closing %q: %v\n", inPath, err)
		}
	}()

	reader := bufio.NewReader(gzipReader)

	if strings.Contains(inPath, "software.jsonl.gz") {
		jsonReader := bondsmith_io.NewJsonReader(reader, func() *Paper {
			return &Paper{}
		})

		fileSoftwareMap := make(map[string]int)

		for paper, err := range jsonReader.Read() {
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return nil, err
			}

			for _, mention := range paper.Mentions {
				if mention.SoftwareType == "implicit" {
					continue
				}
				switch mention.SoftwareName.NormalizedForm {
				case "script", "code", "scripts", "survival", "library", "software", "interface", "program":
					continue
				}

				fileSoftwareMap[mention.SoftwareName.NormalizedForm]++
			}
		}

		return fileSoftwareMap, nil
	} else if strings.Contains(inPath, "software.pbl.gz") {
		return nil, fmt.Errorf("%w: proto not implemented yet: %q", ErrCountLicenses, inPath)
	} else {
		return nil, fmt.Errorf("%w: got unknown file extension for %q", ErrCountLicenses, inPath)
	}
}

func processFile2(inPath string) (map[MentionDyad]int, error) {
	file, err := os.Open(inPath)
	if err != nil {
		return nil, fmt.Errorf("%w: opening %q: %w", ErrCountLicenses, inPath, err)
	}
	defer func() {
		err := file.Close()
		if err != nil {
			log.Printf("error closing %q: %v\n", inPath, err)
		}
	}()

	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		return nil, fmt.Errorf("%w: opening gzip reader for %q: %w", ErrCountLicenses, inPath, err)
	}
	defer func() {
		err := gzipReader.Close()
		if err != nil {
			log.Printf("error closing %q: %v\n", inPath, err)
		}
	}()

	reader := bufio.NewReader(gzipReader)

	if strings.Contains(inPath, "software.jsonl.gz") {
		jsonReader := bondsmith_io.NewJsonReader(reader, func() *Paper {
			return &Paper{}
		})

		dyadMap := make(map[MentionDyad]int)

		for paper, err := range jsonReader.Read() {
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return nil, err
			}

			seenInPaper := make(map[string]bool)

			sort.Slice(paper.Mentions, func(i, j int) bool {
				return paper.Mentions[i].SoftwareName.NormalizedForm < paper.Mentions[j].SoftwareName.NormalizedForm
			})

			for i, mention := range paper.Mentions {
				if !allowMap[mention.SoftwareName.NormalizedForm] {
					continue
				}
				if seenInPaper[mention.SoftwareName.NormalizedForm] {
					continue
				}
				seenInPaper[mention.SoftwareName.NormalizedForm] = true

				seenInMention := make(map[string]bool)

				for _, mention2 := range paper.Mentions[i+1:] {
					if !allowMap[mention2.SoftwareName.NormalizedForm] {
						continue
					}
					if seenInMention[mention2.SoftwareName.NormalizedForm] {
						continue
					}
					seenInMention[mention2.SoftwareName.NormalizedForm] = true

					if mention.SoftwareName.NormalizedForm == mention2.SoftwareName.NormalizedForm {
						continue
					}

					dyad := MentionDyad{_1: mention.SoftwareName.NormalizedForm, _2: mention2.SoftwareName.NormalizedForm}
					dyadMap[dyad]++
				}
			}
		}

		return dyadMap, nil
	} else if strings.Contains(inPath, "software.pbl.gz") {
		return nil, fmt.Errorf("%w: proto not implemented yet: %q", ErrCountLicenses, inPath)
	} else {
		return nil, fmt.Errorf("%w: got unknown file extension for %q", ErrCountLicenses, inPath)
	}
}

var zeroPaper = &Paper{}

type Paper struct {
	Mentions []Mention `json:"mentions"`
}

func (p *Paper) Reset() {
	*p = *zeroPaper
}

type Mention struct {
	SoftwareName SoftwareName `json:"software-name"`
	SoftwareType string       `json:"software-type"`
}

type SoftwareName struct {
	NormalizedForm string `json:"NormalizedForm"`
}
