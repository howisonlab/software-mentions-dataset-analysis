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
	"strings"
	"time"
)

var cmd = cobra.Command{
	Use:     "license-count FILE",
	Short:   "Count the licenses in a .jsonl file",
	Args:    cobra.ExactArgs(1),
	Version: "0.1.0",
	RunE:    runE,
}

var ErrCountLicenses = errors.New("counting licenses")

func runE(cmd *cobra.Command, args []string) error {
	inPath := args[0]
	if ext := filepath.Ext(inPath); ext != ".jsonl" {
		return fmt.Errorf("%w: got file extension %q but want %q", ErrCountLicenses, ext, ".jsonl")
	}

	file, err := os.Open(inPath)
	if err != nil {
		return fmt.Errorf("%w: opening %q: %w", ErrCountLicenses, inPath, err)
	}

	reader := bufio.NewReader(file)

	licenseMap := make(map[string]int)

	stats, err := file.Stat()
	if err != nil {
		return fmt.Errorf("%w: reading stats of %q: %w", ErrCountLicenses, inPath, err)
	}

	width, _, err := terminal.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return fmt.Errorf("%w: getting terminal size: %w", ErrCountLicenses, err)
	}
	p := mpb.New(mpb.WithWidth(width))
	bar := p.AddBar(stats.Size(),
		mpb.PrependDecorators(decor.AverageSpeed(decor.UnitKiB, "%.1f")),
		mpb.AppendDecorators(decor.AverageETA(decor.ET_STYLE_GO)))

	start := time.Now()
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		var entry Entry
		err = json.Unmarshal(line, &entry)
		if err != nil {
			return err
		}

		license := entry.License
		if strings.HasPrefix(license, "CC ") {
			license = strings.ToLower(license)
			license = "cc-" + license[3:]
		} else if license == "CC0" {
			license = "cc0"
		} else if license == "NO-CC CODE" {
			license = "no-cc code"
		} else if license == "" {
			license = "NONE"
		}

		licenseMap[license]++
		bar.IncrBy(len(line), time.Since(start))
	}

	licenses := make([]string, len(licenseMap))
	i := 0
	for k := range licenseMap {
		licenses[i] = k
		i++
	}

	sort.Slice(licenses, func(i, j int) bool {
		return licenseMap[licenses[i]] > licenseMap[licenses[j]]
	})

	for _, license := range licenses {
		fmt.Printf("%s;%d\n", license, licenseMap[license])
	}

	return nil
}

type Entry struct {
	Id  string `json:"id"`
	Doi string `json:"doi"`

	Latex string `json:"latex"`
	Xml   string `json:"xml"`
	Pdf   string `json:"pdf"`

	Arxiv   string `json:"arxiv"`
	Pmid    string `json:"pmid"`
	Pmcid   string `json:"pmcid"`
	IstexId string `json:"istexId"`

	// "json", "pdf", "latex", "xml"
	Resources []string `json:"resources"`
	License   string   `json:"license"`
	OaLink    string   `json:"oa_link"`
}

func main() {
	err := cmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
