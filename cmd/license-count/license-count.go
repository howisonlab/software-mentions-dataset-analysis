package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
	"github.com/willbeason/software-mentions/pkg/papers"
	"golang.org/x/crypto/ssh/terminal"
	"google.golang.org/protobuf/proto"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"time"
)

var cmd = cobra.Command{
	Use:     "license-count FILE",
	Short:   "Count the licenses in a .jsonl file",
	Args:    cobra.ExactArgs(1),
	Version: "0.1.0",
	RunE:    runE,
}

var cpuprofile *string

var ErrCountLicenses = errors.New("counting licenses")

func runE(_ *cobra.Command, args []string) error {
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	inPath := args[0]
	if ext := filepath.Ext(inPath); ext != ".pbl" {
		return fmt.Errorf("%w: got file extension %q but want %q", ErrCountLicenses, ext, ".pbl")
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
	entry := &papers.PaperId{}
	bytes := make([]byte, 0, 256)
	incrEvery := 1000
	i := 0
	nRead := 0
	for {
		// Reset entry for reuse.
		entry.Reset()

		nProtoBytes, err := binary.ReadUvarint(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("%w: reading proto size: %w", ErrCountLicenses, err)
		}

		if int(nProtoBytes) > len(bytes) {
			bytes = make([]byte, nProtoBytes)
		}
		bytes := bytes[:nProtoBytes]
		_, err = io.ReadFull(reader, bytes)
		if err != nil {
			return err
		}

		err = proto.Unmarshal(bytes, entry)
		if err != nil {
			return fmt.Errorf("%w: unmarshalling proto: %w", ErrCountLicenses, err)
		}

		license, err := papers.ToLicenseString(entry.License)
		if err != nil {
			return err
		}

		licenseMap[license]++
		nSizeBytes := binary.Size(nProtoBytes)
		i++
		nRead += nSizeBytes + int(nProtoBytes)
		if i%incrEvery == 0 {
			bar.IncrBy(nRead, time.Since(start))
			nRead = 0
		}
	}

	licenses := make([]string, len(licenseMap))
	i = 0
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

func main() {
	cpuprofile = cmd.Flags().String("cpuprofile", "", "write cpu profile to `file`")

	err := cmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
