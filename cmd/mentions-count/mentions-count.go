package main

import (
	"errors"
	"fmt"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/parquet/file"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	"github.com/spf13/cobra"
	"github.com/willbeason/software-mentions/pkg/tables"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sort"
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

func runE(cmd *cobra.Command, args []string) error {
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

	inDir := args[0]

	//softwarePath := filepath.Join(inDir, tables.Software+tables.ParquetExt)
	mentionsPath := filepath.Join(inDir, tables.Mentions+tables.ParquetExt)

	mentionsReader, err := file.OpenParquetFile(mentionsPath, true)
	if err != nil {
		return err
	}

	fileReader, err := pqarrow.NewFileReader(
		mentionsReader,
		pqarrow.ArrowReadProperties{
			Parallel:  true,
			BatchSize: 1 << 20,
		},
		nil)
	if err != nil {
		return err
	}

	ctx := cmd.Context()
	recordReader, err := fileReader.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return err
	}

	softwareByPaper := make(map[string]map[string]bool)

	for {
		record, err := recordReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		paperIds := record.Column(0).(*array.String)
		softwareIds := record.Column(1).(*array.String)

		for row := range int(record.NumRows()) {
			paperId := paperIds.Value(row)
			softwareId := softwareIds.Value(row)

			if ignoreSoftware[softwareId] {
				continue
			}

			paperSoftware, ok := softwareByPaper[paperId]
			if !ok {
				paperSoftware = make(map[string]bool)
			}

			paperSoftware[softwareId] = true
			softwareByPaper[paperId] = paperSoftware
		}
	}

	nMentions := make(map[string]int)
	for _, paperSoftware := range softwareByPaper {
		for softwareId := range paperSoftware {
			nMentions[softwareId]++
		}
	}

	allowMap := make(map[string]bool)

	var softwareList []string
	for k, count := range nMentions {
		if count < 100 {
			continue
		}
		allowMap[k] = true
		softwareList = append(softwareList, k)
	}

	fmt.Println(len(softwareList))

	sort.Slice(softwareList, func(i, j int) bool {
		return nMentions[softwareList[i]] > nMentions[softwareList[j]]
	})

	for i, softwareId := range softwareList[:10] {
		fmt.Printf("%d;%s;%d\n", i, softwareId, nMentions[softwareId])
	}

	comentionsCounts := make(map[MentionDyad]int)

	for _, mentions := range softwareByPaper {
		for k := range mentions {
			if !allowMap[k] {
				continue
			}
			for l := range mentions {
				if !allowMap[l] {
					continue
				}

				if k >= l {
					continue
				}

				comentionsCounts[MentionDyad{_1: k, _2: l}]++
			}
		}
	}

	var comentionsList []MentionDyad
	for k, count := range comentionsCounts {
		if count < 100 {
			continue
		}
		comentionsList = append(comentionsList, k)
	}

	sort.Slice(comentionsList, func(i, j int) bool {
		return comentionsCounts[comentionsList[i]] > comentionsCounts[comentionsList[j]]
	})

	for _, dyad := range comentionsList[:20] {
		fmt.Printf("%s;%s;%d\n", dyad._1, dyad._2, comentionsCounts[dyad])
	}

	return nil
}

type MentionDyad struct {
	_1 string
	_2 string
}

var ignoreSoftware = map[string]bool{
	"script":    true,
	"code":      true,
	"scripts":   true,
	"survival":  true,
	"library":   true,
	"software":  true,
	"interface": true,
	"program":   true,
}

const IncrEvery = 1 << 10
