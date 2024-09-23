package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
	"github.com/willbeason/software-mentions/pkg/papers"
	"golang.org/x/crypto/ssh/terminal"
	"google.golang.org/protobuf/proto"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
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
	Use:     "mentions-convert DIR",
	Short:   "Convert software mentions to protobuf",
	Args:    cobra.ExactArgs(2),
	Version: "0.1.0",
	RunE:    runE,
}

var ErrMentionsConvert = fmt.Errorf("converting mentions")

func runE(cmd *cobra.Command, args []string) error {
	outPath := args[1]

	width, _, err := terminal.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return fmt.Errorf("%w: getting terminal size: %w", ErrMentionsConvert, err)
	}
	p := mpb.New(mpb.WithWidth(width))

	mentionCountsPath, err := cmd.Flags().GetString("mention-counts")
	if err != nil {
		return err
	}

	mentions := make(chan *papers.Mentions, 1000)
	go func() {
		inPath := args[0]
		err := processDirectory(inPath, p, 0, mentions)
		if err != nil {
			panic(err)
		}

		close(mentions)
	}()

	counts := make(map[string]int)

	writeWg := sync.WaitGroup{}
	writeWg.Add(1)
	go func() {
		outFile, err := os.Create(outPath)
		if err != nil {
			panic(err)
		}
		defer func() {
			err := outFile.Close()
			if err != nil {
				fmt.Println(err)
			}
		}()

		for mention := range mentions {
			for _, m := range mention.Mentions {
				counts[m.SoftwareName.NormalizedForm]++
			}
			err := writeProto(outFile, mention)
			if err != nil {
				panic(err)
			}
		}

		writeWg.Done()
	}()

	writeWg.Wait()

	if mentionCountsPath == "" {
		return nil
	}

	names := make([]string, len(counts))
	i := 0
	for name := range counts {
		names[i] = name
		i++
	}

	sort.Slice(names, func(i, j int) bool {
		return counts[names[i]] > counts[names[j]]
	})

	outFile, err := os.Create(mentionCountsPath)
	if err != nil {
		return fmt.Errorf("creating file for mention counts: %w", err)
	}
	defer func() {
		err := outFile.Close()
		if err != nil {
			fmt.Println(err)
		}
	}()

	for _, name := range names {
		_, err := fmt.Fprintf(outFile, "%s;%d\n", name, counts[name])
		if err != nil {
			return err
		}
	}

	return nil
}

func writeProto(w io.Writer, mention *papers.Mentions) error {
	bytes, err := proto.Marshal(mention)
	if err != nil {
		return fmt.Errorf("marshalling proto: %w", err)
	}

	nBytes := len(bytes)

	var buf []byte
	buf = binary.AppendUvarint(buf, uint64(nBytes))
	_, err = w.Write(buf)
	if err != nil {
		return fmt.Errorf("writing proto length: %w", err)
	}

	_, err = w.Write(bytes)
	if err != nil {
		return fmt.Errorf("writing proto: %w", err)
	}

	return nil
}

func processDirectory(inPath string, p *mpb.Progress, depth int, out chan<- *papers.Mentions) error {
	names, err := os.ReadDir(inPath)
	if err != nil {
		return fmt.Errorf("%w: stat %q: %w", ErrMentionsConvert, inPath, err)
	}

	pathName := filepath.Base(inPath)

	var bar *mpb.Bar
	var start time.Time
	if depth < 2 {
		bar = p.AddBar(int64(len(names)),
			mpb.AppendDecorators(decor.AverageETA(decor.ET_STYLE_HHMMSS)),
			mpb.PrependDecorators(decor.Name(pathName)),
			mpb.PrependDecorators(decor.CountersNoUnit("%3d/%3d", decor.WCSyncSpace)),
			mpb.BarRemoveOnComplete())
		start = time.Now()
	}

	sort.Slice(names, func(i, j int) bool {
		return names[i].Name() < names[j].Name()
	})

	for _, name := range names {
		entryPath := filepath.Join(inPath, name.Name())

		if name.IsDir() {
			err = processDirectory(entryPath, p, depth+1, out)
			if err != nil {
				return err
			}
		} else if strings.HasSuffix(name.Name(), ".software.json") {
			err = processFile(entryPath, out)
			if err != nil {
				return err
			}
		}

		if bar != nil {
			bar.IncrBy(1, time.Since(start))
		}
	}

	return nil
}

type MentionJson struct {
	Mentions []Mention `json:"mentions"`
}

type Mention struct {
	SoftwareName SoftwareName `json:"software-name"`
}

type SoftwareName struct {
	NormalizedForm string `json:"normalizedForm"`
}

func processFile(inPath string, out chan<- *papers.Mentions) error {
	base := filepath.Base(inPath)
	splits := strings.Split(base, ".")

	if len(splits[0]) != 36 {
		return nil
	}

	id, err := papers.ToUUID(splits[0])
	if err != nil {
		return fmt.Errorf("parsing UUID from filename %q: %w", inPath, err)
	}

	file, err := os.Open(inPath)
	if err != nil {
		return err
	}
	defer func() {
		err := file.Close()
		if err != nil {
			fmt.Println(err)
		}
	}()

	decoder := json.NewDecoder(file)
	mentionJson := &MentionJson{}
	err = decoder.Decode(mentionJson)
	if err != nil {
		return err
	}

	mention := &papers.Mentions{}
	mention.Id = id
	for _, m := range mentionJson.Mentions {
		mention.Mentions = append(mention.Mentions, &papers.Mention{
			SoftwareName: &papers.SoftwareName{
				NormalizedForm: m.SoftwareName.NormalizedForm,
			},
		})
	}

	out <- mention

	return nil
}
