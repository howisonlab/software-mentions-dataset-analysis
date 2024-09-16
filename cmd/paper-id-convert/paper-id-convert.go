package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/go-cmp/cmp"
	"github.com/spf13/cobra"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
	"github.com/willbeason/software-mentions/pkg/papers"
	"golang.org/x/crypto/ssh/terminal"
	"google.golang.org/protobuf/proto"
	"io"
	"os"
	"path/filepath"
	"time"
)

func main() {
	cmd.Flags().Bool("validate", false, "validate the transform is not lossy")
	err := cmd.Execute()

	if err != nil {
		os.Exit(1)
	}
}

var cmd = cobra.Command{
	Use:     "paper-id-convert INFILE OUTFILE",
	Short:   "Collect statistics about keys and values in .jsonl files",
	Args:    cobra.ExactArgs(2),
	Version: "0.1.0",
	RunE:    runE,
}

var ErrConvert = errors.New("converting to proto")

func runE(cmd *cobra.Command, args []string) error {
	validate, err := cmd.Flags().GetBool("validate")
	if err != nil {
		return err
	}

	inPath := args[0]
	if ext := filepath.Ext(inPath); ext != ".jsonl" {
		return fmt.Errorf("%w: got input file extension %q but want %q", ErrConvert, ext, ".jsonl")
	}

	outPath := args[1]
	if ext := filepath.Ext(outPath); ext != ".pbl" {
		return fmt.Errorf("%w: got output file extension %q but want %q", ErrConvert, ext, ".pbl")
	}

	file, err := os.Open(inPath)
	if err != nil {
		return fmt.Errorf("%w: opening %q: %w", ErrConvert, inPath, err)
	}

	reader := bufio.NewReader(file)

	stats, err := file.Stat()
	if err != nil {
		return fmt.Errorf("%w: reading stats of %q: %w", ErrConvert, inPath, err)
	}

	width, _, err := terminal.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return fmt.Errorf("%w: getting terminal size: %w", ErrConvert, err)
	}
	p := mpb.New(mpb.WithWidth(width))
	bar := p.AddBar(stats.Size(),
		mpb.PrependDecorators(decor.AverageSpeed(decor.UnitKiB, "%.1f")),
		mpb.AppendDecorators(decor.AverageETA(decor.ET_STYLE_GO)))

	outFile, err := os.Create(outPath)
	defer func() {
		if err := outFile.Close(); err != nil {
			fmt.Printf("%v: closing output file %q: %v\n", ErrConvert, outPath, err)
		}
	}()

	writer := bufio.NewWriter(outFile)

	start := time.Now()
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		entry := &papers.PaperIdJson{}
		err = json.Unmarshal(line, entry)
		if err != nil {
			return fmt.Errorf("%w: unmarshalling JSON: %w", ErrConvert, err)
		}

		idProto, err := entry.MarshalProto()
		if err != nil {
			return fmt.Errorf("%w: converting to proto: %w", ErrConvert, err)
		}

		if validate {
			entry2 := &papers.PaperIdJson{}
			err = entry2.UnmarshalProto(idProto)
			if err != nil {
				return err
			}

			if diff := cmp.Diff(entry, entry2, cmp.FilterPath(func(path cmp.Path) bool {
				return path.Last().String() == ".License"
			}, cmp.Comparer(func(left, right string) bool {
				leftType, _ := papers.ToLicenseType(left)
				rightType, _ := papers.ToLicenseType(right)
				return leftType == rightType
			}))); diff != "" {
				return fmt.Errorf("%w: converting to proto and back is lossy: %s", ErrConvert, diff)
			}
		}

		protoBytes, err := proto.Marshal(idProto)
		if err != nil {
			return err
		}

		nProtoBytes := len(protoBytes)

		var buf []byte
		binary.AppendUvarint(buf, uint64(nProtoBytes))
		_, err = writer.Write(buf)
		if err != nil {
			return fmt.Errorf("%w: writing length to %q: %w", ErrConvert, outPath, err)
		}

		_, err = writer.Write(protoBytes)
		if err != nil {
			return fmt.Errorf("%w: writing proto to %q: %w", ErrConvert, outPath, err)
		}

		bar.IncrBy(len(line), time.Since(start))
	}

	return nil
}
