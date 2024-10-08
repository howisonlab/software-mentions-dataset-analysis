package tables

import "github.com/apache/arrow/go/v18/arrow"

const (
	Papers   = "papers"
	Software = "software"
	Mentions = "mentions"

	ParquetExt = ".parquet"
)

var (
	SoftwareSchema = arrow.NewSchema([]arrow.Field{
		{Name: "normalizedForm", Type: arrow.BinaryTypes.String},
		{Name: "wikidataId", Type: arrow.BinaryTypes.String},
		{Name: "softwareType", Type: &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Uint8,
			ValueType: arrow.BinaryTypes.String,
			Ordered:   false,
		}},
	}, nil)

	MentionsSchema = arrow.NewSchema([]arrow.Field{
		{Name: "paperId", Type: arrow.BinaryTypes.String},
		{Name: "softwareId", Type: arrow.BinaryTypes.String},
	}, nil)
)
