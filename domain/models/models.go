package models

type ClickhouseTableName string
type StatsQueryResultType string

type HistogramData struct {
	RangeStart float64 `db:"rangeStart"` // Добавляем теги для правильного маппинга
	RangeEnd   float64 `db:"rangeEnd"`
	Count      int     `db:"count"`
}
type ColumnInfo struct {
	Name string
	Type string //Date DateTime64 Int64 Float64
}
type QueryResult struct {
	Sql          string
	SingleOrMany StatsQueryResultType
}

type HeaderAnalysis struct {
	Headers        []string // Итоговые заголовки
	FirstRowIsData bool     // Является ли первая строка данными
	FirstDataRow   []string // Первая строка с данными
}
type BasicStats struct {
	TotalRows        int64   `db:"total_rows"`
	UniqueValues     int64   `db:"unique_values"`
	NullCount        int64   `db:"null_count"`
	EmptyStringCount int64   `db:"empty_string_count"`
	WhitespaceCount  int64   `db:"whitespace_count"`
	MinLength        int     `db:"min_length"`
	MaxLength        int     `db:"max_length"`
	AvgLength        float64 `db:"avg_length"`
}
type StringColumnStats struct {
	TotalRows          int64
	UniqueValues       int64
	NullCount          int64
	EmptyStringCount   int64
	WhitespaceCount    int64
	MinLength          int
	MaxLength          int
	AvgLength          float64
	PopularValues      []ValueCount
	UnpopularValues    []ValueCount
	LengthDistribution []LengthFrequency
}

type ValueCount struct {
	Value   string
	Count   int64
	Percent float64
}

type LengthFrequency struct {
	Length    int
	Frequency int64
}
type LengthDistribution struct {
	StrLength int
	Count     int64
}
type FrequencyData struct {
	Value string
	Count int64
	Type  string
}
type DateCount struct {
	Date     string
	Count    int64
	SumValue float64
}
