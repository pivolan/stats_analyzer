package main

import (
	"reflect"
	"testing"
)

func TestAnalyzeHeaders(t *testing.T) {
	tests := []struct {
		name          string
		input         []string
		wantHeaders   []string
		wantIsData    bool
		wantFirstData []string
	}{
		{
			name:          "Valid headers",
			input:         []string{"Name", "Age", "Email", "Phone"},
			wantHeaders:   []string{"name", "age", "email", "phone"},
			wantIsData:    false,
			wantFirstData: []string{"Name", "Age", "Email", "Phone"},
		},
		{
			name:          "Numeric data",
			input:         []string{"123", "456", "789", "101"},
			wantHeaders:   []string{"column_1", "column_2", "column_3", "column_4"},
			wantIsData:    true,
			wantFirstData: []string{"123", "456", "789", "101"},
		},
		{
			name:          "Date data",
			input:         []string{"2024-01-01", "2024-01-02", "2024-01-03"},
			wantHeaders:   []string{"column_1", "column_2", "column_3"},
			wantIsData:    true,
			wantFirstData: []string{"2024-01-01", "2024-01-02", "2024-01-03"},
		},
		{
			name:          "Mixed headers with special characters",
			input:         []string{"User Name!", "Age#", "Email@", "Phone$"},
			wantHeaders:   []string{"user_name", "age", "email", "phone"},
			wantIsData:    false,
			wantFirstData: []string{"User Name!", "Age#", "Email@", "Phone$"},
		},
		{
			name:          "Duplicate headers",
			input:         []string{"Name", "Name", "Name", "Age"},
			wantHeaders:   []string{"name", "name_1", "name_2", "age"},
			wantIsData:    false,
			wantFirstData: []string{"Name", "Name", "Name", "Age"},
		},
		{
			name:          "Empty headers",
			input:         []string{"", "", "", ""},
			wantHeaders:   []string{"column_1", "column_2", "column_3", "column_4"},
			wantIsData:    true,
			wantFirstData: []string{"", "", "", ""},
		},
		{
			name:          "product_name;sales_quantity;unit_price;total_revenue",
			input:         []string{"product_name", "sales_quantity", "unit_price", "total_revenue"},
			wantHeaders:   []string{"product_name", "sales_quantity", "unit_price", "total_revenue"},
			wantIsData:    false,
			wantFirstData: []string{"", "", "", ""},
		},
		{
			name:          "Mixed data with numbers and text",
			input:         []string{"John", "30", "john@email.com", "123-456-7890"},
			wantHeaders:   []string{"column_1", "column_2", "column_3", "column_4"},
			wantIsData:    true,
			wantFirstData: []string{"John", "30", "john@email.com", "123-456-7890"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AnalyzeHeaders(tt.input)

			if got == nil {
				t.Fatal("AnalyzeHeaders returned nil")
			}

			if !reflect.DeepEqual(got.Headers, tt.wantHeaders) {
				t.Errorf("Headers = %v, want %v", got.Headers, tt.wantHeaders)
			}

			if got.FirstRowIsData != tt.wantIsData {
				t.Errorf("FirstRowIsData = %v, want %v", got.FirstRowIsData, tt.wantIsData)
			}

			if !reflect.DeepEqual(got.FirstDataRow, tt.wantFirstData) {
				t.Errorf("FirstDataRow = %v, want %v", got.FirstDataRow, tt.wantFirstData)
			}
		})
	}
}

func TestIsLikelyHeader(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"Empty string", "", false},
		{"Simple header", "Name", true},
		{"Header with space", "User Name", true},
		{"Number", "123", false},
		{"Date", "2024-01-01", false},
		{"Special characters", "User#Name!", true},
		{"Only special chars", "###", false},
		{"Mixed content", "User123", true},
		{"Rus", "колонка1", true},
		{"Email", "test@email.com", true},
		{"Phone", "+1-234-567-8900", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isLikelyHeader(tt.input); got != tt.want {
				t.Errorf("isLikelyHeader(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestValidateHeaders(t *testing.T) {
	tests := []struct {
		name     string
		headers  []string
		expected []string
	}{
		{
			name:     "No duplicates",
			headers:  []string{"name", "age", "email"},
			expected: []string{"name", "age", "email"},
		},
		{
			name:     "With duplicates",
			headers:  []string{"name", "name", "name"},
			expected: []string{"name", "name_1", "name_2"},
		},
		{
			name:     "Mixed duplicates",
			headers:  []string{"name", "age", "name", "email", "age"},
			expected: []string{"name", "age", "name_1", "email", "age_1"},
		},
		{
			name:     "Empty headers",
			headers:  []string{},
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ValidateHeaders(tt.headers)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("ValidateHeaders() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestIsNumericData(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		want   bool
	}{
		{
			name:   "All numbers",
			values: []string{"123", "456", "789", "101"},
			want:   true,
		},
		{
			name:   "Mixed data",
			values: []string{"123", "abc", "456", "def"},
			want:   false,
		},
		{
			name:   "Decimal numbers",
			values: []string{"123.45", "456.78", "789.01"},
			want:   true,
		},
		{
			name:   "Empty strings",
			values: []string{"", "", ""},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isNumericData(tt.values); got != tt.want {
				t.Errorf("isNumericData() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsDateData(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		want   bool
	}{
		{
			name:   "ISO dates",
			values: []string{"2024-01-01", "2024-01-02", "2024-01-03"},
			want:   true,
		},
		{
			name:   "Mixed formats",
			values: []string{"2024-01-01", "01/02/2024", "03.01.2024"},
			want:   true,
		},
		{
			name:   "Not dates",
			values: []string{"abc", "def", "ghi"},
			want:   false,
		},
		{
			name:   "Mixed data",
			values: []string{"2024-01-01", "not a date", "2024-01-03"},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isDateData(tt.values); got != tt.want {
				t.Errorf("isDateData() = %v, want %v", got, tt.want)
			}
		})
	}
}
