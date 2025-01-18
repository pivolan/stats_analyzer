package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/pivolan/stats_analyzer/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Mock для GORM DB
type MockDB struct {
	*gorm.DB
	mock.Mock
	lastQuery string // Added to track the last executed query
}

func NewMockDB() *MockDB {
	return &MockDB{
		DB: &gorm.DB{},
	}
}

// Переопределяем метод Exec
func (m *MockDB) Exec(query string, values ...interface{}) *gorm.DB {
	args := m.Called(query, values)
	fmt.Println(query)
	m.lastQuery = query // Store the last executed query
	return args.Get(0).(*gorm.DB)
}

func TestImportDataIntoClickHouse(t *testing.T) {
	original := getMD5String
	getMD5String = func(input string) string {
		return "123456" // Фиксированное значение для тестов
	}
	defer func() {
		getMD5String = original // Восстанавливаем оригинальную функцию после теста
	}()

	testCases := []struct {
		name            string
		csvContent      string
		expectedTable   string
		expectedLastSQL string // Added expected last SQL query
		expectedError   bool
		setupMock       func(*MockDB)
	}{
		{
			name: "Basic CSV Import",
			csvContent: `id,name,age,date
1,John Doe,30,2024-01-01
2,Jane Smith,25,2024-01-02
3,Bob Johnson,35,2024-01-03`,
			expectedTable:   "id_name_age_123456",
			expectedLastSQL: "INSERT INTO id_name_age_123456 FORMAT CSV \n1,'John Doe',30,'2024-01-01'\n2,'Jane Smith',25,'2024-01-02'\n3,'Bob Johnson',35,'2024-01-03'\n",
			expectedError:   false,
			setupMock: func(db *MockDB) {
				db.On("Exec", mock.Anything, mock.Anything).Return(&gorm.DB{})
			},
		},
		{
			name: "CSV with Different Types",
			csvContent: `product,price,quantity,date_added
Apple,1.99,100,2024-01-01
Orange,2.50,150,2024-01-02
Banana,1.75,200,2024-01-03`,
			expectedTable:   "product_price_quantity_123456",
			expectedLastSQL: "INSERT INTO product_price_quantity_123456 FORMAT CSV \n1,'Apple',1.99,100,'2024-01-01'\n2,'Orange',2.50,150,'2024-01-02'\n3,'Banana',1.75,200,'2024-01-03'\n",
			expectedError:   false,
			setupMock: func(db *MockDB) {
				db.On("Exec", mock.Anything, mock.Anything).Return(&gorm.DB{})
			},
		},
		{
			name: "CSV no header",
			csvContent: `Apple,1.99,100,2024-01-01
Orange,2.50,150,2024-01-02
Banana,1.75,200,2024-01-03`,
			expectedTable:   "column_1_column_2_column_3_123456",
			expectedLastSQL: "INSERT INTO column_1_column_2_column_3_123456 FORMAT CSV \n1,'Apple',1.99,100,'2024-01-01'\n2,'Orange',2.50,150,'2024-01-02'\n3,'Banana',1.75,200,'2024-01-03'\n",
			expectedError:   false,
			setupMock: func(db *MockDB) {
				db.On("Exec", mock.Anything, mock.Anything).Return(&gorm.DB{})
			},
		},
		{
			name: "CSV 1 column",
			csvContent: `name
Apple
Orange
Banana`,
			expectedTable:   "name_123456",
			expectedLastSQL: "INSERT INTO name_123456 FORMAT CSV \n1,'Apple'\n2,'Orange'\n3,'Banana'\n",
			expectedError:   false,
			setupMock: func(db *MockDB) {
				db.On("Exec", mock.Anything, mock.Anything).Return(&gorm.DB{})
			},
		},
		{
			name: "CSV numeric",
			csvContent: `Task1;Task2;Task0
123;321;4235
5234;234;3
1432;342;123
123;23423;532
23454;123;234
23;1675;124
`,
			expectedTable:   "column_1_column_2_column_3_123456",
			expectedLastSQL: "INSERT INTO column_1_column_2_column_3_123456 FORMAT CSV \n1,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123\n2,1235,1235,1235,1235,1235,1235,1235,1235,1235,1235,1235,1235,1235,1235,1235,1235,1235,1235,1235,1235,1235,1235,1235,1235,1235,1235,1235,1235,1235,1235\n3,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123\n4,5234,5234,5234,5234,5234,5234,5234,5234,5234,5234,5234,5234,5234,5234,5234,5234,5234,5234,5234,5234,5234,5234,5234,5234,5234,5234,5234,5234,5234,5234\n5,1432,1432,1432,1432,1432,1432,1432,1432,1432,1432,1432,1432,1432,1432,1432,1432,1432,1432,1432,1432,1432,1432,1432,1432,1432,1432,1432,1432,1432,1432\n6,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123,123\n7,23454,23454,23454,23454,23454,23454,23454,23454,23454,23454,23454,23454,23454,23454,23454,23454,23454,23454,23454,23454,23454,23454,23454,23454,23454,23454,23454,23454,23454,23454\n8,23,23,23,23,23,23,23,23,23,23,23,23,23,23,23,23,23,23,23,23,23,23,23,23,23,23,23,23,23,23\n",
			expectedError:   false,
			setupMock: func(db *MockDB) {
				db.On("Exec", mock.Anything, mock.Anything).Return(&gorm.DB{})
			},
		},
		{
			name: "CSV numeric header",
			csvContent: `Task1;Task2;Task0
123;321;4235
5234;234;3
1432;342;123
123;23423;532
23454;123;234
23;1675;124
`,
			expectedTable:   "task1_task2_task0_123456",
			expectedLastSQL: "INSERT INTO task1_task2_task0_123456 FORMAT CSV \n1,123,321,4235\n2,5234,234,3\n3,1432,342,123\n4,123,23423,532\n5,23454,123,234\n6,23,1675,124\n",
			expectedError:   false,
			setupMock: func(db *MockDB) {
				db.On("Exec", mock.Anything, mock.Anything).Return(&gorm.DB{})
			},
		},
		{
			name:            "Empty CSV",
			csvContent:      "",
			expectedTable:   "",
			expectedLastSQL: "",
			expectedError:   true,
			setupMock:       func(db *MockDB) {},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Создаем временный файл
			tmpFile, err := os.CreateTemp("", "test_*.csv")
			assert.NoError(t, err)
			defer os.Remove(tmpFile.Name())

			// Записываем тестовые данные
			_, err = tmpFile.WriteString(tc.csvContent)
			assert.NoError(t, err)
			tmpFile.Close()

			// Создаем и настраиваем мок для БД
			mockDB := NewMockDB()
			tc.setupMock(mockDB)

			// Выполняем тестируемую функцию
			tableName, err := importDataIntoClickHouse(tmpFile.Name(), mockDB)

			// Проверяем результаты
			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Contains(t, tableName, tc.expectedTable)
				// Verify the last SQL query
				assert.Equal(t, tc.expectedLastSQL, mockDB.lastQuery)
			}
		})
	}
}

// Тест для вспомогательных функций
func TestDetectDelimiter(t *testing.T) {
	testCases := []struct {
		name          string
		content       string
		expectedDelim rune
		expectedError bool
	}{
		{
			name:          "Comma Delimiter",
			content:       "header1,header2,header3\nvalue1,value2,value3",
			expectedDelim: ',',
			expectedError: false,
		},
		{
			name:          "Semicolon Delimiter",
			content:       "header1;header2;header3\nvalue1;value2;value3",
			expectedDelim: ';',
			expectedError: false,
		},
		{
			name:          "Tab Delimiter",
			content:       "header1\theader2\theader3\nvalue1\tvalue2\tvalue3",
			expectedDelim: '\t',
			expectedError: false,
		},
		{
			name:          "No Clear Delimiter",
			content:       "header1header2header3",
			expectedDelim: ',', // По умолчанию возвращается запятая
			expectedError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Создаем временный файл
			tmpFile, err := os.CreateTemp("", "test_*.csv")
			assert.NoError(t, err)
			defer os.Remove(tmpFile.Name())

			// Записываем тестовые данные
			_, err = tmpFile.WriteString(tc.content)
			assert.NoError(t, err)
			tmpFile.Close()

			// Проверяем определение разделителя
			delimiter, err := detectDelimiter(tmpFile.Name())

			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedDelim, delimiter)
			}
		})
	}
}

func TestReplaceSpecialSymbols(t *testing.T) {
	testCases := []struct {
		input    string
		expected string
	}{
		{
			input:    "hello world",
			expected: "hello_world",
		},
		{
			input:    "test@example.com",
			expected: "test_example_com",
		},
		{
			input:    "__test__",
			expected: "test",
		},
		{
			input:    "123-456_789",
			expected: "123_456_789",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			result := replaceSpecialSymbols(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestSetZeroFields(t *testing.T) {
	tests := []struct {
		name     string
		target   CommonStat
		source   CommonStat
		expected CommonStat
	}{
		{
			name: "Basic zero field replacement",
			target: CommonStat{
				Count: 0,
				Uniq:  100,
			},
			source: CommonStat{
				Count: 50,
				Uniq:  200,
			},
			expected: CommonStat{
				Count: 50,  // Should be replaced because target is zero
				Uniq:  100, // Should not be replaced because target is non-zero
			},
		},
		{
			name: "All numeric fields test",
			target: CommonStat{
				Count:      0,
				Uniq:       0,
				Avg:        0,
				Min:        0,
				Max:        0,
				Median:     0,
				Quantile01: 0,
				Quantile09: 0,
			},
			source: CommonStat{
				Count:      100,
				Uniq:       200,
				Avg:        50.5,
				Min:        10.0,
				Max:        90.0,
				Median:     45.5,
				Quantile01: 15.0,
				Quantile09: 85.0,
			},
			expected: CommonStat{
				Count:      100,
				Uniq:       200,
				Avg:        50.5,
				Min:        10.0,
				Max:        90.0,
				Median:     45.5,
				Quantile01: 15.0,
				Quantile09: 85.0,
			},
		},
		{
			name: "Mixed zero and non-zero fields",
			target: CommonStat{
				Count:     100,
				Uniq:      0,
				Avg:       25.5,
				Min:       0,
				Max:       75.0,
				Median:    0,
				IsNumeric: true,
			},
			source: CommonStat{
				Count:     200,
				Uniq:      150,
				Avg:       30.0,
				Min:       10.0,
				Max:       80.0,
				Median:    35.5,
				IsNumeric: false,
			},
			expected: CommonStat{
				Count:     100,  // Not replaced because target is non-zero
				Uniq:      150,  // Replaced because target is zero
				Avg:       25.5, // Not replaced because target is non-zero
				Min:       10.0, // Replaced because target is zero
				Max:       75.0, // Not replaced because target is non-zero
				Median:    35.5, // Replaced because target is zero
				IsNumeric: true, // Boolean fields are not affected by setZeroFields
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := tt.target
			setZeroFields(&target, tt.source)

			// Check Count
			if target.Count != tt.expected.Count {
				t.Errorf("Count = %v, want %v", target.Count, tt.expected.Count)
			}

			// Check Uniq
			if target.Uniq != tt.expected.Uniq {
				t.Errorf("Uniq = %v, want %v", target.Uniq, tt.expected.Uniq)
			}

			// Check Avg
			if target.Avg != tt.expected.Avg {
				t.Errorf("Avg = %v, want %v", target.Avg, tt.expected.Avg)
			}

			// Check Min
			if target.Min != tt.expected.Min {
				t.Errorf("Min = %v, want %v", target.Min, tt.expected.Min)
			}

			// Check Max
			if target.Max != tt.expected.Max {
				t.Errorf("Max = %v, want %v", target.Max, tt.expected.Max)
			}

			// Check Median
			if target.Median != tt.expected.Median {
				t.Errorf("Median = %v, want %v", target.Median, tt.expected.Median)
			}

			// Check IsNumeric (though it's not affected by setZeroFields)
			if target.IsNumeric != tt.expected.IsNumeric {
				t.Errorf("IsNumeric = %v, want %v", target.IsNumeric, tt.expected.IsNumeric)
			}
		})
	}
}

func TestImportDataIntoClickHouseEnv(t *testing.T) {
	cfg := config.GetConfig()
	db, err := gorm.Open(mysql.Open(cfg.DatabaseDSN), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	assert.NoError(t, err)

	table, err := importDataIntoClickHouse("a.csv", db)
	assert.NoError(t, err)
	fmt.Println(table)
	stat := analyzeStatistics(table)
	formattedText := GenerateCommonInfoMsg(stat)
	fmt.Println(formattedText)
}
