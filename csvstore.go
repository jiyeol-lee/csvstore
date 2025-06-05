package csvstore

import (
	"encoding/csv"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// CSVStore represents a CSV-based storage system
type CSVStore struct {
	basePath string
	mu       sync.RWMutex
}

// CSVRecord represents a row in CSV
type CSVRecord map[string]string

// QueryCondition represents a filter condition
type QueryCondition struct {
	Column   string
	Operator string // "=", "!=", ">", "<", ">=", "<=", "contains", "starts_with", "ends_with"
	Value    string
}

// QueryResult represents query results
type QueryResult struct {
	Records []CSVRecord
	Count   int
}

// NewCSVStore creates a new CSV-based storage system
func NewCSVStore(basePath string) (*CSVStore, error) {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %w", err)
	}

	return &CSVStore{
		basePath: basePath,
	}, nil
}

// getTablePath returns the file path for a table
func (cs *CSVStore) getTablePath(tableName string) string {
	return filepath.Join(cs.basePath, tableName+".csv")
}

// CreateTable creates a new CSV table with headers
func (cs *CSVStore) CreateTable(tableName string, headers []string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	tablePath := cs.getTablePath(tableName)

	// Check if table already exists
	if _, err := os.Stat(tablePath); err == nil {
		return fmt.Errorf("table %s already exists", tableName)
	}

	file, err := os.Create(tablePath)
	if err != nil {
		return fmt.Errorf("failed to create table file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write headers
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write headers: %w", err)
	}

	return nil
}

// Query executes a query on the CSV table
func (cs *CSVStore) Query(tableName string, conditions []QueryCondition) (*QueryResult, error) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	records, err := cs.loadTable(tableName)
	if err != nil {
		return nil, err
	}

	// Apply filters
	filteredRecords := make([]CSVRecord, 0)
	for _, record := range records {
		if cs.matchesConditions(record, conditions) {
			filteredRecords = append(filteredRecords, record)
		}
	}

	return &QueryResult{
		Records: filteredRecords,
		Count:   len(filteredRecords),
	}, nil
}

// Select retrieves specific columns from query results
func (cs *CSVStore) Select(
	tableName string,
	columns []string,
	conditions []QueryCondition,
) (*QueryResult, error) {
	result, err := cs.Query(tableName, conditions)
	if err != nil {
		return nil, err
	}

	// If no columns specified, return all columns
	if len(columns) == 0 {
		return result, nil
	}

	// Project only selected columns
	projectedRecords := make([]CSVRecord, len(result.Records))
	for i, record := range result.Records {
		projectedRecord := make(CSVRecord)
		maps.Copy(projectedRecord, record)
		// Keep only selected columns
		for key := range projectedRecord {
			if !slices.Contains(columns, key) {
				delete(projectedRecord, key)
			}
		}
		projectedRecords[i] = projectedRecord
	}

	return &QueryResult{
		Records: projectedRecords,
		Count:   len(projectedRecords),
	}, nil
}

// Insert adds a new record to the table
func (cs *CSVStore) Insert(tableName string, record CSVRecord) (CSVRecord, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	tablePath := cs.getTablePath(tableName)

	// Read existing data to get headers
	headers, err := cs.getHeaders(tableName)
	if err != nil {
		return nil, err
	}

	// Open file in append mode
	file, err := os.OpenFile(tablePath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open table file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Convert record to row based on headers order
	row := make([]string, len(headers))
	for i, header := range headers {
		row[i] = record[header]
	}

	// Add id if not provided
	if record["id"] == "" && slices.Contains(headers, "id") {
		for i, header := range headers {
			if header == "id" {
				row[i] = strconv.Itoa(int(time.Now().UnixNano())) // Use timestamp as unique ID
				break
			}
		}
	}

	rfc3339Now := time.Now().Format(time.RFC3339Nano)
	// Add created_at if not provided
	if record["created_at"] == "" && slices.Contains(headers, "created_at") {
		for i, header := range headers {
			if header == "created_at" {
				row[i] = rfc3339Now
				break
			}
		}
	}
	// Add updated_at if not provided
	if record["updated_at"] == "" && slices.Contains(headers, "updated_at") {
		for i, header := range headers {
			if header == "updated_at" {
				row[i] = rfc3339Now
				break
			}
		}
	}

	if err := writer.Write(row); err != nil {
		return nil, fmt.Errorf("failed to write record: %w", err)
	}

	insertedRecord := make(CSVRecord)
	for i, header := range headers {
		insertedRecord[header] = row[i]
	}
	return insertedRecord, nil
}

// Update updates records matching conditions
func (cs *CSVStore) Update(
	tableName string,
	updates CSVRecord,
	conditions []QueryCondition,
) (*QueryResult, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	records, err := cs.loadTable(tableName)
	if err != nil {
		return nil, err
	}

	headers, err := cs.getHeaders(tableName)
	if err != nil {
		return nil, err
	}

	updatedRecords := make([]CSVRecord, 0)
	for i, record := range records {
		if cs.matchesConditions(record, conditions) {
			// Store the original record before updating
			originalRecord := make(CSVRecord)
			maps.Copy(originalRecord, record)

			// Apply updates
			maps.Copy(records[i], updates)
			// Update timestamp
			if slices.Contains(headers, "updated_at") {
				records[i]["updated_at"] = time.Now().Format(time.RFC3339Nano)
			}

			// Store the updated record
			updatedRecord := make(CSVRecord)
			maps.Copy(updatedRecord, records[i])
			updatedRecords = append(updatedRecords, updatedRecord)
		}
	}

	result := &QueryResult{
		Records: updatedRecords,
		Count:   len(updatedRecords),
	}

	if result.Count > 0 {
		err = cs.saveTable(tableName, headers, records)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// Delete removes records matching conditions
func (cs *CSVStore) Delete(tableName string, conditions []QueryCondition) (*QueryResult, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	records, err := cs.loadTable(tableName)
	if err != nil {
		return nil, err
	}

	headers, err := cs.getHeaders(tableName)
	if err != nil {
		return nil, err
	}

	filteredRecords := make([]CSVRecord, 0)
	deletedRecords := make([]CSVRecord, 0)

	for _, record := range records {
		if !cs.matchesConditions(record, conditions) {
			filteredRecords = append(filteredRecords, record)
		} else {
			// Store the deleted record
			deletedRecord := make(CSVRecord)
			maps.Copy(deletedRecord, record)
			deletedRecords = append(deletedRecords, deletedRecord)
		}
	}

	result := &QueryResult{
		Records: deletedRecords,
		Count:   len(deletedRecords),
	}

	if result.Count > 0 {
		err = cs.saveTable(tableName, headers, filteredRecords)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// loadTable loads all records from a CSV table
func (cs *CSVStore) loadTable(tableName string) ([]CSVRecord, error) {
	tablePath := cs.getTablePath(tableName)

	file, err := os.Open(tablePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open table file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV: %w", err)
	}

	if len(rows) < 1 {
		return []CSVRecord{}, nil
	}

	headers := rows[0]
	records := make([]CSVRecord, 0, len(rows)-1)

	for _, row := range rows[1:] {
		record := make(CSVRecord)
		for i, value := range row {
			if i < len(headers) {
				record[headers[i]] = value
			}
		}
		records = append(records, record)
	}

	return records, nil
}

// getHeaders retrieves the headers of a CSV table
func (cs *CSVStore) getHeaders(tableName string) ([]string, error) {
	tablePath := cs.getTablePath(tableName)

	file, err := os.Open(tablePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open table file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read headers: %w", err)
	}

	return headers, nil
}

// saveTable saves the records back to the CSV file
func (cs *CSVStore) saveTable(tableName string, headers []string, records []CSVRecord) error {
	tablePath := cs.getTablePath(tableName)

	file, err := os.Create(tablePath)
	if err != nil {
		return fmt.Errorf("failed to create table file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write headers
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write headers: %w", err)
	}

	// Write records
	for _, record := range records {
		row := make([]string, len(headers))
		for i, header := range headers {
			row[i] = record[header]
		}
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("failed to write record: %w", err)
		}
	}

	return nil
}

// matchesConditions checks if a record matches all conditions
func (cs *CSVStore) matchesConditions(record CSVRecord, conditions []QueryCondition) bool {
	for _, condition := range conditions {
		if !cs.matchesCondition(record, condition) {
			return false // AND logic
		}
	}
	return true
}

// matchesCondition checks if a record matches a single condition
func (cs *CSVStore) matchesCondition(record CSVRecord, condition QueryCondition) bool {
	value, exists := record[condition.Column]
	if !exists {
		return false
	}

	switch condition.Operator {
	case "=", "==":
		return value == condition.Value
	case "!=":
		return value != condition.Value
	case ">":
		return compareNumeric(value, condition.Value) > 0
	case "<":
		return compareNumeric(value, condition.Value) < 0
	case ">=":
		return compareNumeric(value, condition.Value) >= 0
	case "<=":
		return compareNumeric(value, condition.Value) <= 0
	case "contains":
		return strings.Contains(strings.ToLower(value), strings.ToLower(condition.Value))
	case "starts_with":
		return strings.HasPrefix(strings.ToLower(value), strings.ToLower(condition.Value))
	case "ends_with":
		return strings.HasSuffix(strings.ToLower(value), strings.ToLower(condition.Value))
	default:
		return false
	}
}

// compareNumeric compares two numeric strings and returns -1, 0, or 1
func compareNumeric(a, b string) int {
	numA, errA := strconv.ParseFloat(a, 64)
	numB, errB := strconv.ParseFloat(b, 64)

	if errA != nil || errB != nil {
		// Fall back to string comparison
		return strings.Compare(a, b)
	}

	if numA < numB {
		return -1
	}
	if numA > numB {
		return 1
	}
	return 0
}

// GetTablePath returns the file path for a table (for external access)
func (cs *CSVStore) GetTablePath(tableName string) string {
	return cs.getTablePath(tableName)
}

// ListTables returns all available tables
func (cs *CSVStore) ListTables() ([]string, error) {
	files, err := os.ReadDir(cs.basePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	tables := make([]string, 0)
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".csv") {
			tableName := strings.TrimSuffix(file.Name(), ".csv")
			tables = append(tables, tableName)
		}
	}

	return tables, nil
}
