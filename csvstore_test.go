package csvstore

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func getTestDir() string {
	return fmt.Sprintf("/tmp/csvstore/test_%d", time.Now().UnixNano())
}

func TestNewCSVStore(t *testing.T) {
	testDir := getTestDir()

	store, err := NewCSVStore(testDir)
	if err != nil {
		t.Fatalf("Failed to create CSVStore: %v", err)
	}

	if store.basePath != testDir {
		t.Errorf("Expected basePath %s, got %s", testDir, store.basePath)
	}

	// Verify directory was created
	if _, err := os.Stat(testDir); os.IsNotExist(err) {
		t.Errorf("Directory %s was not created", testDir)
	}

	// Cleanup
	defer os.RemoveAll(testDir)
}

func TestGetTablePath(t *testing.T) {
	testDir := getTestDir()
	store, err := NewCSVStore(testDir)
	if err != nil {
		t.Fatalf("Failed to create CSVStore: %v", err)
	}
	defer os.RemoveAll(testDir)

	tableName := "test_table_for_path"
	expectedPath := filepath.Join(testDir, tableName+".csv")
	actualPath := store.GetTablePath(tableName)

	if actualPath != expectedPath {
		t.Errorf("GetTablePath: expected %s, got %s", expectedPath, actualPath)
	}
}

func TestCreateTable(t *testing.T) {
	testDir := getTestDir()
	store, err := NewCSVStore(testDir)
	if err != nil {
		t.Fatalf("Failed to create CSVStore: %v", err)
	}
	defer os.RemoveAll(testDir)

	headers := []string{"id", "name", "email", "created_at"}
	tableName := "users"

	err = store.CreateTable(tableName, headers)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Verify table file exists
	tablePath := store.GetTablePath(tableName)
	if _, err := os.Stat(tablePath); os.IsNotExist(err) {
		t.Errorf("Table file %s was not created", tablePath)
	}

	// Test creating duplicate table
	err = store.CreateTable(tableName, headers)
	if err == nil {
		t.Error("Expected error when creating duplicate table")
	}
}

func TestInsert(t *testing.T) {
	testDir := getTestDir()
	store, err := NewCSVStore(testDir)
	if err != nil {
		t.Fatalf("Failed to create CSVStore: %v", err)
	}
	defer os.RemoveAll(testDir)

	tableName := "users"
	headers := []string{"id", "name", "email", "created_at", "updated_at"}

	err = store.CreateTable(tableName, headers)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	record := CSVRecord{
		"name":  "John Doe",
		"email": "john@example.com",
	}

	insertedRecord, err := store.Insert(tableName, record)
	if err != nil {
		t.Fatalf("Failed to insert record: %v", err)
	}

	// Check that id was automatically added
	if insertedRecord["id"] == "" {
		t.Error("Expected id to be automatically set")
	}

	if insertedRecord["name"] != "John Doe" {
		t.Errorf("Expected name 'John Doe', got '%s'", insertedRecord["name"])
	}

	// Check that created_at was automatically added
	if insertedRecord["created_at"] == "" {
		t.Error("Expected created_at to be automatically set")
	}

	// Check that updated_at was automatically added
	if insertedRecord["updated_at"] == "" {
		t.Error("Expected updated_at to be automatically set")
	}
}

func TestQuery(t *testing.T) {
	testDir := getTestDir()
	store, err := NewCSVStore(testDir)
	if err != nil {
		t.Fatalf("Failed to create CSVStore: %v", err)
	}
	defer os.RemoveAll(testDir)

	tableName := "products"
	headers := []string{"id", "name", "price", "category"}

	err = store.CreateTable(tableName, headers)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	records := []CSVRecord{
		{"id": "1", "name": "Laptop", "price": "999.99", "category": "Electronics"},
		{"id": "2", "name": "Book", "price": "19.99", "category": "Books"},
		{"id": "3", "name": "Phone", "price": "599.99", "category": "Electronics"},
	}

	for _, record := range records {
		_, err = store.Insert(tableName, record)
		if err != nil {
			t.Fatalf("Failed to insert record: %v", err)
		}
	}

	// Test query with no conditions
	result, err := store.Query(tableName, nil)
	if err != nil {
		t.Fatalf("Failed to query table: %v", err)
	}
	if result.Count != 3 {
		t.Errorf("Expected 3 records, got %d", result.Count)
	}

	// Test query with equality condition
	conditions := []QueryCondition{
		{Column: "category", Operator: "=", Value: "Electronics"},
	}
	result, err = store.Query(tableName, conditions)
	if err != nil {
		t.Fatalf("Failed to query table: %v", err)
	}
	if result.Count != 2 {
		t.Errorf("Expected 2 electronics records, got %d", result.Count)
	}

	// Test query with numeric comparison
	conditions = []QueryCondition{
		{Column: "price", Operator: ">", Value: "500"},
	}
	result, err = store.Query(tableName, conditions)
	if err != nil {
		t.Fatalf("Failed to query table: %v", err)
	}
	if result.Count != 2 {
		t.Errorf("Expected 2 records with price > 500, got %d", result.Count)
	}

	// Test query with string contains
	conditions = []QueryCondition{
		{Column: "name", Operator: "contains", Value: "book"},
	}
	result, err = store.Query(tableName, conditions)
	if err != nil {
		t.Fatalf("Failed to query table: %v", err)
	}
	if result.Count != 1 {
		t.Errorf("Expected 1 record containing 'book', got %d", result.Count)
	}
}

func TestSelect(t *testing.T) {
	testDir := getTestDir()
	store, err := NewCSVStore(testDir)
	if err != nil {
		t.Fatalf("Failed to create CSVStore: %v", err)
	}
	defer os.RemoveAll(testDir)

	tableName := "users"
	headers := []string{"id", "name", "email", "age"}

	err = store.CreateTable(tableName, headers)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	record := CSVRecord{
		"name":  "Alice",
		"email": "alice@example.com",
		"age":   "25",
	}

	_, err = store.Insert(tableName, record)
	if err != nil {
		t.Fatalf("Failed to insert record: %v", err)
	}

	// Select specific columns
	columns := []string{"name", "email"}
	result, err := store.Select(tableName, columns, nil)
	if err != nil {
		t.Fatalf("Failed to select columns: %v", err)
	}

	if result.Count != 1 {
		t.Errorf("Expected 1 record, got %d", result.Count)
	}

	selectedRecord := result.Records[0]
	if len(selectedRecord) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(selectedRecord))
	}

	if selectedRecord["name"] != "Alice" {
		t.Errorf("Expected name 'Alice', got '%s'", selectedRecord["name"])
	}

	if _, exists := selectedRecord["age"]; exists {
		t.Error("Age column should not be present in selected result")
	}
}

func TestUpdate(t *testing.T) {
	testDir := getTestDir()
	store, err := NewCSVStore(testDir)
	if err != nil {
		t.Fatalf("Failed to create CSVStore: %v", err)
	}
	defer os.RemoveAll(testDir)

	tableName := "users"
	headers := []string{"id", "name", "email", "updated_at"}

	err = store.CreateTable(tableName, headers)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	initialRecords := []CSVRecord{
		{"id": "1", "name": "John", "email": "john@example.com"},
		{"id": "2", "name": "Jane", "email": "jane@example.com"},
	}

	for _, record := range initialRecords {
		_, err = store.Insert(tableName, record)
		if err != nil {
			t.Fatalf("Failed to insert record: %v", err)
		}
	}

	// Store original updated_at for John
	queryJohnCondition := []QueryCondition{{Column: "name", Operator: "=", Value: "John"}}
	originalJohnResult, err := store.Query(tableName, queryJohnCondition)
	if err != nil {
		t.Fatalf("Failed to query John before update: %v", err)
	}
	if originalJohnResult.Count != 1 {
		t.Fatalf("Expected 1 John record before update, got %d", originalJohnResult.Count)
	}
	originalJohnUpdatedAt := originalJohnResult.Records[0]["updated_at"]

	conditions := []QueryCondition{
		{Column: "name", Operator: "=", Value: "John"},
	}

	updatedResult, err := store.Update(tableName, CSVRecord{
		"email": "newemail@example.com",
	}, conditions)
	if err != nil {
		t.Fatalf("Failed to update records: %v", err)
	}

	if updatedResult.Count != 1 {
		t.Errorf("Expected 1 updated record, got %d", updatedResult.Count)
	}

	if len(updatedResult.Records) != 1 {
		t.Fatalf("Expected 1 record in updatedResult.Records, got %d", len(updatedResult.Records))
	}
	updatedRecord := updatedResult.Records[0]

	if updatedRecord["name"] != "John" {
		t.Errorf("Expected updated record name to be 'John', got '%s'", updatedRecord["name"])
	}
	if updatedRecord["email"] != "newemail@example.com" {
		t.Errorf(
			"Expected updated record email to be 'newemail@example.com', got '%s'",
			updatedRecord["email"],
		)
	}
	if updatedRecord["id"] != "1" {
		t.Errorf("Expected updated record id to be '1', got '%s'", updatedRecord["id"])
	}

	// Verify update by querying again
	queriedAfterUpdateResult, err := store.Query(tableName, conditions)
	if err != nil {
		t.Fatalf("Failed to query updated record: %v", err)
	}
	if queriedAfterUpdateResult.Count != 1 {
		t.Fatalf(
			"Expected 1 John record after update query, got %d",
			queriedAfterUpdateResult.Count,
		)
	}
	queriedRecord := queriedAfterUpdateResult.Records[0]

	if queriedRecord["email"] != "newemail@example.com" {
		t.Errorf("Expected updated email in queried record, got '%s'", queriedRecord["email"])
	}

	// Check the updated_at is changed
	if originalJohnUpdatedAt == queriedRecord["updated_at"] {
		t.Errorf(
			"Expected updated_at to be different after update. Original: %s, New: %s",
			originalJohnUpdatedAt,
			queriedRecord["updated_at"],
		)
	}
	if queriedRecord["updated_at"] == "" {
		t.Error("Expected updated_at to be set in queried record")
	}
	// Also check the updated_at in the returned result from Update
	if originalJohnUpdatedAt == updatedRecord["updated_at"] {
		t.Errorf(
			"Expected updated_at in Update() result to be different. Original: %s, New: %s",
			originalJohnUpdatedAt,
			updatedRecord["updated_at"],
		)
	}
	if updatedRecord["updated_at"] == "" {
		t.Error("Expected updated_at to be set in Update() result")
	}
}

func TestDelete(t *testing.T) {
	testDir := getTestDir()
	store, err := NewCSVStore(testDir)
	if err != nil {
		t.Fatalf("Failed to create CSVStore: %v", err)
	}
	defer os.RemoveAll(testDir)

	tableName := "users"
	headers := []string{"id", "name", "email"}

	err = store.CreateTable(tableName, headers)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	records := []CSVRecord{
		{"id": "1", "name": "John", "email": "john@example.com"},
		{"id": "2", "name": "Jane", "email": "jane@example.com"},
		{"id": "3", "name": "Bob", "email": "bob@example.com"},
	}

	for _, record := range records {
		_, err = store.Insert(tableName, record)
		if err != nil {
			t.Fatalf("Failed to insert record: %v", err)
		}
	}

	// Delete records
	conditions := []QueryCondition{
		{Column: "name", Operator: "=", Value: "Jane"},
	}

	deletedResult, err := store.Delete(tableName, conditions)
	if err != nil {
		t.Fatalf("Failed to delete records: %v", err)
	}

	if deletedResult.Count != 1 {
		t.Errorf("Expected 1 deleted record, got %d", deletedResult.Count)
	}

	if len(deletedResult.Records) != 1 {
		t.Fatalf("Expected 1 record in deletedResult.Records, got %d", len(deletedResult.Records))
	}
	deletedRecord := deletedResult.Records[0]
	if deletedRecord["name"] != "Jane" {
		t.Errorf("Expected deleted record name to be 'Jane', got '%s'", deletedRecord["name"])
	}
	if deletedRecord["email"] != "jane@example.com" {
		t.Errorf(
			"Expected deleted record email to be 'jane@example.com', got '%s'",
			deletedRecord["email"],
		)
	}
	if deletedRecord["id"] != "2" {
		t.Errorf("Expected deleted record id to be '2', got '%s'", deletedRecord["id"])
	}
}

func TestEdgeCases(t *testing.T) {
	testDir := getTestDir()
	store, err := NewCSVStore(testDir)
	if err != nil {
		t.Fatalf("Failed to create CSVStore: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Test operations on non-existent table
	_, err = store.Query("nonexistent", nil)
	if err == nil {
		t.Error("Expected error when querying non-existent table")
	}

	_, err = store.Insert("nonexistent", CSVRecord{"id": "1"})
	if err == nil {
		t.Error("Expected error when inserting to non-existent table")
	}

	_, err = store.Update("nonexistent", CSVRecord{"name": "test"}, nil)
	if err == nil {
		t.Error("Expected error when updating non-existent table")
	}

	_, err = store.Delete("nonexistent", nil)
	if err == nil {
		t.Error("Expected error when deleting from non-existent table")
	}

	// Test empty table operations
	tableName := "empty_table"
	headers := []string{"id", "name"}

	err = store.CreateTable(tableName, headers)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	result, err := store.Query(tableName, nil)
	if err != nil {
		t.Fatalf("Failed to query empty table: %v", err)
	}
	if result.Count != 0 {
		t.Errorf("Expected 0 records in empty table, got %d", result.Count)
	}

	// Test update/delete on empty table
	updatedResult, err := store.Update(tableName, CSVRecord{"name": "test"}, nil)
	if err != nil {
		t.Fatalf("Failed to update empty table: %v", err)
	}
	if updatedResult.Count != 0 {
		t.Errorf("Expected 0 updated records, got %d", updatedResult.Count)
	}
	if len(updatedResult.Records) != 0 {
		t.Errorf(
			"Expected 0 records in updatedResult.Records for empty table update, got %d",
			len(updatedResult.Records),
		)
	}

	deletedResult, err := store.Delete(tableName, nil)
	if err != nil {
		t.Fatalf("Failed to delete from empty table: %v", err)
	}
	if deletedResult.Count != 0 {
		t.Errorf("Expected 0 deleted records, got %d", deletedResult.Count)
	}
	if len(deletedResult.Records) != 0 {
		t.Errorf(
			"Expected 0 records in deletedResult.Records for empty table delete, got %d",
			len(deletedResult.Records),
		)
	}
}
