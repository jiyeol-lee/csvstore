package csvstore

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

func TestQuerySortedRange(t *testing.T) {
	testDir := getTestDir()
	store, err := NewCSVStore(testDir)
	if err != nil {
		t.Fatalf("Failed to create CSVStore: %v", err)
	}
	defer os.RemoveAll(testDir)

	tableName := "range_test_items"
	headers := []string{"id", "name", "value"} // "value" will be treated as string for sorting
	err = store.CreateTable(tableName, headers)
	if err != nil {
		t.Fatalf("Failed to create table '%s': %v", tableName, err)
	}

	// Records for testing, unsorted by "value":
	// ItemA: "100", ItemB: "200", ItemC: "150", ItemD: "250", ItemE: "050"
	recordsToInsert := []CSVRecord{
		{"id": "1", "name": "ItemA", "value": "100"},
		{"id": "2", "name": "ItemB", "value": "200"},
		{"id": "3", "name": "ItemC", "value": "150"},
		{"id": "4", "name": "ItemD", "value": "250"},
		{"id": "5", "name": "ItemE", "value": "050"},
	}

	for _, rec := range recordsToInsert {
		_, err = store.Insert(tableName, rec)
		if err != nil {
			t.Fatalf("Failed to insert record into '%s': %v", tableName, err)
		}
	}

	// Expected order (asc): ItemE ("050"), ItemA ("100"), ItemC ("150"), ItemB ("200"), ItemD ("250")
	// Expected order (desc): ItemD ("250"), ItemB ("200"), ItemC ("150"), ItemA ("100"), ItemE ("050")

	// Test Case 1: Ascending order, limit 2
	// Expect ItemE ("050"), ItemA ("100")
	res1, err1 := store.QuerySortedRange(tableName, "value", "asc", 2)
	if err1 != nil {
		t.Fatalf("Test Case 1 (Asc Limit 2): Expected no error, got %v", err1)
	}
	if len(res1) != 2 {
		t.Errorf("Test Case 1 (Asc Limit 2): Expected 2 records, got %d", len(res1))
	} else {
		if res1[0]["name"] != "ItemE" || res1[1]["name"] != "ItemA" {
			t.Errorf("Test Case 1 (Asc Limit 2): Records not in expected order. Got: %v, %v", res1[0]["name"], res1[1]["name"])
		}
	}

	// Test Case 2: Descending order, limit 3
	// Expect ItemD ("250"), ItemB ("200"), ItemC ("150")
	res2, err2 := store.QuerySortedRange(tableName, "value", "desc", 3)
	if err2 != nil {
		t.Fatalf("Test Case 2 (Desc Limit 3): Expected no error, got %v", err2)
	}
	if len(res2) != 3 {
		t.Errorf("Test Case 2 (Desc Limit 3): Expected 3 records, got %d", len(res2))
	} else {
		if res2[0]["name"] != "ItemD" || res2[1]["name"] != "ItemB" || res2[2]["name"] != "ItemC" {
			t.Errorf("Test Case 2 (Desc Limit 3): Records not in expected order. Got: %v, %v, %v", res2[0]["name"], res2[1]["name"], res2[2]["name"])
		}
	}

	// Test Case 3: Ascending order, limit 0
	res3, err3 := store.QuerySortedRange(tableName, "value", "asc", 0)
	if err3 != nil {
		t.Fatalf("Test Case 3 (Asc Limit 0): Expected no error, got %v", err3)
	}
	if len(res3) != 0 {
		t.Errorf("Test Case 3 (Asc Limit 0): Expected 0 records, got %d", len(res3))
	}

	// Test Case 4: Ascending order, limit > total (e.g., 10)
	// Expect all 5 records in ascending order
	res4, err4 := store.QuerySortedRange(tableName, "value", "asc", 10)
	if err4 != nil {
		t.Fatalf("Test Case 4 (Asc Limit > Total): Expected no error, got %v", err4)
	}
	if len(res4) != 5 {
		t.Errorf("Test Case 4 (Asc Limit > Total): Expected 5 records, got %d", len(res4))
	} else {
		if res4[0]["name"] != "ItemE" || res4[1]["name"] != "ItemA" || res4[2]["name"] != "ItemC" || res4[3]["name"] != "ItemB" || res4[4]["name"] != "ItemD" {
			t.Errorf("Test Case 4 (Asc Limit > Total): Records not in expected order. Got: %v, %v, %v, %v, %v", res4[0]["name"], res4[1]["name"], res4[2]["name"], res4[3]["name"], res4[4]["name"])
		}
	}

	// Test Case 5: Non-existent table
	_, err5 := store.QuerySortedRange("fake_table", "value", "asc", 2)
	if err5 == nil {
		t.Error("Test Case 5 (Non-existent table): Expected error, got nil")
	}

	// Test Case 6: Invalid sort column
	_, err6 := store.QuerySortedRange(tableName, "fake_column", "asc", 2)
	if err6 == nil {
		t.Error("Test Case 6 (Invalid column): Expected error, got nil")
	}

	// Test Case 7: Empty table
	emptyTableName := "empty_range_table"
	err = store.CreateTable(emptyTableName, headers)
	if err != nil {
		t.Fatalf("Test Case 7 (Empty Table): Failed to create table '%s': %v", emptyTableName, err)
	}
	res7, err7 := store.QuerySortedRange(emptyTableName, "value", "asc", 2)
	if err7 != nil {
		t.Fatalf("Test Case 7 (Empty Table): Expected no error, got %v", err7)
	}
	if len(res7) != 0 {
		t.Errorf("Test Case 7 (Empty Table): Expected 0 records, got %d", len(res7))
	}

	// Test Case 8: Invalid sortOrder string
	_, err8 := store.QuerySortedRange(tableName, "value", "invalid_sort_order", 2)
	if err8 == nil {
		t.Error(
			"Test Case 8 (Invalid sortOrder): Expected error for invalid sortOrder string, got nil",
		)
	} else {
		if !strings.Contains(err8.Error(), "sortBy must be either 'asc' or 'desc'") {
			t.Errorf("Test Case 8 (Invalid sortOrder): Expected error about sortOrder, got %v", err8)
		}
	}
}
