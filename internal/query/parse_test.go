package query

import "testing"

func TestParseSimpleSelect(t *testing.T) {
	q, err := ParseSQL("SELECT id, name FROM users WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if q.Info.StatementType != "SELECT" {
		t.Errorf("got %q, want SELECT", q.Info.StatementType)
	}
	if !q.Info.HasWhere {
		t.Error("expected HasWhere")
	}
	if q.Info.HasSelectStar {
		t.Error("did not expect HasSelectStar")
	}
	if q.Info.HasJoin {
		t.Error("did not expect HasJoin")
	}
	if len(q.Info.Tables) != 1 || q.Info.Tables[0].Name != "users" {
		t.Errorf("unexpected tables: %v", q.Info.Tables)
	}
}

func TestDetectSelectStar(t *testing.T) {
	q, err := ParseSQL("SELECT * FROM orders")
	if err != nil {
		t.Fatal(err)
	}
	if !q.Info.HasSelectStar {
		t.Error("expected HasSelectStar")
	}
	if q.Info.HasWhere {
		t.Error("did not expect HasWhere")
	}
	if q.Info.HasLimit {
		t.Error("did not expect HasLimit")
	}
}

func TestDetectJoin(t *testing.T) {
	q, err := ParseSQL("SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id WHERE o.total > 100")
	if err != nil {
		t.Fatal(err)
	}
	if !q.Info.HasJoin {
		t.Error("expected HasJoin")
	}
	if !q.Info.HasWhere {
		t.Error("expected HasWhere")
	}
	if len(q.Info.Tables) != 2 {
		t.Errorf("expected 2 tables, got %d", len(q.Info.Tables))
	}
}

func TestDetectLimit(t *testing.T) {
	q, err := ParseSQL("SELECT * FROM users LIMIT 10")
	if err != nil {
		t.Fatal(err)
	}
	if !q.Info.HasLimit {
		t.Error("expected HasLimit")
	}
}

func TestParseError(t *testing.T) {
	_, err := ParseSQL("SELEC broken")
	if err == nil {
		t.Error("expected error for invalid SQL")
	}
}

func TestDetectUpdateWithoutWhere(t *testing.T) {
	q, err := ParseSQL("UPDATE users SET name = 'test'")
	if err != nil {
		t.Fatal(err)
	}
	if q.Info.StatementType != "UPDATE" {
		t.Errorf("got %q, want UPDATE", q.Info.StatementType)
	}
	if q.Info.HasWhere {
		t.Error("did not expect HasWhere")
	}
}

func TestDetectDeleteWithWhere(t *testing.T) {
	q, err := ParseSQL("DELETE FROM users WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if q.Info.StatementType != "DELETE" {
		t.Errorf("got %q, want DELETE", q.Info.StatementType)
	}
	if !q.Info.HasWhere {
		t.Error("expected HasWhere")
	}
}

func TestFuncWrappedExtract(t *testing.T) {
	q, err := ParseSQL("SELECT * FROM events WHERE EXTRACT(year FROM created_at) = 2025")
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Info.FuncWrappedColumns) != 1 {
		t.Fatalf("expected 1 FuncWrappedColumn, got %d", len(q.Info.FuncWrappedColumns))
	}
	fwc := q.Info.FuncWrappedColumns[0]
	if fwc.Column != "created_at" || fwc.FuncName != "extract" {
		t.Errorf("got column=%q func=%q, want created_at/extract", fwc.Column, fwc.FuncName)
	}
}

func TestFuncWrappedTypeCast(t *testing.T) {
	q, err := ParseSQL("SELECT * FROM events WHERE created_at::date = '2025-01-01'")
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Info.FuncWrappedColumns) != 1 {
		t.Fatalf("expected 1 FuncWrappedColumn, got %d", len(q.Info.FuncWrappedColumns))
	}
	fwc := q.Info.FuncWrappedColumns[0]
	if fwc.Column != "created_at" || fwc.FuncName != "::date" {
		t.Errorf("got column=%q func=%q, want created_at/::date", fwc.Column, fwc.FuncName)
	}
}

func TestFuncWrappedDateTrunc(t *testing.T) {
	q, err := ParseSQL("SELECT * FROM events WHERE date_trunc('month', created_at) = '2025-01-01'")
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Info.FuncWrappedColumns) != 1 {
		t.Fatalf("expected 1 FuncWrappedColumn, got %d", len(q.Info.FuncWrappedColumns))
	}
	fwc := q.Info.FuncWrappedColumns[0]
	if fwc.Column != "created_at" || fwc.FuncName != "date_trunc" {
		t.Errorf("got column=%q func=%q, want created_at/date_trunc", fwc.Column, fwc.FuncName)
	}
}

func TestFuncWrappedToChar(t *testing.T) {
	q, err := ParseSQL("SELECT * FROM events WHERE to_char(created_at, 'YYYY-MM') = '2025-01'")
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Info.FuncWrappedColumns) != 1 {
		t.Fatalf("expected 1 FuncWrappedColumn, got %d", len(q.Info.FuncWrappedColumns))
	}
	fwc := q.Info.FuncWrappedColumns[0]
	if fwc.Column != "created_at" || fwc.FuncName != "to_char" {
		t.Errorf("got column=%q func=%q, want created_at/to_char", fwc.Column, fwc.FuncName)
	}
}

func TestFuncWrappedQualifiedColumn(t *testing.T) {
	q, err := ParseSQL("SELECT * FROM events e WHERE e.created_at::date = '2025-01-01'")
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Info.FuncWrappedColumns) != 1 {
		t.Fatalf("expected 1 FuncWrappedColumn, got %d", len(q.Info.FuncWrappedColumns))
	}
	fwc := q.Info.FuncWrappedColumns[0]
	if fwc.Table == nil || *fwc.Table != "e" {
		t.Errorf("expected table=e, got %v", fwc.Table)
	}
	if fwc.Column != "created_at" {
		t.Errorf("expected column=created_at, got %q", fwc.Column)
	}
}

func TestNoFuncWrappedForLiteralFunction(t *testing.T) {
	q, err := ParseSQL("SELECT * FROM events WHERE created_at > now()")
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Info.FuncWrappedColumns) != 0 {
		t.Errorf("expected no FuncWrappedColumns, got %d: %v", len(q.Info.FuncWrappedColumns), q.Info.FuncWrappedColumns)
	}
}

func TestNoFuncWrappedInSelect(t *testing.T) {
	q, err := ParseSQL("SELECT lower(name) FROM users WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Info.FuncWrappedColumns) != 0 {
		t.Errorf("expected no FuncWrappedColumns for SELECT-only function, got %d", len(q.Info.FuncWrappedColumns))
	}
}
