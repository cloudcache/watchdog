package watchdog

import "testing"

func TestSplitSQLStatementsSkipsCommentsAndKeepsQuotedSemicolons(t *testing.T) {
	statements := SplitSQLStatements(`
-- create tenant table
CREATE TABLE tenants (id VARCHAR(26) PRIMARY KEY);
/* seed with semicolon-like content */
INSERT INTO tenants (id) VALUES ('tenant;dev');
# final marker
INSERT INTO watchdog_installation (id, config_path) VALUES ("default", "a;b");
`)
	if len(statements) != 3 {
		t.Fatalf("statement count = %d: %#v", len(statements), statements)
	}
	if statements[1] != "INSERT INTO tenants (id) VALUES ('tenant;dev')" {
		t.Fatalf("second statement = %q", statements[1])
	}
	if statements[2] != `INSERT INTO watchdog_installation (id, config_path) VALUES ("default", "a;b")` {
		t.Fatalf("third statement = %q", statements[2])
	}
}

func TestSplitSQLStatementsIgnoresEmptyInput(t *testing.T) {
	statements := SplitSQLStatements(`
-- only comments
/* block */
;
`)
	if len(statements) != 0 {
		t.Fatalf("statement count = %d", len(statements))
	}
}
