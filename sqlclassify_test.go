package main

import "testing"

func TestIsDML(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sql  string
		want bool
	}{
		{"UPDATE Singers SET FirstName = 'x' WHERE SingerId = 1", true},
		{"INSERT INTO Singers (SingerId) VALUES (1)", true},
		{"DELETE FROM Singers WHERE SingerId = 1", true},
		{"  delete from Singers where SingerId = 1", true},
		{"-- comment\nUPDATE Singers SET FirstName = 'x' WHERE SingerId = 1", true},
		{"/* comment */\nDELETE FROM Singers WHERE SingerId = 1", true},
		{"/* outer */ -- line\n  UPDATE T SET c = 1", true},
		{"SELECT * FROM Singers", false},
		{"-- leading comment only\nSELECT 1", false},
		{"WITH x AS (SELECT 1) SELECT * FROM x", false},
		{"", false},
		{"-- only a comment", false},
	}
	for _, tc := range tests {
		t.Run(tc.sql, func(t *testing.T) {
			t.Parallel()
			if got := isDML(tc.sql); got != tc.want {
				t.Fatalf("isDML(%q) = %v, want %v", tc.sql, got, tc.want)
			}
		})
	}
}
