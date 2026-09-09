package watchdog

import "testing"

func TestParsePageInteger(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		fallback int
		minimum  int
		maximum  int
		want     int
		wantErr  bool
	}{
		{name: "empty uses fallback", raw: " ", fallback: 50, minimum: 1, maximum: 500, want: 50},
		{name: "minimum", raw: "1", fallback: 50, minimum: 1, maximum: 500, want: 1},
		{name: "maximum", raw: "500", fallback: 50, minimum: 1, maximum: 500, want: 500},
		{name: "not an integer", raw: "1.5", fallback: 50, minimum: 1, maximum: 500, wantErr: true},
		{name: "below minimum", raw: "0", fallback: 50, minimum: 1, maximum: 500, wantErr: true},
		{name: "above maximum", raw: "501", fallback: 50, minimum: 1, maximum: 500, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePageInteger(tt.raw, tt.fallback, tt.minimum, tt.maximum)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parsePageInteger() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("parsePageInteger() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestParseAgentPageIntegerCompatibility(t *testing.T) {
	got, err := parseAgentPageInteger("25", 50, 1, 500)
	if err != nil || got != 25 {
		t.Fatalf("parseAgentPageInteger() = %d, %v; want 25, nil", got, err)
	}
}
