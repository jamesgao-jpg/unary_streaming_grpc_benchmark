// This file verifies benchmark configuration validation.
package main

import "testing"

// TestStaticWindowValidation verifies dynamic and supported static window settings.
func TestStaticWindowValidation(t *testing.T) {
	cfg, err := loadConfig("config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name      string
		value     int32
		wantError bool
	}{
		{name: "dynamic", value: 0},
		{name: "minimum static", value: 65536},
		{name: "below minimum", value: 65535, wantError: true},
		{name: "negative", value: -1, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg.GRPC.Client.StaticWindowBytes = test.value
			if err := cfg.validate(); (err != nil) != test.wantError {
				t.Fatalf("validate() error = %v, wantError = %t", err, test.wantError)
			}
		})
	}
}
