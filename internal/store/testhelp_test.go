package store

import "os"

func writeLineForTest(path, s string) error {
	return os.WriteFile(path, []byte(s), 0o644)
}
