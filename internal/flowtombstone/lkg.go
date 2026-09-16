// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowtombstone

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
)

const lkgMaxBytes = 1 << 20

func LoadFile(path string) (Barrier, error) {
	file, err := os.Open(path)
	if err != nil {
		return Barrier{}, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, lkgMaxBytes+1))
	if err != nil || len(data) == 0 || len(data) > lkgMaxBytes {
		if err == nil {
			err = ErrInvalidBarrier
		}
		return Barrier{}, err
	}
	var barrier Barrier
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&barrier); err != nil {
		return Barrier{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Barrier{}, ErrInvalidBarrier
	}
	if err := barrier.Validate(); err != nil {
		return Barrier{}, err
	}
	return barrier, nil
}

func SaveFile(path string, barrier Barrier) error {
	if path == "" || barrier.Validate() != nil {
		return ErrInvalidBarrier
	}
	if current, err := LoadFile(path); err == nil {
		guard, guardErr := NewGuard(&current)
		if guardErr != nil {
			return guardErr
		}
		if err := guard.Install(barrier); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	data, err := json.Marshal(barrier)
	if err != nil || len(data) > lkgMaxBytes {
		return ErrInvalidBarrier
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".raw-delete-barrier-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	directoryHandle, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer directoryHandle.Close()
	return directoryHandle.Sync()
}
